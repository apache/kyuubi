/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.flink

import java.io.{File, FilenameFilter}
import java.nio.file.{Files, Paths}

import scala.collection.mutable

import com.google.common.annotations.VisibleForTesting
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.{ApplicationManagerInfo, KyuubiApplicationManager, ProcBuilder}
import org.apache.kyuubi.engine.flink.FlinkProcessBuilder._
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.util.command.CommandLineUtils._

/**
 * A builder to build flink sql engine progress.
 */
class FlinkProcessBuilder(
    override val proxyUser: String,
    override val doAsEnabled: Boolean,
    override val conf: KyuubiConf,
    val engineRefId: String,
    val extraEngineLog: Option[OperationLog] = None)
  extends ProcBuilder {

  @VisibleForTesting
  def this(proxyUser: String, doAsEnabled: Boolean, conf: KyuubiConf) {
    this(proxyUser, doAsEnabled, conf, "")
  }

  val flinkHome: String = getEngineHome(shortName)

  val flinkExecutable: String = {
    Paths.get(flinkHome, "bin", FLINK_EXEC_FILE).toFile.getCanonicalPath
  }

  // flink.execution.target are required in Kyuubi conf currently
  val executionTarget: Option[String] = conf.getOption("flink.execution.target")

  private lazy val proxyUserEnable: Boolean = {
    var flinkDoAsEnabled = conf.get(ENGINE_FLINK_DOAS_ENABLED)
    if (flinkDoAsEnabled && !UserGroupInformation.isSecurityEnabled) {
      warn(s"${ENGINE_FLINK_DOAS_ENABLED.key} can only be enabled on Kerberized environment.")
      flinkDoAsEnabled = false
    }
    flinkDoAsEnabled
  }

  override protected def module: String = "kyuubi-flink-sql-engine"

  override protected def mainClass: String = "org.apache.kyuubi.engine.flink.FlinkSQLEngine"

  override def env: Map[String, String] = {
    val flinkConfDir =
      conf.getEnvs.getOrElse("FLINK_CONF_DIR", s"$flinkHome${File.separator}conf")
    val flinkExtraEnvs = if (proxyUserEnable) {
      Map(
        "FLINK_CONF_DIR" -> flinkConfDir,
        FLINK_PROXY_USER_KEY -> proxyUser) ++ generateTokenFile()
    } else {
      Map("FLINK_CONF_DIR" -> flinkConfDir)
    }
    conf.getEnvs ++ flinkExtraEnvs
  }

  override def clusterManager(): Option[String] = {
    executionTarget match {
      case Some("yarn-application") => Some("yarn")
      case _ => None
    }
  }

  override def appMgrInfo(): ApplicationManagerInfo = {
    ApplicationManagerInfo(clusterManager())
  }

  override protected val commands: Iterable[String] = {
    KyuubiApplicationManager.tagApplication(engineRefId, shortName, clusterManager(), conf)
    // flink.execution.target are required in Kyuubi conf currently
    executionTarget match {
      case Some("yarn-application") =>
        val buffer = new mutable.ListBuffer[String]()
        buffer += flinkExecutable
        buffer += "run-application"

        val flinkExtraJars = new mutable.ListBuffer[String]
        // locate flink sql jars
        val flinkSqlJars = Paths.get(flinkHome)
          .resolve("opt")
          .toFile
          .listFiles(new FilenameFilter {
            override def accept(dir: File, name: String): Boolean = {
              name.toLowerCase.startsWith("flink-sql-client") ||
              name.toLowerCase.startsWith("flink-sql-gateway")
            }
          }).map(f => f.getAbsolutePath).sorted
        flinkExtraJars ++= flinkSqlJars

        val userJars = conf.get(ENGINE_FLINK_APPLICATION_JARS)
        userJars.foreach(jars => flinkExtraJars ++= jars.split(","))

        val hiveConfDirOpt = env.get("HIVE_CONF_DIR")
        hiveConfDirOpt.foreach { hiveConfDir =>
          val hiveConfFile = Paths.get(hiveConfDir).resolve("hive-site.xml")
          if (!Files.exists(hiveConfFile)) {
            throw new KyuubiException(s"The file $hiveConfFile does not exists. " +
              s"Please put hive-site.xml when HIVE_CONF_DIR env $hiveConfDir is configured.")
          }
          flinkExtraJars += s"$hiveConfFile"
        }

        val externalProxyUserConf: Map[String, String] = if (proxyUserEnable) {
          // FLINK-31109 (1.17.0): Flink only supports hadoop proxy user when delegation tokens
          // fetch is managed outside, but disabling `security.delegation.tokens.enabled` will cause
          // delegation token updates on JobManager not to be passed to TaskManagers.
          // Based on the solution in
          // https://github.com/apache/flink/pull/22009#issuecomment-2122226755, we removed
          // `HadoopModuleFactory` from `security.module.factory.classes` and disabled delegation
          // token providers (hadoopfs/hbase/HiveServer2) that do not support proxyUser.
          // FLINK-35525: We need to add `yarn.security.appmaster.delegation.token.services=kyuubi`
          // configuration to pass hdfs token obtained by kyuubi provider to the yarn client.
          Map(
            "security.module.factory.classes" ->
              ("org.apache.flink.runtime.security.modules.JaasModuleFactory;" +
                "org.apache.flink.runtime.security.modules.ZookeeperModuleFactory"),
            "security.delegation.token.provider.hadoopfs.enabled" -> "false",
            "security.delegation.token.provider.hbase.enabled" -> "false",
            "security.delegation.token.provider.HiveServer2.enabled" -> "false",
            "yarn.security.appmaster.delegation.token.services" -> "kyuubi")
        } else {
          Map.empty
        }

        val customFlinkConf = conf.getAllWithPrefix(FLINK_CONF_PREFIX, "") ++ externalProxyUserConf
        // add custom yarn.ship-files
        flinkExtraJars ++= customFlinkConf.get(YARN_SHIP_FILES_KEY)
        val yarnAppName = customFlinkConf.get(YARN_APPLICATION_NAME_KEY)
          .orElse(conf.getOption(APP_KEY))
        buffer += "-t"
        buffer += "yarn-application"
        buffer += s"-Dyarn.ship-files=${flinkExtraJars.mkString(";")}"
        buffer += s"-Dyarn.application.name=${yarnAppName.get}"
        buffer += s"-Dyarn.tags=${conf.getOption(YARN_TAG_KEY).get}"
        buffer += "-Dcontainerized.master.env.FLINK_CONF_DIR=."

        hiveConfDirOpt.foreach { _ =>
          buffer += "-Dcontainerized.master.env.HIVE_CONF_DIR=."
        }

        customFlinkConf.filter { case (k, _) =>
          !Seq("app.name", YARN_SHIP_FILES_KEY, YARN_APPLICATION_NAME_KEY, YARN_TAG_KEY)
            .contains(k)
        }.foreach { case (k, v) =>
          buffer += s"-D$k=$v"
        }

        buffer += "-c"
        buffer += s"$mainClass"
        buffer += s"${mainResource.get}"

        buffer ++= confKeyValue(KYUUBI_SESSION_USER_KEY, proxyUser)

        buffer ++= confKeyValues(conf.getAll.filter(_._1.startsWith("kyuubi.")))

        buffer

      case _ =>
        val buffer = new mutable.ListBuffer[String]()
        buffer += executable

        val memory = conf.get(ENGINE_FLINK_MEMORY)
        buffer += s"-Xmx$memory"
        val javaOptions = conf.get(ENGINE_FLINK_JAVA_OPTIONS).filter(StringUtils.isNotBlank(_))
        if (javaOptions.isDefined) {
          buffer ++= parseOptionString(javaOptions.get)
        }
        val classpathEntries = new mutable.LinkedHashSet[String]
        // flink engine runtime jar
        mainResource.foreach(classpathEntries.add)
        // flink sql jars
        Paths.get(flinkHome)
          .resolve("opt")
          .toFile
          .listFiles(new FilenameFilter {
            override def accept(dir: File, name: String): Boolean = {
              name.toLowerCase.startsWith("flink-sql-client") ||
              name.toLowerCase.startsWith("flink-sql-gateway")
            }
          }).sorted.foreach(jar => classpathEntries.add(jar.getAbsolutePath))

        // jars from flink lib
        classpathEntries.add(s"$flinkHome${File.separator}lib${File.separator}*")

        // classpath contains flink configurations, default to flink.home/conf
        classpathEntries.add(env.getOrElse("FLINK_CONF_DIR", s"$flinkHome${File.separator}conf"))
        // classpath contains hadoop configurations
        env.get("HADOOP_CONF_DIR").foreach(classpathEntries.add)
        env.get("YARN_CONF_DIR").foreach(classpathEntries.add)
        env.get("HBASE_CONF_DIR").foreach(classpathEntries.add)
        env.get("HIVE_CONF_DIR").foreach(classpathEntries.add)
        val hadoopCp = env.get(FLINK_HADOOP_CLASSPATH_KEY)
        val extraCp = conf.get(ENGINE_FLINK_EXTRA_CLASSPATH)
        extraCp.foreach(classpathEntries.add)

        val hasHadoopJar = {
          val files = Paths.get(flinkHome)
            .resolve("lib")
            .toFile
            .listFiles(new FilenameFilter {
              override def accept(dir: File, name: String): Boolean = {
                name.startsWith("hadoop-client") ||
                name.startsWith("flink-shaded-hadoop")
              }
            })
          files != null && files.nonEmpty
        }

        if (!hasHadoopJar) {
          hadoopCp.foreach(classpathEntries.add)
        }

        if (!hasHadoopJar && hadoopCp.isEmpty && extraCp.isEmpty) {
          warn(s"No Hadoop client jars found in $flinkHome/lib, and the conf of " +
            s"$FLINK_HADOOP_CLASSPATH_KEY and ${ENGINE_FLINK_EXTRA_CLASSPATH.key} is empty.")
          debug("Detected development environment.")
          mainResource.foreach { path =>
            val devHadoopJars = Paths.get(path).getParent
              .resolve(s"scala-$SCALA_COMPILE_VERSION")
              .resolve("jars")
            if (!Files.exists(devHadoopJars)) {
              throw new KyuubiException(
                s"The path $devHadoopJars does not exist. Please set " +
                  s"${FLINK_HADOOP_CLASSPATH_KEY} or ${ENGINE_FLINK_EXTRA_CLASSPATH.key} " +
                  s"to configure the location of Hadoop client jars. Alternatively," +
                  s"you can place the required hadoop-client or flink-shaded-hadoop jars " +
                  s"directly into the Flink lib directory: $flinkHome/lib.")
            }
            classpathEntries.add(s"$devHadoopJars${File.separator}*")
          }
        }
        buffer ++= genClasspathOption(classpathEntries)

        buffer += mainClass

        buffer ++= confKeyValue(KYUUBI_SESSION_USER_KEY, proxyUser)

        buffer ++= confKeyValues(conf.getAll)

        buffer
    }
  }

  private def generateTokenFile(): Option[(String, String)] = {
    if (conf.get(ENGINE_FLINK_DOAS_GENERATE_TOKEN_FILE)) {
      // We disabled `hadoopfs` token service, which may cause yarn client to miss hdfs token.
      // So we generate a hadoop token file to pass kyuubi engine tokens to submit process.
      // TODO: Removed this after FLINK-35525 (1.20.0), delegation tokens will be passed
      //  by `kyuubi` provider
      generateEngineTokenFile.map(tokenFile => "HADOOP_TOKEN_FILE_LOCATION" -> tokenFile)
    } else {
      None
    }
  }

  override def close(destroyProcess: Boolean): Unit = {
    super.close(destroyProcess)
  }

  override def shortName: String = "flink"
}

object FlinkProcessBuilder {
  final val FLINK_EXEC_FILE = "flink"
  final val FLINK_CONF_PREFIX = "flink"
  final val APP_KEY = "flink.app.name"
  final val YARN_TAG_KEY = "yarn.tags"
  final val YARN_SHIP_FILES_KEY = "yarn.ship-files"
  final val YARN_APPLICATION_NAME_KEY = "yarn.application.name"

  final val FLINK_HADOOP_CLASSPATH_KEY = "FLINK_HADOOP_CLASSPATH"
  final val FLINK_PROXY_USER_KEY = "HADOOP_PROXY_USER"
  final val FLINK_SECURITY_KEYTAB_KEY = "security.kerberos.login.keytab"
  final val FLINK_SECURITY_PRINCIPAL_KEY = "security.kerberos.login.principal"
}
