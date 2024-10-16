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
  extends ProcBuilder with Logging {

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

  override protected def module: String = "kyuubi-flink-sql-engine"

  override protected def mainClass: String = "org.apache.kyuubi.engine.flink.FlinkSQLEngine"

  override def env: Map[String, String] = conf.getEnvs +
    ("FLINK_CONF_DIR" -> conf.getEnvs.getOrElse(
      "FLINK_CONF_DIR",
      s"$flinkHome${File.separator}conf"))

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

        val customFlinkConf = conf.getAllWithPrefix(FLINK_CONF_PREFIX, "")
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
        hadoopCp.foreach(classpathEntries.add)
        val extraCp = conf.get(ENGINE_FLINK_EXTRA_CLASSPATH)
        extraCp.foreach(classpathEntries.add)
        if (hadoopCp.isEmpty && extraCp.isEmpty) {
          warn(s"The conf of ${FLINK_HADOOP_CLASSPATH_KEY} and " +
            s"${ENGINE_FLINK_EXTRA_CLASSPATH.key} is empty.")
          debug("Detected development environment.")
          mainResource.foreach { path =>
            val devHadoopJars = Paths.get(path).getParent
              .resolve(s"scala-$SCALA_COMPILE_VERSION")
              .resolve("jars")
            if (!Files.exists(devHadoopJars)) {
              throw new KyuubiException(s"The path $devHadoopJars does not exists. " +
                s"Please set ${FLINK_HADOOP_CLASSPATH_KEY} or ${ENGINE_FLINK_EXTRA_CLASSPATH.key}" +
                s" for configuring location of hadoop client jars, etc.")
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
}
