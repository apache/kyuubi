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

import java.io.{File, FilenameFilter, FileOutputStream}
import java.nio.file.{Files, Paths}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.google.common.annotations.VisibleForTesting
import org.apache.hadoop.util.StringUtils

import org.apache.kyuubi._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.{KyuubiApplicationManager, ProcBuilder}
import org.apache.kyuubi.engine.flink.FlinkProcessBuilder._
import org.apache.kyuubi.operation.log.OperationLog

/**
 * A builder to build flink sql engine progress.
 */
class FlinkProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    val engineRefId: String,
    val extraEngineLog: Option[OperationLog] = None)
  extends ProcBuilder with Logging {

  @VisibleForTesting
  def this(proxyUser: String, conf: KyuubiConf) {
    this(proxyUser, conf, "")
  }

  val flinkHome: String = getEngineHome(shortName)

  val flinkExecutable: String = {
    Paths.get(flinkHome, "bin", FLINK_EXEC_FILE).toFile.getCanonicalPath
  }

  override protected def module: String = "kyuubi-flink-sql-engine"

  override protected def mainClass: String = "org.apache.kyuubi.engine.flink.FlinkSQLEngine"

  override def env: Map[String, String] = conf.getEnvs +
    (FLINK_PROXY_USER_KEY -> proxyUser)

  override def clusterManager(): Option[String] = Some("yarn")

  override protected val commands: Array[String] = {
    KyuubiApplicationManager.tagApplication(engineRefId, shortName, clusterManager(), conf)

    // flink.execution.target are required in Kyuubi conf currently
    val executionTarget = conf.getOption("flink.execution.target")
    executionTarget match {
      case Some("yarn-application") =>
        val buffer = new ArrayBuffer[String]()
        buffer += flinkExecutable
        buffer += "run-application"

        // write and set kyuubi configuration
        val persistedConf = new Properties()
        val ioTmpDir = System.getProperty("java.io.tmpdir")
        val engineTmpDir = Paths.get(ioTmpDir, s"kyuubi-engine-$engineRefId")
        Files.createDirectories(engineTmpDir)
        val tmpKyuubiConfFile =
          Paths.get(engineTmpDir.toAbsolutePath.toString, KYUUBI_CONF_FILE_NAME).toFile
        val tmpKyuubiConf = tmpKyuubiConfFile.getCanonicalPath
        // Scala 2.12 have ambiguous reference for properties#putAll with Java 11
        // see https://github.com/scala/bug/issues/10418
        conf.getAll.asJava.forEach((k, v) => persistedConf.put(k, v))
        persistedConf.put(KYUUBI_SESSION_USER_KEY, s"$proxyUser")
        persistedConf.store(
          new FileOutputStream(tmpKyuubiConf),
          "persisted Kyuubi conf for Flink SQL engine")
        tmpKyuubiConfFile.deleteOnExit()
        engineTmpDir.toFile.deleteOnExit()

        // locate flink sql jars
        val flinkExtraJars = Paths.get(flinkHome)
          .resolve("opt")
          .toFile
          .listFiles(new FilenameFilter {
            override def accept(dir: File, name: String): Boolean = {
              name.toLowerCase.startsWith("flink-sql-client") ||
              name.toLowerCase.startsWith("flink-sql-gateway")
            }
          }).map(f => f.getAbsolutePath).sorted

        buffer += s"-Dyarn.ship-files=$tmpKyuubiConf"
        buffer += s"-Dpipeline.jars=${StringUtils.join(",", flinkExtraJars)}"
        buffer += s"-Dyarn.tags=${conf.getOption(YARN_TAG_KEY).get}"
        buffer += "-Dcontainerized.master.env.FLINK_CONF_DIR=."
        buffer += "-c"
        buffer += s"$mainClass"
        buffer += s"${mainResource.get}"
        buffer.toArray

      case _ =>
        val buffer = new ArrayBuffer[String]()
        buffer += executable

        val memory = conf.get(ENGINE_FLINK_MEMORY)
        buffer += s"-Xmx$memory"
        val javaOptions = conf.get(ENGINE_FLINK_JAVA_OPTIONS)
        if (javaOptions.isDefined) {
          buffer += javaOptions.get
        }

        buffer += "-cp"
        val classpathEntries = new java.util.LinkedHashSet[String]
        // flink engine runtime jar
        mainResource.foreach(classpathEntries.add)
        // flink sql client jar
        val flinkSqlClientPath = Paths.get(flinkHome)
          .resolve("opt")
          .toFile
          .listFiles(new FilenameFilter {
            override def accept(dir: File, name: String): Boolean = {
              name.toLowerCase.startsWith("flink-sql-client")
            }
          }).head.getAbsolutePath
        classpathEntries.add(flinkSqlClientPath)

        // jars from flink lib
        classpathEntries.add(s"$flinkHome${File.separator}lib${File.separator}*")

        // classpath contains flink configurations, default to flink.home/conf
        classpathEntries.add(env.getOrElse("FLINK_CONF_DIR", s"$flinkHome${File.separator}conf"))
        // classpath contains hadoop configurations
        env.get("HADOOP_CONF_DIR").foreach(classpathEntries.add)
        env.get("YARN_CONF_DIR").foreach(classpathEntries.add)
        env.get("HBASE_CONF_DIR").foreach(classpathEntries.add)
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
        buffer += classpathEntries.asScala.mkString(File.pathSeparator)
        buffer += mainClass

        buffer += "--conf"
        buffer += s"$KYUUBI_SESSION_USER_KEY=$proxyUser"

        for ((k, v) <- conf.getAll) {
          buffer += "--conf"
          buffer += s"$k=$v"
        }
        buffer.toArray
    }
  }

  override def shortName: String = "flink"
}

object FlinkProcessBuilder {
  final val FLINK_EXEC_FILE = "flink"
  final val YARN_APP_KEY = "yarn.application.name"
  final val YARN_TAG_KEY = "yarn.tags"
  final val FLINK_HADOOP_CLASSPATH_KEY = "FLINK_HADOOP_CLASSPATH"
  final val FLINK_PROXY_USER_KEY = "HADOOP_PROXY_USER"
}
