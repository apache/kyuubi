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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.google.common.annotations.VisibleForTesting

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

  override protected def module: String = "kyuubi-flink-sql-engine"

  override protected def mainClass: String = "org.apache.kyuubi.engine.flink.FlinkSQLEngine"

  override def env: Map[String, String] = conf.getEnvs +
    (FLINK_PROXY_USER_KEY -> proxyUser)

  override protected val commands: Array[String] = {
    KyuubiApplicationManager.tagApplication(engineRefId, shortName, clusterManager(), conf)
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
      warn(s"The conf of ${FLINK_HADOOP_CLASSPATH_KEY} and ${ENGINE_FLINK_EXTRA_CLASSPATH.key}" +
        s" is empty.")
      debug("Detected development environment")
      mainResource.foreach { path =>
        val devHadoopJars = Paths.get(path).getParent
          .resolve(s"scala-$SCALA_COMPILE_VERSION")
          .resolve("jars")
        if (!Files.exists(devHadoopJars)) {
          throw new KyuubiException(s"The path $devHadoopJars does not exists. " +
            s"Please set ${FLINK_HADOOP_CLASSPATH_KEY} or ${ENGINE_FLINK_EXTRA_CLASSPATH.key} " +
            s"for configuring location of hadoop client jars, etc")
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

  override def shortName: String = "flink"
}

object FlinkProcessBuilder {
  final val APP_KEY = "yarn.application.name"
  final val TAG_KEY = "yarn.tags"
  final val FLINK_HADOOP_CLASSPATH_KEY = "FLINK_HADOOP_CLASSPATH"
  final val FLINK_PROXY_USER_KEY = "HADOOP_PROXY_USER"
}
