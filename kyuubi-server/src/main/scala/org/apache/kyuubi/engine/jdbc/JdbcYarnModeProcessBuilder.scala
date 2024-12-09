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
package org.apache.kyuubi.engine.jdbc

import java.io.File
import java.nio.file.Paths
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.{Logging, SCALA_COMPILE_VERSION, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_JDBC_EXTRA_CLASSPATH, ENGINE_JDBC_MEMORY}
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_ENGINE_ID, KYUUBI_SESSION_USER_KEY}
import org.apache.kyuubi.engine.{ApplicationManagerInfo, KyuubiApplicationManager}
import org.apache.kyuubi.engine.deploy.yarn.EngineYarnModeSubmitter._
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.util.command.CommandLineUtils.{confKeyValue, confKeyValues}

/**
 * A process builder for JDBC on Yarn.
 *
 * It will new a process on kyuubi server side to submit jdbc engine to yarn.
 */
class JdbcYarnModeProcessBuilder(
    override val proxyUser: String,
    override val doAsEnabled: Boolean,
    override val conf: KyuubiConf,
    override val engineRefId: String,
    override val extraEngineLog: Option[OperationLog] = None)
  extends JdbcProcessBuilder(proxyUser, doAsEnabled, conf, engineRefId, extraEngineLog)
  with Logging {

  override protected def mainClass: String =
    "org.apache.kyuubi.engine.jdbc.deploy.JdbcYarnModeSubmitter"

  override def isClusterMode(): Boolean = true

  override def clusterManager(): Option[String] = Some("yarn")

  override def appMgrInfo(): ApplicationManagerInfo = ApplicationManagerInfo(clusterManager())

  override protected val commands: Iterable[String] = {
    KyuubiApplicationManager.tagApplication(engineRefId, shortName, clusterManager(), conf)
    val buffer = new ArrayBuffer[String]()
    buffer += executable

    val memory = conf.get(ENGINE_JDBC_MEMORY)
    buffer += s"-Xmx$memory"
    buffer += "-cp"

    val classpathEntries = new util.LinkedHashSet[String]
    classpathEntries.addAll(hadoopConfFiles())
    classpathEntries.addAll(yarnConfFiles())
    classpathEntries.addAll(jarFiles(true))

    buffer += classpathEntries.asScala.mkString(File.pathSeparator)
    buffer += mainClass

    buffer ++= confKeyValue(KYUUBI_SESSION_USER_KEY, proxyUser)
    buffer ++= confKeyValue(KYUUBI_ENGINE_ID, engineRefId)

    buffer ++= confKeyValue(
      KYUUBI_ENGINE_DEPLOY_YARN_MODE_JARS_KEY,
      jarFiles(false).asScala.mkString(KYUUBI_ENGINE_DEPLOY_YARN_MODE_ARCHIVE_SEPARATOR))
    buffer ++= confKeyValue(
      KYUUBI_ENGINE_DEPLOY_YARN_MODE_HADOOP_CONF_KEY,
      hadoopConfFiles().asScala.mkString(KYUUBI_ENGINE_DEPLOY_YARN_MODE_ARCHIVE_SEPARATOR))
    buffer ++= confKeyValue(
      KYUUBI_ENGINE_DEPLOY_YARN_MODE_YARN_CONF_KEY,
      yarnConfFiles().asScala.mkString(KYUUBI_ENGINE_DEPLOY_YARN_MODE_ARCHIVE_SEPARATOR))

    buffer ++= confKeyValues(conf.getAll)

    buffer
  }

  private def jarFiles(isClasspath: Boolean): util.LinkedHashSet[String] = {
    val jarEntries = new util.LinkedHashSet[String]
    mainResource.foreach(jarEntries.add)
    val javaOptions = conf.get(ENGINE_JDBC_EXTRA_CLASSPATH).filter(StringUtils.isNotBlank(_))
    if (isClasspath) {
      javaOptions.foreach(jarEntries.add)
    }
    mainResource.foreach { path =>
      val parent = Paths.get(path).getParent
      if (Utils.isTesting) {
        // add dev classpath
        val jdbcDeps = parent
          .resolve(s"scala-$SCALA_COMPILE_VERSION")
          .resolve("jars")
        jarEntries.add(s"$jdbcDeps${appendClasspathSuffix(isClasspath)}")
      } else {
        // add prod classpath
        jarEntries.add(s"$parent${appendClasspathSuffix(isClasspath)}")
      }
    }
    jarEntries
  }

  private def hadoopConfFiles(): util.LinkedHashSet[String] = {
    val confEntries = new util.LinkedHashSet[String]
    env.get("HADOOP_CONF_DIR").foreach(confEntries.add)
    confEntries
  }

  private def yarnConfFiles(): util.LinkedHashSet[String] = {
    val confEntries = new util.LinkedHashSet[String]
    env.get("YARN_CONF_DIR").foreach(confEntries.add)
    confEntries
  }

  private def appendClasspathSuffix(isClasspath: Boolean): String = {
    if (isClasspath) {
      s"${File.separator}*"
    } else {
      ""
    }
  }
}
