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

import scala.collection.mutable

import com.google.common.annotations.VisibleForTesting
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.{KyuubiException, Logging, SCALA_COMPILE_VERSION, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_DEPLOY_YARN_MODE_APP_NAME, ENGINE_JDBC_CONNECTION_PASSWORD, ENGINE_JDBC_CONNECTION_URL, ENGINE_JDBC_DEPLOY_MODE, ENGINE_JDBC_EXTRA_CLASSPATH, ENGINE_JDBC_JAVA_OPTIONS, ENGINE_JDBC_MEMORY, ENGINE_KEYTAB, ENGINE_PRINCIPAL}
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.engine.deploy.DeployMode
import org.apache.kyuubi.engine.deploy.DeployMode.{LOCAL, YARN}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.util.command.CommandLineUtils._

class JdbcProcessBuilder(
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

  /**
   * The short name of the engine process builder, we use this for form the engine jar paths now
   * see `mainResource`
   */
  override def shortName: String = "jdbc"

  override protected def module: String = "kyuubi-jdbc-engine"

  /**
   * The class containing the main method
   */
  override protected def mainClass: String = "org.apache.kyuubi.engine.jdbc.JdbcSQLEngine"

  override protected val commands: Iterable[String] = {
    require(
      conf.get(ENGINE_JDBC_CONNECTION_URL).nonEmpty,
      s"Jdbc server url can not be null! Please set ${ENGINE_JDBC_CONNECTION_URL.key}")
    val buffer = new mutable.ListBuffer[String]()
    buffer += executable

    val memory = conf.get(ENGINE_JDBC_MEMORY)
    buffer += s"-Xmx$memory"

    val javaOptions = conf.get(ENGINE_JDBC_JAVA_OPTIONS).filter(StringUtils.isNotBlank(_))
    if (javaOptions.isDefined) {
      buffer ++= parseOptionString(javaOptions.get)
    }
    val classpathEntries = new mutable.LinkedHashSet[String]
    mainResource.foreach(classpathEntries.add)
    mainResource.foreach { path =>
      val parent = Paths.get(path).getParent
      if (Utils.isTesting) {
        // add dev classpath
        val jdbcDeps = parent
          .resolve(s"scala-$SCALA_COMPILE_VERSION")
          .resolve("jars")
        classpathEntries.add(s"$jdbcDeps${File.separator}*")
      } else {
        // add prod classpath
        classpathEntries.add(s"$parent${File.separator}*")
      }
    }

    val extraCp = conf.get(ENGINE_JDBC_EXTRA_CLASSPATH)
    extraCp.foreach(classpathEntries.add)
    buffer ++= genClasspathOption(classpathEntries)

    buffer += mainClass

    buffer ++= confKeyValue(KYUUBI_SESSION_USER_KEY, proxyUser)

    buffer ++= confKeyValues(conf.getAll)

    buffer
  }

  override def toString: String = {
    if (commands == null) {
      super.toString
    } else {
      redactConfValues(
        Utils.redactCommandLineArgs(conf, commands),
        Set(ENGINE_JDBC_CONNECTION_PASSWORD.key)).map {
        case arg if arg.startsWith("-") => s"\\\n\t$arg"
        case arg => arg
      }.mkString(" ")
    }
  }
}

object JdbcProcessBuilder extends Logging {

  final val JDBC_ENGINE_NAME = "jdbc.engine.name"

  def apply(
      proxyUser: String,
      doAsEnabled: Boolean,
      conf: KyuubiConf,
      engineRefId: String,
      extraEngineLog: Option[OperationLog],
      defaultEngineName: String): JdbcProcessBuilder = {
    checkKeytab(proxyUser, conf)
    DeployMode.withName(conf.get(ENGINE_JDBC_DEPLOY_MODE)) match {
      case LOCAL =>
        new JdbcProcessBuilder(proxyUser, doAsEnabled, conf, engineRefId, extraEngineLog)
      case YARN =>
        warn(s"JDBC on YARN model is experimental.")
        conf.setIfMissing(ENGINE_DEPLOY_YARN_MODE_APP_NAME, Some(defaultEngineName))
        new JdbcYarnModeProcessBuilder(proxyUser, doAsEnabled, conf, engineRefId, extraEngineLog)
      case other => throw new KyuubiException(s"Unsupported deploy mode: $other")
    }
  }

  private def checkKeytab(proxyUser: String, conf: KyuubiConf): Unit = {
    val principal = conf.get(ENGINE_PRINCIPAL)
    val keytab = conf.get(ENGINE_KEYTAB)
    if (!UserGroupInformation.isSecurityEnabled) {
      if (principal.isDefined || keytab.isDefined) {
        warn("Principal and keytab takes no effect when hadoop security is not enabled.")
      }
      return
    }

    require(
      principal.isDefined == keytab.isDefined,
      s"Both principal and keytab must be defined, or neither.")
    if (principal.isDefined && keytab.isDefined) {
      val ugi = UserGroupInformation
        .loginUserFromKeytabAndReturnUGI(principal.get, keytab.get)
      require(
        ugi.getShortUserName == proxyUser,
        s"Proxy user: $proxyUser is not same with " +
          s"engine principal: ${ugi.getShortUserName}.")
    }

    val deployMode = DeployMode.withName(conf.get(ENGINE_JDBC_DEPLOY_MODE))
    if (principal.isEmpty && keytab.isEmpty && deployMode == YARN) {
      warn("JDBC on YARN can not work properly without principal and keytab.")
    }
  }
}
