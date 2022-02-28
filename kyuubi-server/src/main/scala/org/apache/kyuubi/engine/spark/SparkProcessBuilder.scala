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

package org.apache.kyuubi.engine.spark

import java.io.{File, FilenameFilter, IOException}
import java.lang.ProcessBuilder.Redirect
import java.net.URI
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SPARK_MAIN_RESOURCE, FRONTEND_THRIFT_BINARY_BIND_HOST}
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.ZooKeeperAuthTypes
import org.apache.kyuubi.operation.log.OperationLog

class SparkProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    val extraEngineLog: Option[OperationLog] = None)
  extends ProcBuilder with Logging {

  import SparkProcessBuilder._

  override protected val executable: String = {
    Paths.get(
      resolveSparkHome(env),
      "bin",
      SPARK_SUBMIT_FILE).toFile.getAbsolutePath
  }

  override def mainClass: String = "org.apache.kyuubi.engine.spark.SparkSQLEngine"

  override def mainResource: Option[String] = {
    // 1. get the main resource jar for user specified config first
    // TODO use SPARK_SCALA_VERSION instead of SCALA_COMPILE_VERSION
    val jarName = s"${module}_$SCALA_COMPILE_VERSION-$KYUUBI_VERSION.jar"
    conf.get(ENGINE_SPARK_MAIN_RESOURCE).filter { userSpecified =>
      // skip check exist if not local file.
      val uri = new URI(userSpecified)
      val schema = if (uri.getScheme != null) uri.getScheme else "file"
      schema match {
        case "file" => Files.exists(Paths.get(userSpecified))
        case _ => true
      }
    }.orElse {
      // 2. get the main resource jar from system build default
      env.get(KyuubiConf.KYUUBI_HOME)
        .map { Paths.get(_, "externals", "engines", "spark", jarName) }
        .filter(Files.exists(_)).map(_.toAbsolutePath.toFile.getCanonicalPath)
    }.orElse {
      // 3. get the main resource from dev environment
      Option(Paths.get("externals", module, "target", jarName))
        .filter(Files.exists(_)).orElse {
          Some(Paths.get("..", "externals", module, "target", jarName))
        }.map(_.toAbsolutePath.toFile.getCanonicalPath)
    }
  }

  override protected def commands: Array[String] = {
    val buffer = new ArrayBuffer[String]()
    buffer += executable
    buffer += CLASS
    buffer += mainClass

    var allConf = conf.getAll

    // if enable sasl kerberos authentication for zookeeper, need to upload the server ketab file
    if (ZooKeeperAuthTypes.withName(conf.get(HighAvailabilityConf.HA_ZK_ENGINE_AUTH_TYPE))
        == ZooKeeperAuthTypes.KERBEROS) {
      allConf = allConf ++ zkAuthKeytabFileConf(allConf)
    }

    /**
     * Converts kyuubi configs to configs that Spark could identify.
     * - If the key is start with `spark.`, keep it AS IS as it is a Spark Conf
     * - If the key is start with `hadoop.`, it will be prefixed with `spark.hadoop.`
     * - Otherwise, the key will be added a `spark.` prefix
     */
    allConf.foreach { case (k, v) =>
      val newKey =
        if (k.startsWith("spark.")) {
          k
        } else if (k.startsWith("hadoop.")) {
          "spark.hadoop." + k
        } else {
          "spark." + k
        }
      buffer += CONF
      buffer += s"$newKey=$v"
    }

    /**
     * Kyuubi respect user setting config, if user set `spark.driver.host`, will pass it on.
     * If user don't set this, will use thrift binary bind host to set.
     * Kyuubi wants the Engine to bind hostName or IP with Kyuubi.
     * Spark driver will pass this configuration as the driver-url to the executors
     * to build RPC communication.
     */
    if (!allConf.contains("spark.driver.host")) {
      conf.get(FRONTEND_THRIFT_BINARY_BIND_HOST).foreach(host => {
        buffer += CONF
        buffer += s"spark.driver.host=${host}"
      })
    }

    // iff the keytab is specified, PROXY_USER is not supported
    if (!useKeytab()) {
      buffer += PROXY_USER
      buffer += proxyUser
    }

    mainResource.foreach { r => buffer += r }

    buffer.toArray
  }

  override def toString: String = commands.map {
    case arg if arg.startsWith("--") => s"\\\n\t$arg"
    case arg => arg
  }.mkString(" ")

  override protected def module: String = "kyuubi-spark-sql-engine"

  val YARN_APP_NAME_REGEX: Regex = "application_\\d+_\\d+".r

  private def useKeytab(): Boolean = {
    val principal = conf.getOption(PRINCIPAL)
    val keytab = conf.getOption(KEYTAB)
    if (principal.isEmpty || keytab.isEmpty) {
      false
    } else {
      try {
        val ugi = UserGroupInformation
          .loginUserFromKeytabAndReturnUGI(principal.get, keytab.get)
        val keytabEnabled = ugi.getShortUserName == proxyUser
        if (!keytabEnabled) {
          warn(s"The session proxy user: $proxyUser is not same with " +
            s"spark principal: ${ugi.getShortUserName}, so we can't support use keytab. " +
            s"Fallback to use proxy user.")
        }
        keytabEnabled
      } catch {
        case e: IOException =>
          error(s"Failed to login for ${principal.get}", e)
          false
      }
    }
  }

  private def zkAuthKeytabFileConf(sparkConf: Map[String, String]): Map[String, String] = {
    val zkAuthKeytab = conf.get(HighAvailabilityConf.HA_ZK_AUTH_KEYTAB)
    if (zkAuthKeytab.isDefined) {
      sparkConf.get(SPARK_FILES) match {
        case Some(files) =>
          Map(SPARK_FILES -> s"$files,${zkAuthKeytab.get}")
        case _ =>
          Map(SPARK_FILES -> zkAuthKeytab.get)
      }
    } else {
      Map()
    }
  }

  override def killApplication(line: String = lastRowsOfLog.toArray.mkString("\n")): String =
    YARN_APP_NAME_REGEX.findFirstIn(line) match {
      case Some(appId) =>
        env.get(KyuubiConf.KYUUBI_HOME) match {
          case Some(kyuubiHome) =>
            val pb = new ProcessBuilder("/bin/sh", s"$kyuubiHome/bin/stop-application.sh", appId)
            pb.environment()
              .putAll(childProcEnv.asJava)
            pb.redirectError(Redirect.appendTo(engineLog))
            pb.redirectOutput(Redirect.appendTo(engineLog))
            val process = pb.start()
            process.waitFor() match {
              case id if id != 0 => s"Failed to kill Application $appId, please kill it manually. "
              case _ => s"Killed Application $appId successfully. "
            }
          case None =>
            s"KYUUBI_HOME is not set! Failed to kill Application $appId, please kill it manually."
        }
      case None => ""
    }

}

object SparkProcessBuilder {
  final val APP_KEY = "spark.app.name"
  final val TAG_KEY = "spark.yarn.tags"

  final val CONF = "--conf"
  final val CLASS = "--class"
  final private val PROXY_USER = "--proxy-user"
  final private val SPARK_FILES = "spark.files"
  final private val PRINCIPAL = "spark.kerberos.principal"
  final private val KEYTAB = "spark.kerberos.keytab"
  // Get the appropriate spark-submit file
  final private val SPARK_SUBMIT_FILE = if (Utils.isWindows) "spark-submit.cmd" else "spark-submit"

  def resolveSparkHome(env: Map[String, String]): String = {
    env.get("SPARK_HOME").orElse {
      val cwd = getClass.getProtectionDomain.getCodeSource.getLocation.getPath
        .split("kyuubi-server")
      assert(cwd.length > 1)
      Option(
        Paths.get(cwd.head)
          .resolve("externals")
          .resolve("kyuubi-download")
          .resolve("target")
          .toFile
          .listFiles(new FilenameFilter {
            override def accept(dir: File, name: String): Boolean = {
              dir.isDirectory && name.startsWith("spark-")
            }
          }))
        .flatMap(_.headOption)
        .map(_.getAbsolutePath)
    }.getOrElse {
      throw KyuubiSQLException("SPARK_HOME is not set! " +
        "For more detail information on installing and configuring Spark, please visit " +
        "https://kyuubi.apache.org/docs/stable/deployment/settings.html#environments")
    }
  }
}
