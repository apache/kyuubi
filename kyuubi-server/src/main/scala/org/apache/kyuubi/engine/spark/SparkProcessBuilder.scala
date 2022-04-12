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

import java.io.{File, IOException}
import java.nio.file.Paths

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.kyuubi._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.AuthTypes
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.util.KyuubiHadoopUtils

class SparkProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    val extraEngineLog: Option[OperationLog] = None)
  extends ProcBuilder with Logging {

  import SparkProcessBuilder._

  val yarnClient = getYarnClient

  def getYarnClient: YarnClient = YarnClient.createYarnClient

  override protected val executable: String = {
    val sparkHome = getEngineHome("spark")
    Paths.get(sparkHome, "bin", SPARK_SUBMIT_FILE).toFile.getCanonicalPath
  }

  override def mainClass: String = "org.apache.kyuubi.engine.spark.SparkSQLEngine"

  override protected def commands: Array[String] = {
    val buffer = new ArrayBuffer[String]()
    buffer += executable
    buffer += CLASS
    buffer += mainClass

    var allConf = conf.getAll

    // if enable sasl kerberos authentication for zookeeper, need to upload the server ketab file
    if (AuthTypes.withName(conf.get(HighAvailabilityConf.HA_ZK_ENGINE_AUTH_TYPE))
        == AuthTypes.KERBEROS) {
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

    // iff the keytab is specified, PROXY_USER is not supported
    if (!useKeytab()) {
      buffer += PROXY_USER
      buffer += proxyUser
    }

    mainResource.foreach { r => buffer += r }

    buffer.toArray
  }

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
        try {
          val yarnConf = new YarnConfiguration(KyuubiHadoopUtils.newHadoopConf(conf))
          yarnClient.init(yarnConf)
          yarnClient.start()
          val applicationId = ApplicationId.fromString(appId)
          yarnClient.killApplication(applicationId)
          s"Killed Application $appId successfully."
        } catch {
          case e: Throwable =>
            s"Failed to kill Application $appId, please kill it manually." +
              s" Caused by ${e.getMessage}."
        } finally {
          if (yarnClient != null) {
            yarnClient.stop()
          }
        }
      case None => ""
    }

  override protected def shortName: String = "spark"

  protected def getSparkDefaultsConf(): Map[String, String] = {
    val sparkDefaultsConfFile = env.get(SPARK_CONF_DIR)
      .orElse(env.get(SPARK_HOME).map(_ + File.separator + "conf"))
      .map(_ + File.separator + SPARK_CONF_FILE_NAME)
      .map(new File(_)).filter(_.exists())
    Utils.getPropertiesFromFile(sparkDefaultsConfFile)
  }
}

object SparkProcessBuilder {
  final val APP_KEY = "spark.app.name"
  final val TAG_KEY = "spark.yarn.tags"
  final val MASTER_KEY = "spark.master"

  final private[spark] val CONF = "--conf"
  final private[spark] val CLASS = "--class"
  final private[spark] val PROXY_USER = "--proxy-user"
  final private[spark] val SPARK_FILES = "spark.files"
  final private[spark] val SPARK_JARS = "spark.jars"
  final private[spark] val PRINCIPAL = "spark.kerberos.principal"
  final private[spark] val KEYTAB = "spark.kerberos.keytab"
  // Get the appropriate spark-submit file
  final private val SPARK_SUBMIT_FILE = if (Utils.isWindows) "spark-submit.cmd" else "spark-submit"
  final private val SPARK_HOME = "SPARK_HOME"
  final private val SPARK_CONF_DIR = "SPARK_CONF_DIR"
  final private val SPARK_CONF_FILE_NAME = "spark-defaults.conf"
}
