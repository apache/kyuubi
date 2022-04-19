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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.kyuubi._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_INIT_TIMEOUT, ENGINE_TYPE}
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

  def getYarnClient: YarnClient = YarnClient.createYarnClient

  private val sparkHome = getEngineHome(shortName)

  override protected val executable: String = {
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

  override def killApplication(clue: Either[String, String]): String = clue match {
    case Left(engineRefId) => killApplicationByTag(engineRefId)
    case Right(_) => ""
  }

  private def killApplicationByTag(engineRefId: String): String = {
    conf.getOption(MASTER_KEY).orElse(getSparkDefaultsConf().get(MASTER_KEY)) match {
      case Some("yarn") =>
        var applicationId: ApplicationId = null
        val yarnClient = getYarnClient
        try {
          val yarnConf = new YarnConfiguration(KyuubiHadoopUtils.newHadoopConf(conf))
          yarnClient.init(yarnConf)
          yarnClient.start()
          val apps = yarnClient.getApplications(null, null, Set(engineRefId).asJava)
          if (apps.isEmpty) return s"There are no Application tagged with $engineRefId," +
            s" please kill it manually."
          applicationId = apps.asScala.head.getApplicationId
          yarnClient.killApplication(
            applicationId,
            s"Kyuubi killed this caused by: Timeout(${conf.get(ENGINE_INIT_TIMEOUT)} ms) to" +
              s" launched ${conf.get(ENGINE_TYPE)} engine with $this.")
          s"Killed Application $applicationId tagged with $engineRefId successfully."
        } catch {
          case e: Throwable =>
            s"Failed to kill Application $applicationId tagged with $engineRefId," +
              s" please kill it manually. Caused by ${e.getMessage}."
        } finally {
          yarnClient.stop()
        }
      case _ => "Kill Application only works with YARN, please kill it manually." +
          s" Application tagged with $engineRefId"
    }
  }

  override protected def shortName: String = "spark"

  protected def getSparkDefaultsConf(): Map[String, String] = {
    val sparkDefaultsConfFile = env.get(SPARK_CONF_DIR)
      .orElse(Option(s"$sparkHome${File.separator}conf"))
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
  final private[spark] val PRINCIPAL = "spark.kerberos.principal"
  final private[spark] val KEYTAB = "spark.kerberos.keytab"
  // Get the appropriate spark-submit file
  final private val SPARK_SUBMIT_FILE = if (Utils.isWindows) "spark-submit.cmd" else "spark-submit"
  final private val SPARK_CONF_DIR = "SPARK_CONF_DIR"
  final private val SPARK_CONF_FILE_NAME = "spark-defaults.conf"
}
