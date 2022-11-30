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

import com.google.common.annotations.VisibleForTesting
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.{KyuubiApplicationManager, ProcBuilder}
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.AuthTypes
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.util.Validator

class SparkProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    val engineRefId: String,
    val extraEngineLog: Option[OperationLog] = None)
  extends ProcBuilder with Logging {

  @VisibleForTesting
  def this(proxyUser: String, conf: KyuubiConf) {
    this(proxyUser, conf, "")
  }

  import SparkProcessBuilder._

  private[kyuubi] val sparkHome = getEngineHome(shortName)

  override protected val executable: String = {
    Paths.get(sparkHome, "bin", SPARK_SUBMIT_FILE).toFile.getCanonicalPath
  }

  override def mainClass: String = "org.apache.kyuubi.engine.spark.SparkSQLEngine"

  /**
   * Converts kyuubi config key so that Spark could identify.
   * - If the key is start with `spark.`, keep it AS IS as it is a Spark Conf
   * - If the key is start with `hadoop.`, it will be prefixed with `spark.hadoop.`
   * - Otherwise, the key will be added a `spark.` prefix
   */
  protected def convertConfigKey(key: String): String = {
    if (key.startsWith("spark.")) {
      key
    } else if (key.startsWith("hadoop.")) {
      "spark.hadoop." + key
    } else {
      "spark." + key
    }
  }

  override protected val commands: Array[String] = {
    KyuubiApplicationManager.tagApplication(engineRefId, shortName, clusterManager(), conf)
    val buffer = new ArrayBuffer[String]()
    buffer += executable
    buffer += CLASS
    buffer += mainClass

    var allConf = conf.getAll

    // if enable sasl kerberos authentication for zookeeper, need to upload the server keytab file
    if (AuthTypes.withName(conf.get(HighAvailabilityConf.HA_ZK_ENGINE_AUTH_TYPE))
        == AuthTypes.KERBEROS) {
      allConf = allConf ++ zkAuthKeytabFileConf(allConf)
    }

    allConf.foreach { case (k, v) =>
      buffer += CONF
      buffer += s"${convertConfigKey(k)}=$v"
    }

    // if the keytab is specified, PROXY_USER is not supported
    tryKeytab() match {
      case None =>
        setSparkUserName(proxyUser, buffer)
        buffer += PROXY_USER
        buffer += proxyUser
      case Some(name) =>
        setSparkUserName(name, buffer)
    }

    mainResource.foreach { r => buffer += r }

    buffer.toArray
  }

  override protected def module: String = "kyuubi-spark-sql-engine"

  private def tryKeytab(): Option[String] = {
    val principal = conf.getOption(PRINCIPAL)
    val keytab = conf.getOption(KEYTAB)
    if (principal.isEmpty || keytab.isEmpty) {
      None
    } else {
      try {
        val ugi = UserGroupInformation
          .loginUserFromKeytabAndReturnUGI(principal.get, keytab.get)
        if (ugi.getShortUserName != proxyUser) {
          warn(s"The session proxy user: $proxyUser is not same with " +
            s"spark principal: ${ugi.getShortUserName}, so we can't support use keytab. " +
            s"Fallback to use proxy user.")
          None
        } else {
          Some(ugi.getShortUserName)
        }
      } catch {
        case e: IOException =>
          error(s"Failed to login for ${principal.get}", e)
          None
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

  override def shortName: String = "spark"

  protected lazy val defaultMaster: Option[String] = {
    val confDir = env.getOrElse(SPARK_CONF_DIR, s"$sparkHome${File.separator}conf")
    val defaults =
      try {
        val confFile = new File(s"$confDir${File.separator}$SPARK_CONF_FILE_NAME")
        if (confFile.exists()) {
          Utils.getPropertiesFromFile(Some(confFile))
        } else {
          Map.empty[String, String]
        }
      } catch {
        case _: Exception =>
          warn(s"Failed to load spark configurations from $confDir")
          Map.empty[String, String]
      }
    defaults.get(MASTER_KEY)
  }

  override def clusterManager(): Option[String] = {
    conf.getOption(MASTER_KEY).orElse(defaultMaster)
  }

  override def validateConf: Unit = Validator.validateConf(conf)

  // For spark on kubernetes, spark pod using env SPARK_USER_NAME as current user
  def setSparkUserName(userName: String, buffer: ArrayBuffer[String]): Unit = {
    clusterManager().foreach { cm =>
      if (cm.toUpperCase.startsWith("K8S")) {
        buffer += CONF
        buffer += s"spark.kubernetes.driverEnv.SPARK_USER_NAME=$userName"
        buffer += CONF
        buffer += s"spark.executorEnv.SPARK_USER_NAME=$userName"
      }
    }
  }
}

object SparkProcessBuilder {
  final val APP_KEY = "spark.app.name"
  final val TAG_KEY = "spark.yarn.tags"
  final val MASTER_KEY = "spark.master"
  final val INTERNAL_RESOURCE = "spark-internal"

  /**
   * The path configs from Spark project that might upload local files:
   * - SparkSubmit
   * - org.apache.spark.deploy.yarn.Client::prepareLocalResources
   * - KerberosConfDriverFeatureStep::configurePod
   * - KubernetesUtils.uploadAndTransformFileUris
   */
  final val PATH_CONFIGS = Seq(
    SPARK_FILES,
    "spark.jars",
    "spark.archives",
    "spark.yarn.jars",
    "spark.yarn.dist.files",
    "spark.yarn.dist.pyFiles",
    "spark.submit.pyFiles",
    "spark.yarn.dist.jars",
    "spark.yarn.dist.archives",
    "spark.kerberos.keytab",
    "spark.yarn.keytab",
    "spark.kubernetes.kerberos.krb5.path",
    "spark.kubernetes.file.upload.path")

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
