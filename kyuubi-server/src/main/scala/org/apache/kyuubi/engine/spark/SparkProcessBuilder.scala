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

import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ProcBuilder
import org.apache.kyuubi.ha.HighAvailabilityConf
import org.apache.kyuubi.ha.client.AuthTypes
import org.apache.kyuubi.operation.log.OperationLog

class SparkProcessBuilder(
    override val proxyUser: String,
    override val conf: KyuubiConf,
    val extraEngineLog: Option[OperationLog] = None)
  extends ProcBuilder with Logging {

  import SparkProcessBuilder._

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

    // if enable sasl kerberos authentication for zookeeper, need to upload the server keytab file
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
