/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.yarn

import java.io.{File, IOException}
import java.util.concurrent.{Executors, TimeUnit}

import scala.util.control.NonFatal

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.{ApplicationAttemptId, FinalApplicationStatus}
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.conf.YarnConfiguration._
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf}

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.service.CompositeService

/**
 * The ApplicationMaster which runs Kyuubi Server inside it.
 */
class KyuubiAppMaster private(name: String) extends CompositeService(name)
  with Logging {

  def this() = this(classOf[KyuubiAppMaster].getSimpleName)
  private lazy val amRMClient = AMRMClient.createAMRMClient()

  private[this] var yarnConf: YarnConfiguration = _
  private[this] val server: KyuubiServer = new KyuubiServer()
  private[this] var interval: Int = _
  private[this] var amMaxAttempts: Int = _

  @volatile private[this] var amStatus = FinalApplicationStatus.SUCCEEDED
  @volatile private[this] var finalMsg = ""

  private[this] var failureCount = 0

  private[this] val heartbeatTask = new Runnable {
    override def run(): Unit = {
      try {
        amRMClient.allocate(0.1f)
        failureCount = 0
      } catch {
        case _: InterruptedException =>
        case e: ApplicationAttemptNotFoundException =>
          failureCount += 1
          error(s"Exception from heartbeat thread. Tried $failureCount time(s)", e)
          stop(FinalApplicationStatus.FAILED, e.getMessage)
        case e: Throwable =>
          failureCount += 1
          val msg = s"Heartbeat thread fails for $failureCount times in a row. "
          if (!NonFatal(e) || failureCount >= amMaxAttempts + 2) {
            stop(FinalApplicationStatus.FAILED, msg + e.getMessage)
          } else {
            warn(msg, e)
          }
      }
    }
  }

  private[this] val executor = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("AM-RM-Heartbeat" + "-%d").build())

  private[this] def getAppAttemptId: ApplicationAttemptId = {
    val containerIdString = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name())
    ConverterUtils.toContainerId(containerIdString).getApplicationAttemptId
  }

  /**
   * Clean up the staging directory.
   */
  private[this] def cleanupStagingDir(): Unit = {
    var stagingDirPath: Path = null
    try {
      if (conf.getBoolean("spark.kyuubi.staging.cleanable", defaultValue = true)) {
        stagingDirPath = new Path(System.getenv("KYUUBI_YARN_STAGING_DIR"))
        info("Deleting staging directory " + stagingDirPath)
        val fs = stagingDirPath.getFileSystem(yarnConf)
        fs.delete(stagingDirPath, true)
      }
    } catch {
      case ioe: IOException =>
        error("Failed to cleanup staging dir " + stagingDirPath, ioe)
    }
  }

  private[this] def stop(stat: FinalApplicationStatus, msg: String): Unit = {
    amStatus = stat
    finalMsg = msg
    executor.shutdownNow()
    if (getAppAttemptId.getAttemptId >= amMaxAttempts) {
      amRMClient.unregisterApplicationMaster(amStatus, finalMsg, "")
      cleanupStagingDir()
    }
    amRMClient.stop()
    System.exit(-1)
  }

  override def init(conf: SparkConf): Unit = {
    try {
      info(s"Service: [$name] is initializing.")
      this.conf = conf
      yarnConf = new YarnConfiguration(KyuubiSparkUtil.newConfiguration(conf))
      amMaxAttempts = yarnConf.getInt(RM_AM_MAX_ATTEMPTS, DEFAULT_RM_AM_MAX_ATTEMPTS)
      amRMClient.init(yarnConf)
      amRMClient.start()

      val principal = conf.get(KyuubiSparkUtil.PRINCIPAL, "")
      val keytab = conf.get(KyuubiSparkUtil.KEYTAB, "")
      if (principal.nonEmpty && keytab.nonEmpty && new File(keytab).exists()) {
        conf.set(KyuubiSparkUtil.METASTORE_KEYTAB, keytab)
        conf.set(KyuubiSparkUtil.METASTORE_PRINCIPAL, principal)
        UserGroupInformation.loginUserFromKeytab(principal, keytab)
      }

      addService(server)
      super.init(conf)
    } catch {
      case e: Exception =>
        val msg = "Error initializing Kyuubi Server: "
        error(msg, e)
        stop(FinalApplicationStatus.FAILED, msg + e.getMessage)
    }
  }

  override def start(): Unit = {
    try {
      info(s"Service: [$name] is starting.")
      // TODO:(Kent) Add App tracking url
      val feService = server.feService
      amRMClient.registerApplicationMaster(
        feService.getServerIPAddress.getHostName,
        feService.getPortNumber, "")
      val expiryInterval = yarnConf.getInt(RM_AM_EXPIRY_INTERVAL_MS, 120000)
      interval = math.max(100 * 1000, math.min(expiryInterval / 2, 3000))
      info(s"Scheduling ApplicationMaster heartbeat in every $interval ms")
      executor.scheduleAtFixedRate(heartbeatTask, interval, interval, TimeUnit.MILLISECONDS)
      super.start()
      info(server.getName + " started!")
    } catch {
      case e: Exception =>
        val msg = "Error starting Kyuubi Server: "
        error(msg, e)
        stop(FinalApplicationStatus.FAILED, msg + e.getMessage)
    }
  }

  override def stop(): Unit = server.stop()
}

object KyuubiAppMaster extends Logging {

  def main(args: Array[String]): Unit = {
    try {
      KyuubiSparkUtil.initDaemon(logger)
      KyuubiSparkUtil.getAndSetKyuubiFirstClassLoader
      val conf = new SparkConf()

      conf.set(KyuubiConf.FRONTEND_BIND_PORT.key, "0")
      val sparkJars = System.getenv("KYUUBI_YARN_SPARK_JARS")
      if (sparkJars != null) {
        info(s"Setting ${KyuubiSparkUtil.SPARK_YARN_JARS} to $sparkJars")
        conf.set(KyuubiSparkUtil.SPARK_YARN_JARS, sparkJars)
      }
      val appMasterArgs = AppMasterArguments(args)
      appMasterArgs.propertiesFile.map(KyuubiSparkUtil.getPropertiesFromFile) match {
        case Some(props) => props.foreach { case (k, v) =>
          conf.set(k, v)
          sys.props(k) = v
        }
        case _ => warn("Failed to load property file.")
      }
      KyuubiSparkUtil.setupCommonConfig(conf)

      val master = new KyuubiAppMaster()
      KyuubiSparkUtil.addShutdownHook(master.stop())
      master.init(conf)
      master.start()
    } catch {
      case e: Exception =>
        error("Uncaught exception: ", e)
        System.exit(-1)
    }
  }
}
