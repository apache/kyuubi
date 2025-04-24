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

package org.apache.kyuubi.engine

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.{FinalApplicationStatus, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_YARN_SUBMIT_TIMEOUT, YarnUserStrategy}
import org.apache.kyuubi.config.KyuubiConf.YarnUserStrategy._
import org.apache.kyuubi.engine.ApplicationOperation._
import org.apache.kyuubi.engine.ApplicationState.ApplicationState
import org.apache.kyuubi.engine.YarnApplicationOperation.toApplicationState
import org.apache.kyuubi.server.metadata.MetadataManager
import org.apache.kyuubi.util.KyuubiHadoopUtils

class YarnApplicationOperation extends ApplicationOperation with Logging {

  private var yarnConf: Configuration = _
  @volatile private var adminYarnClient: Option[YarnClient] = None
  private var submitTimeout: Long = _

  override def initialize(conf: KyuubiConf, metadataManager: Option[MetadataManager]): Unit = {
    submitTimeout = conf.get(KyuubiConf.ENGINE_YARN_SUBMIT_TIMEOUT)
    yarnConf = KyuubiHadoopUtils.newYarnConfiguration(conf)

    def createYarnClientWithCurrentUser(): Unit = {
      val c = createYarnClient(yarnConf)
      info(s"Creating admin YARN client with current user: ${Utils.currentUser}.")
      adminYarnClient = Some(c)
    }

    def createYarnClientWithProxyUser(proxyUser: String): Unit = Utils.doAs(proxyUser) { () =>
      val c = createYarnClient(yarnConf)
      info(s"Creating admin YARN client with proxy user: $proxyUser.")
      adminYarnClient = Some(c)
    }

    YarnUserStrategy.withName(conf.get(KyuubiConf.YARN_USER_STRATEGY)) match {
      case NONE =>
        createYarnClientWithCurrentUser()
      case ADMIN if conf.get(KyuubiConf.YARN_USER_ADMIN) == Utils.currentUser =>
        createYarnClientWithCurrentUser()
      case ADMIN =>
        createYarnClientWithProxyUser(conf.get(KyuubiConf.YARN_USER_ADMIN))
      case OWNER =>
        info("Skip initializing admin YARN client")
    }
  }

  private def createYarnClient(_yarnConf: Configuration): YarnClient = {
    // YarnClient is thread-safe
    val yarnClient = YarnClient.createYarnClient()
    yarnClient.init(_yarnConf)
    yarnClient.start()
    yarnClient
  }

  private def withYarnClient[T](proxyUser: Option[String])(action: YarnClient => T): T = {
    (adminYarnClient, proxyUser) match {
      case (Some(yarnClient), _) =>
        action(yarnClient)
      case (None, Some(user)) =>
        Utils.doAs(user) { () =>
          var yarnClient: YarnClient = null
          try {
            yarnClient = createYarnClient(yarnConf)
            action(yarnClient)
          } finally {
            Utils.tryLogNonFatalError(yarnClient.close())
          }
        }
      case (None, None) =>
        throw new IllegalStateException("Methods initialize and isSupported must be called ahead")
    }
  }

  override def isSupported(appMgrInfo: ApplicationManagerInfo): Boolean =
    appMgrInfo.resourceManager.exists(_.toLowerCase(Locale.ROOT).startsWith("yarn"))

  override def killApplicationByTag(
      appMgrInfo: ApplicationManagerInfo,
      tag: String,
      proxyUser: Option[String] = None): KillResponse = withYarnClient(proxyUser) { yarnClient =>
    try {
      val reports = yarnClient.getApplications(null, null, Set(tag).asJava)
      if (reports.isEmpty) {
        (false, NOT_FOUND)
      } else {
        try {
          val applicationId = reports.get(0).getApplicationId
          yarnClient.killApplication(applicationId)
          (true, s"Succeeded to terminate: $applicationId with $tag")
        } catch {
          case e: Exception =>
            (false, s"Failed to terminate application with $tag, due to ${e.getMessage}")
        }
      }
    } catch {
      case e: Exception =>
        (
          false,
          s"Failed to get while terminating application with tag $tag, due to ${e.getMessage}")
    }
  }

  override def getApplicationInfoByTag(
      appMgrInfo: ApplicationManagerInfo,
      tag: String,
      proxyUser: Option[String] = None,
      submitTime: Option[Long] = None): ApplicationInfo = withYarnClient(proxyUser) { yarnClient =>
    debug(s"Getting application info from YARN cluster by tag: $tag")
    val reports = yarnClient.getApplications(null, null, Set(tag).asJava)
    if (reports.isEmpty) {
      debug(s"Can't find target application from YARN cluster by tag: $tag")
      submitTime match {
        case Some(_submitTime) =>
          val elapsedTime = System.currentTimeMillis - _submitTime
          if (elapsedTime < submitTimeout) {
            info(s"Wait for YARN application[tag: $tag] to be submitted, " +
              s"elapsed time: ${elapsedTime}ms, return ${ApplicationInfo.UNKNOWN} status")
            ApplicationInfo.UNKNOWN
          } else {
            error(s"Can't find target application from YARN cluster by tag: $tag, " +
              s"elapsed time: ${elapsedTime}ms exceeds ${ENGINE_YARN_SUBMIT_TIMEOUT.key}: " +
              s"${submitTimeout}ms, return ${ApplicationInfo.NOT_FOUND} status")
            ApplicationInfo.NOT_FOUND
          }
        case _ => ApplicationInfo.NOT_FOUND
      }
    } else {
      val report = reports.get(0)
      val info = ApplicationInfo(
        id = report.getApplicationId.toString,
        name = report.getName,
        state = toApplicationState(
          report.getApplicationId.toString,
          report.getYarnApplicationState,
          report.getFinalApplicationStatus),
        url = Option(report.getTrackingUrl),
        error = Option(report.getDiagnostics))
      debug(s"Successfully got application info by tag: $tag. $info")
      info
    }
  }

  override def stop(): Unit = adminYarnClient.foreach { yarnClient =>
    Utils.tryLogNonFatalError(yarnClient.stop())
  }
}

object YarnApplicationOperation extends Logging {
  def toApplicationState(
      appId: String,
      yarnAppState: YarnApplicationState,
      finalAppStatus: FinalApplicationStatus): ApplicationState = {
    (yarnAppState, finalAppStatus) match {
      case (YarnApplicationState.NEW, FinalApplicationStatus.UNDEFINED) |
          (YarnApplicationState.NEW_SAVING, FinalApplicationStatus.UNDEFINED) |
          (YarnApplicationState.SUBMITTED, FinalApplicationStatus.UNDEFINED) |
          (YarnApplicationState.ACCEPTED, FinalApplicationStatus.UNDEFINED) =>
        ApplicationState.PENDING
      case (YarnApplicationState.RUNNING, FinalApplicationStatus.UNDEFINED) |
          (YarnApplicationState.RUNNING, FinalApplicationStatus.SUCCEEDED) =>
        ApplicationState.RUNNING
      case (YarnApplicationState.FINISHED, FinalApplicationStatus.SUCCEEDED) =>
        ApplicationState.FINISHED
      case (YarnApplicationState.FINISHED, FinalApplicationStatus.FAILED) |
          (YarnApplicationState.FAILED, FinalApplicationStatus.FAILED) =>
        ApplicationState.FAILED
      case (YarnApplicationState.KILLED, FinalApplicationStatus.KILLED) =>
        ApplicationState.KILLED
      case (state, finalStatus) => // any other combination is invalid, so FAIL the application.
        error(s"Unknown YARN state $state for app $appId with final status $finalStatus.")
        ApplicationState.FAILED
    }
  }
}
