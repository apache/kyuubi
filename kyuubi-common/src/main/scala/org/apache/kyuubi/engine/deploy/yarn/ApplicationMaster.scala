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
package org.apache.kyuubi.engine.deploy.yarn

import java.io.{File, IOException}
import java.security.PrivilegedExceptionAction

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier

import org.apache.kyuubi.{KyuubiException, Logging, Utils}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiReservedKeys}
import org.apache.kyuubi.service.Serverable
import org.apache.kyuubi.util.KyuubiHadoopUtils
import org.apache.kyuubi.util.command.CommandLineUtils.confKeyValues
import org.apache.kyuubi.util.reflect.{DynFields, DynMethods}

object ApplicationMaster extends Logging {

  private var amClient: AMRMClient[ContainerRequest] = _
  private var yarnConf: YarnConfiguration = _

  private val kyuubiConf = new KyuubiConf()

  private var currentEngineMainClass: String = _

  private var currentEngine: Serverable = _

  private var finalMsg: String = _

  @volatile private var registered: Boolean = false
  @volatile private var unregistered: Boolean = false
  @volatile private var finalStatus = FinalApplicationStatus.UNDEFINED

  def main(args: Array[String]): Unit = {
    try {
      val amArgs = new ApplicationMasterArguments(args)
      Utils.getPropertiesFromFile(Some(new File(amArgs.propertiesFile))).foreach { case (k, v) =>
        kyuubiConf.set(k, v)
      }
      currentEngineMainClass = amArgs.engineMainClass
      yarnConf = KyuubiHadoopUtils.newYarnConfiguration(kyuubiConf)
      Utils.addShutdownHook(() => {
        if (!unregistered) {
          if (currentEngine != null && currentEngine.selfExited) {
            finalMsg = "Kyuubi Application Master is shutting down."
            finalStatus = FinalApplicationStatus.SUCCEEDED
          } else {
            finalMsg = "Kyuubi Application Master is shutting down with error."
            finalStatus = FinalApplicationStatus.FAILED
          }
          cleanupStagingDir()
          unregister(finalStatus, finalMsg)
        }
      })

      val ugi = kyuubiConf.get(KyuubiConf.ENGINE_PRINCIPAL) match {
        case Some(principalName) if UserGroupInformation.isSecurityEnabled =>
          val originalCreds = UserGroupInformation.getCurrentUser().getCredentials()
          val keytabFilename = kyuubiConf.get(KyuubiConf.ENGINE_KEYTAB).orNull
          if (!new File(keytabFilename).exists()) {
            throw new KyuubiException(s"Keytab file: $keytabFilename does not exist")
          } else {
            info("Attempting to login to Kerberos " +
              s"using principal: $principalName and keytab: $keytabFilename")
            UserGroupInformation.loginUserFromKeytab(principalName, keytabFilename)
          }

          val newUGI = UserGroupInformation.getCurrentUser()

          // Transfer YARN_AM_RM_TOKEN to the new user.
          // Not transfer other tokens, such as HDFS_DELEGATION_TOKEN,
          // to avoid "org.apache.hadoop.ipc.RemoteException(java.io.IOException):
          // Delegation Token can be issued only with kerberos or web authentication"
          // when engine tries to obtain new tokens.
          KyuubiHadoopUtils.getTokenMap(originalCreds).values
            .find(_.getKind == AMRMTokenIdentifier.KIND_NAME)
            .foreach { token =>
              newUGI.addToken(token)
            }
          newUGI
        case _ =>
          val appUser = kyuubiConf.getOption(KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY)
          require(appUser.isDefined, s"${KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY} is not set")
          val newUGI = UserGroupInformation.createRemoteUser(appUser.get)
          newUGI.addCredentials(UserGroupInformation.getCurrentUser.getCredentials)
          newUGI
      }

      ugi.doAs(new PrivilegedExceptionAction[Unit] {
        override def run(): Unit = runApplicationMaster()
      })
    } catch {
      case t: Throwable =>
        error("Error running ApplicationMaster", t)
        finalStatus = FinalApplicationStatus.FAILED
        finalMsg = t.getMessage
        cleanupStagingDir()
        unregister(finalStatus, finalMsg)
        if (currentEngine != null) {
          currentEngine.stop()
        }
    }
  }

  def runApplicationMaster(): Unit = {
    initAmClient()

    runEngine()

    registerAM()
  }

  def runEngine(): Unit = {
    val buffer = new ArrayBuffer[String]()
    buffer ++= confKeyValues(kyuubiConf.getAll)

    val instance = DynFields.builder()
      .impl(currentEngineMainClass, "MODULE$")
      .build[Object].get(null)
    DynMethods.builder("main")
      .hiddenImpl(currentEngineMainClass, classOf[Array[String]])
      .buildChecked()
      .invoke(instance, buffer.toArray)

    currentEngine = DynFields.builder()
      .hiddenImpl(currentEngineMainClass, "currentEngine")
      .buildChecked[Option[Serverable]]()
      .get(instance)
      .get
  }

  def initAmClient(): Unit = {
    amClient = AMRMClient.createAMRMClient()
    amClient.init(yarnConf)
    amClient.start()
  }

  def registerAM(): Unit = {
    val frontendService = currentEngine.frontendServices.head
    val trackingUrl = frontendService.connectionUrl
    val (host, port) = resolveHostAndPort(trackingUrl)
    info("Registering the HiveSQLEngine ApplicationMaster with tracking url " +
      s"$trackingUrl, host = $host, port = $port")
    synchronized {
      amClient.registerApplicationMaster(host, port, trackingUrl)
      registered = true
    }
  }

  def unregister(status: FinalApplicationStatus, diagnostics: String): Unit = {
    synchronized {
      if (registered && !unregistered) {
        info(s"Unregistering ApplicationMaster with $status" +
          Option(diagnostics).map(msg => s" (diagnostics message: $msg)").getOrElse(""))
        unregistered = true
        amClient.unregisterApplicationMaster(status, diagnostics, "")
        if (amClient != null) {
          amClient.stop()
        }
      }
    }
  }

  private def resolveHostAndPort(connectionUrl: String): (String, Int) = {
    val strings = connectionUrl.split(":")
    (strings(0), strings(1).toInt)
  }

  private def cleanupStagingDir(): Unit = {
    val stagingDirPath = new Path(System.getenv("KYUUBI_ENGINE_YARN_MODE_STAGING_DIR"))
    try {
      val fs = stagingDirPath.getFileSystem(yarnConf)
      info("Deleting staging directory " + stagingDirPath)
      fs.delete(stagingDirPath, true)
    } catch {
      case ioe: IOException =>
        error("Failed to cleanup staging dir " + stagingDirPath, ioe)
    }
  }
}
