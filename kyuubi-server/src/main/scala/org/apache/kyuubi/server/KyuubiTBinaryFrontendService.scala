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

package org.apache.kyuubi.server

import java.util.Base64

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.{TOpenSessionReq, TOpenSessionResp, TRenewDelegationTokenReq, TRenewDelegationTokenResp}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.client.{KyuubiServiceDiscovery, ServiceDiscovery}
import org.apache.kyuubi.service.{Serverable, Service, ServiceUtils, TBinaryFrontendService}
import org.apache.kyuubi.service.TFrontendService.{CURRENT_SERVER_CONTEXT, OK_STATUS, SERVER_VERSION}
import org.apache.kyuubi.service.authentication.KyuubiAuthenticationFactory
import org.apache.kyuubi.session.{KyuubiSessionImpl, SessionHandle}

final class KyuubiTBinaryFrontendService(
    override val serverable: Serverable)
  extends TBinaryFrontendService("KyuubiTBinaryFrontend") {

  override lazy val discoveryService: Option[Service] = {
    if (ServiceDiscovery.supportServiceDiscovery(conf)) {
      Some(new KyuubiServiceDiscovery(this))
    } else {
      None
    }
  }

  private def getProxyUser(
      sessionConf: java.util.Map[String, String],
      ipAddress: String,
      realUser: String): String = {
    val proxyUser = sessionConf.get(KyuubiAuthenticationFactory.HS2_PROXY_USER)
    if (proxyUser == null) {
      realUser
    } else {
      KyuubiAuthenticationFactory.verifyProxyAccess(realUser, proxyUser, ipAddress, hadoopConf)
      proxyUser
    }
  }

  /**
   * @param req the thrift open session request
   * @return the real user and the final user
   */
  private def getUserName(req: TOpenSessionReq): (String, String) = {
    val realUser = ServiceUtils.getShortName(authFactory.getRemoteUser.getOrElse(req.getUsername))
    val finalUser =
      if (req.getConfiguration == null) {
        realUser
      } else {
        getProxyUser(req.getConfiguration, authFactory.getIpAddress.orNull, realUser)
      }
    realUser -> finalUser
  }

  @throws[KyuubiSQLException]
  override protected def getSessionHandle(
      req: TOpenSessionReq,
      res: TOpenSessionResp): SessionHandle = {
    val protocol = getMinVersion(SERVER_VERSION, req.getClient_protocol)
    res.setServerProtocolVersion(protocol)
    val (realUser, userName) = getUserName(req)
    val ipAddress = authFactory.getIpAddress.orNull
    val configuration =
      Option(req.getConfiguration).map(_.asScala.toMap).getOrElse(Map.empty[String, String]) ++
        Map(KyuubiConf.SESSION_REAL_USER.key -> realUser)
    val sessionHandle = be.openSession(
      protocol,
      userName,
      req.getPassword,
      ipAddress,
      configuration)
    sessionHandle
  }

  override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
    debug(req.toString)
    info("Client protocol version: " + req.getClient_protocol)
    val resp = new TOpenSessionResp
    try {
      val sessionHandle = getSessionHandle(req, resp)

      val respConfiguration = new java.util.HashMap[String, String]()
      val launchEngineOp = be.sessionManager.getSession(sessionHandle)
        .asInstanceOf[KyuubiSessionImpl].launchEngineOp

      val opHandleIdentifier = launchEngineOp.getHandle.identifier.toTHandleIdentifier
      respConfiguration.put(
        "kyuubi.session.engine.launch.handle.guid",
        Base64.getMimeEncoder.encodeToString(opHandleIdentifier.getGuid))
      respConfiguration.put(
        "kyuubi.session.engine.launch.handle.secret",
        Base64.getMimeEncoder.encodeToString(opHandleIdentifier.getSecret))

      resp.setSessionHandle(sessionHandle.toTSessionHandle)
      resp.setConfiguration(respConfiguration)
      resp.setStatus(OK_STATUS)
      Option(CURRENT_SERVER_CONTEXT.get()).foreach(_.setSessionHandle(sessionHandle))
    } catch {
      case e: Exception =>
        error("Error opening session: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e, verbose = true))
    }
    resp
  }

  override protected def isServer(): Boolean = true

  override def RenewDelegationToken(req: TRenewDelegationTokenReq): TRenewDelegationTokenResp = {
    debug(req.toString)
    val resp = new TRenewDelegationTokenResp
    resp.setStatus(notSupportTokenErrorStatus)
    resp
  }
}
