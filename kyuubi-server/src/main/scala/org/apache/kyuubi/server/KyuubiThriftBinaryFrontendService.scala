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

import org.apache.commons.codec.binary.Base64
import org.apache.hive.service.rpc.thrift.{TOpenSessionReq, TOpenSessionResp}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.ha.client.{KyuubiServiceDiscovery, ServiceDiscovery}
import org.apache.kyuubi.service.{Serverable, Service, ThriftBinaryFrontendService}
import org.apache.kyuubi.session.KyuubiSessionImpl

class KyuubiThriftBinaryFrontendService(
    override val serverable: Serverable)
  extends ThriftBinaryFrontendService("KyuubiThriftBinaryFrontendService") {
  import ThriftBinaryFrontendService._

  override lazy val discoveryService: Option[Service] = {
    if (ServiceDiscovery.supportServiceDiscovery(conf)) {
      Some(new KyuubiServiceDiscovery(this))
    } else {
      None
    }
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
      if (launchEngineOp.shouldRunAsync) {
        val opHandleIdentifier = launchEngineOp.getHandle.identifier.toTHandleIdentifier
        respConfiguration.put("kyuubi.session.launch.engine.handle.guid",
          Base64.encodeBase64String(opHandleIdentifier.getGuid))
        respConfiguration.put("kyuubi.session.launch.engine.handle.secret",
          Base64.encodeBase64String(opHandleIdentifier.getSecret))
      }

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

  override def connectionUrl: String = {
    checkInitialized()
    s"${serverAddr.getCanonicalHostName}:$portNum"
  }

  override protected def oomHook: Runnable = {
    () => serverable.stop()
  }
}
