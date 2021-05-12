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

package org.apache.kyuubi.session

import scala.collection.JavaConverters._

import com.codahale.metrics.MetricRegistry
import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.TException
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TSocket, TTransport}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.EngineRef
import org.apache.kyuubi.ha.client.ServiceDiscovery._
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.service.authentication.PlainSASLHelper
import org.apache.kyuubi.util.ThriftUtils

class KyuubiSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: KyuubiSessionManager,
    sessionConf: KyuubiConf)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  normalizedConf.foreach {
    case ("use:database", _) =>
    case (key, value) => sessionConf.set(key, value)
  }

  private val engine: EngineRef = EngineRef(sessionConf, user, handle)

  private var transport: TTransport = _
  private var client: TCLIService.Client = _
  private var remoteSessionHandle: TSessionHandle = _

  override def open(): Unit = {
    MetricsSystem.tracing { ms =>
      ms.incAndGetCount(CONN_TOTAL)
      ms.incAndGetCount(MetricRegistry.name(CONN_OPEN, user))
    }
    super.open()
    withZkClient(sessionConf) { zkClient =>
      val (host, port) = engine.getOrCreate(zkClient)
      openSession(host, port)
    }
  }

  private def openSession(host: String, port: Int): Unit = {
    val passwd = Option(password).filter(_.nonEmpty).getOrElse("anonymous")
    val loginTimeout = sessionConf.get(ENGINE_LOGIN_TIMEOUT).toInt
    transport = PlainSASLHelper.getPlainTransport(
      user, passwd, new TSocket(host, port, loginTimeout))
    if (!transport.isOpen) {
      logSessionInfo(s"Connecting to engine [$host:$port]")
      transport.open()
      logSessionInfo(s"Connected to engine [$host:$port]")
    }
    client = new TCLIService.Client(new TBinaryProtocol(transport))
    val req = new TOpenSessionReq()
    req.setUsername(user)
    req.setPassword(passwd)
    req.setConfiguration(normalizedConf.asJava)
    logSessionInfo(s"Sending TOpenSessionReq to engine [$host:$port]")
    val resp = client.OpenSession(req)
    logSessionInfo(s"Received TOpenSessionResp from engine [$host:$port]")
    ThriftUtils.verifyTStatus(resp.getStatus)
    remoteSessionHandle = resp.getSessionHandle
    sessionManager.operationManager.setConnection(handle, client, remoteSessionHandle)
  }

  override def close(): Unit = {
    super.close()
    sessionManager.operationManager.removeConnection(handle)
    try {
      if (remoteSessionHandle != null) {
        val req = new TCloseSessionReq(remoteSessionHandle)
        val resp = client.CloseSession(req)
        ThriftUtils.verifyTStatus(resp.getStatus)
      }
    } catch {
      case e: TException =>
        throw KyuubiSQLException("Error while cleaning up the engine resources", e)
    } finally {
      MetricsSystem.tracing(_.decAndGetCount(MetricRegistry.name(CONN_OPEN, user)))
      client = null
      if (transport != null) {
        transport.close()
      }
    }
  }
}
