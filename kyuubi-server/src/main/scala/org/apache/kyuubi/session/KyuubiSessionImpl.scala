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

import com.codahale.metrics.MetricRegistry
import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.TException
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TSocket, TTransport}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.client.KyuubiSyncThriftClient
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.EngineRef
import org.apache.kyuubi.events.KyuubiAuditEvent
import org.apache.kyuubi.events.KyuubiSessionEvent
import org.apache.kyuubi.ha.client.ZooKeeperClientProvider._
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.{Operation, OperationHandle}
import org.apache.kyuubi.server.EventLoggingService
import org.apache.kyuubi.service.authentication.PlainSASLHelper

class KyuubiSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: KyuubiSessionManager,
    sessionConf: KyuubiConf)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  // TODO: needs improve the hardcode
  normalizedConf.foreach {
    case ("use:database", _) =>
    case ("kyuubi.engine.pool.size.threshold", _) =>
    case (key, value) => sessionConf.set(key, value)
  }

  val engine: EngineRef = new EngineRef(sessionConf, user)

  private lazy val auditLogEnable: Boolean = {
    sessionConf.get(AUDIT_LOG_ENABLE)
  }

  private val sessionEvent = KyuubiSessionEvent(this)
  EventLoggingService.onEvent(sessionEvent)

  private var transport: TTransport = _
  private var client: KyuubiSyncThriftClient = _

  private var _handle: SessionHandle = _
  override def handle: SessionHandle = _handle

  override def open(): Unit = {
    MetricsSystem.tracing { ms =>
      ms.incCount(CONN_TOTAL)
    }
    withZkClient(sessionConf) { zkClient =>
      val (host, port) = engine.getOrCreate(zkClient)
      openSession(host, port)
      MetricsSystem.tracing { ms =>
        ms.incCount(MetricRegistry.name(CONN_OPEN, user))
      }
    }
    // we should call super.open after kyuubi session is already opened
    super.open()
  }

  private def openSession(host: String, port: Int): Unit = {
    val passwd = Option(password).filter(_.nonEmpty).getOrElse("anonymous")
    val loginTimeout = sessionConf.get(ENGINE_LOGIN_TIMEOUT).toInt
    transport = PlainSASLHelper.getPlainTransport(
      user, passwd, new TSocket(host, port, loginTimeout))
    if (!transport.isOpen) {
      transport.open()
      logSessionInfo(s"Connected to engine [$host:$port]")
    }
    client = new KyuubiSyncThriftClient(new TBinaryProtocol(transport))
    // use engine SessionHandle directly
    _handle = client.openSession(protocol, user, passwd, normalizedConf)
    sessionManager.operationManager.setConnection(handle, client)
    sessionEvent.openedTime = System.currentTimeMillis()
    sessionEvent.sessionId = handle.identifier.toString
    sessionEvent.clientVersion = handle.protocol.getValue
    EventLoggingService.onEvent(sessionEvent)
    auditEvent(sessionEvent, SessionState.OPEN.toString)
  }

  override protected def runOperation(operation: Operation): OperationHandle = {
    sessionEvent.totalOperations += 1
    super.runOperation(operation)
  }

  override def close(): Unit = {
    super.close()
    if (handle != null) {
      sessionManager.operationManager.removeConnection(handle)
      sessionManager.credentialsManager.removeSessionCredentialsEpoch(handle.identifier.toString)
    }
    try {
      if (client != null) client.closeSession()
    } catch {
      case e: TException =>
        throw KyuubiSQLException("Error while cleaning up the engine resources", e)
    } finally {
      sessionEvent.endTime = System.currentTimeMillis()
      EventLoggingService.onEvent(sessionEvent)
      auditEvent(sessionEvent, SessionState.CLOSE.toString)
      MetricsSystem.tracing(_.decCount(MetricRegistry.name(CONN_OPEN, user)))
      if (transport != null && transport.isOpen) {
        transport.close()
      }
    }
  }

  private def auditEvent(event: KyuubiSessionEvent, state: String): Unit = {
    if (auditLogEnable) {
      val elapsedTime = if (state == SessionState.CLOSE.toString) {
        (event.endTime - event.openedTime) / 1000.0
      } else {
        -1
      }
      val auditEvent = KyuubiAuditEvent(event, state, elapsedTime)
      EventLoggingService.onAuditEvent(auditEvent)
    }
  }

  object SessionState extends Enumeration {
    type SessionState = Value

    val OPEN, CLOSE = Value
  }
}
