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

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.client.KyuubiSyncThriftClient
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_CREDENTIALS_KEY
import org.apache.kyuubi.engine.{EngineRef, KyuubiApplicationManager}
import org.apache.kyuubi.events.{EventBus, KyuubiSessionEvent}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider._
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.{Operation, OperationHandle}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.service.authentication.InternalSecurityAccessor
import org.apache.kyuubi.session.SessionType.SessionType

class KyuubiSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: KyuubiSessionManager,
    sessionConf: KyuubiConf)
  extends KyuubiSession(protocol, user, password, ipAddress, conf, sessionManager) {

  override val sessionType: SessionType = SessionType.SQL

  private[kyuubi] val optimizedConf: Map[String, String] = {
    val confOverlay = sessionManager.sessionConfAdvisor.getConfOverlay(
      user,
      normalizedConf.asJava)
    if (confOverlay != null) {
      normalizedConf ++ confOverlay.asScala
    } else {
      warn(s"the server plugin return null value for user: $user, ignore it")
      normalizedConf
    }
  }

  // TODO: needs improve the hardcode
  optimizedConf.foreach {
    case ("use:database", _) =>
    case ("kyuubi.engine.pool.size.threshold", _) =>
    case (key, value) => sessionConf.set(key, value)
  }

  private lazy val engineCredentials = renewEngineCredentials()

  lazy val engine: EngineRef =
    new EngineRef(sessionConf, user, handle.identifier.toString, sessionManager.applicationManager)
  private[kyuubi] val launchEngineOp = sessionManager.operationManager
    .newLaunchEngineOperation(this, sessionConf.get(SESSION_ENGINE_LAUNCH_ASYNC))

  private val sessionEvent = KyuubiSessionEvent(this)
  EventBus.post(sessionEvent)

  override def getSessionEvent: Option[KyuubiSessionEvent] = {
    Option(sessionEvent)
  }

  override def checkSessionAccessPathURIs(): Unit = {
    KyuubiApplicationManager.checkApplicationAccessPaths(
      sessionConf.get(ENGINE_TYPE),
      sessionConf.getAll,
      sessionManager.getConf)
  }

  private var _client: KyuubiSyncThriftClient = _
  def client: KyuubiSyncThriftClient = _client

  private var _engineSessionHandle: SessionHandle = _

  override def open(): Unit = {
    MetricsSystem.tracing { ms =>
      ms.incCount(CONN_TOTAL)
      ms.incCount(MetricRegistry.name(CONN_OPEN, user))
    }

    // we should call super.open before running launch engine operation
    super.open()

    checkSessionAccessPathURIs()

    runOperation(launchEngineOp)
  }

  private[kyuubi] def openEngineSession(extraEngineLog: Option[OperationLog] = None): Unit = {
    withDiscoveryClient(sessionConf) { discoveryClient =>
      var openEngineSessionConf = optimizedConf
      if (engineCredentials.nonEmpty) {
        sessionConf.set(KYUUBI_ENGINE_CREDENTIALS_KEY, engineCredentials)
        openEngineSessionConf =
          optimizedConf ++ Map(KYUUBI_ENGINE_CREDENTIALS_KEY -> engineCredentials)
      }
      val (host, port) = engine.getOrCreate(discoveryClient, extraEngineLog)
      val passwd =
        if (sessionManager.getConf.get(ENGINE_SECURITY_ENABLED)) {
          InternalSecurityAccessor.get().issueToken()
        } else {
          Option(password).filter(_.nonEmpty).getOrElse("anonymous")
        }
      try {
        _client = KyuubiSyncThriftClient.createClient(user, passwd, host, port, sessionConf)
        _engineSessionHandle = _client.openSession(protocol, user, passwd, openEngineSessionConf)
      } catch {
        case e: Throwable =>
          error(
            s"Opening engine [${engine.defaultEngineName} $host:$port]" +
              s" for $user session failed",
            e)
          throw e
      }
      logSessionInfo(s"Connected to engine [$host:$port]/[${client.engineId.getOrElse("")}]" +
        s" with ${_engineSessionHandle}]")
      sessionEvent.openedTime = System.currentTimeMillis()
      sessionEvent.remoteSessionId = _engineSessionHandle.identifier.toString
      _client.engineId.foreach(e => sessionEvent.engineId = e)
      EventBus.post(sessionEvent)
    }
  }

  override protected def runOperation(operation: Operation): OperationHandle = {
    if (operation != launchEngineOp) {
      waitForEngineLaunched()
      sessionEvent.totalOperations += 1
    }
    super.runOperation(operation)
  }

  @volatile private var engineLaunched: Boolean = false

  private def waitForEngineLaunched(): Unit = {
    if (!engineLaunched) {
      Option(launchEngineOp).foreach { op =>
        val waitingStartTime = System.currentTimeMillis()
        logSessionInfo(s"Starting to wait the launch engine operation finished")

        op.getBackgroundHandle.get()

        val elapsedTime = System.currentTimeMillis() - waitingStartTime
        logSessionInfo(s"Engine has been launched, elapsed time: ${elapsedTime / 1000} s")

        if (_engineSessionHandle == null) {
          val ex = op.getStatus.exception.getOrElse(
            KyuubiSQLException(s"Failed to launch engine for $handle"))
          throw ex
        }

        engineLaunched = true
      }
    }
  }

  private def renewEngineCredentials(): String = {
    try {
      sessionManager.credentialsManager.renewCredentials(user)
    } catch {
      case e: Exception =>
        error(s"Failed to renew engine credentials for $handle", e)
        ""
    }
  }

  override def close(): Unit = {
    super.close()
    sessionManager.credentialsManager.removeSessionCredentialsEpoch(handle.identifier.toString)
    try {
      if (_client != null) _client.closeSession()
    } finally {
      if (engine != null) engine.close()
      sessionEvent.endTime = System.currentTimeMillis()
      EventBus.post(sessionEvent)
      MetricsSystem.tracing(_.decCount(MetricRegistry.name(CONN_OPEN, user)))
    }
  }
}
