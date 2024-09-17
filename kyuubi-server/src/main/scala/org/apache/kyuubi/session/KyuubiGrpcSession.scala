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

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.client.KyuubiGrpcClient
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.engine.EngineRef
import org.apache.kyuubi.events.KyuubiSessionEvent
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.ha.client.ServiceNodeInfo
import org.apache.kyuubi.operation.{Operation, OperationHandle}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.service.authentication.InternalSecurityAccessor
import org.apache.kyuubi.session.SessionType.SessionType
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion

class KyuubiGrpcSession(
    sessionKey: GrpcSessionHandle,
    conf: Map[String, String],
    sessionConf: KyuubiConf,
    sessionManager: KyuubiGrpcSessionManager) extends KyuubiSession(
    protocol = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11,
    user = sessionKey.userId,
    password = "",
    ipAddress = "",
    conf,
    sessionManager) {

  override val handle: GrpcSessionHandle = sessionKey

  private[kyuubi] val optimizedConf: Map[String, String] = normalizedConf

  optimizedConf.foreach {
    case (USE_CATALOG, _) =>
    case (USE_DATABASE, _) =>
    case (key, value) => sessionConf.set(key, value)
  }

  sessionConf.set(FRONTEND_PROTOCOLS, Seq("GRPC"))

  lazy val engine: EngineRef = new EngineRef(
    sessionConf,
    user,
    true,
    new HadoopGroupProvider(),
    handle.sessionId,
    sessionManager.applicationManager)
  private[kyuubi] val launchEngineOp = sessionManager.operationManager
    .newLaunchEngineOperation(this, sessionConf.get(SESSION_ENGINE_LAUNCH_ASYNC))

  @volatile private var _client: KyuubiGrpcClient = _
  def client: KyuubiGrpcClient = _client

  @volatile private var _engineSessionHandle: GrpcSessionHandle = _

  @volatile private var openSessionError: Option[Throwable] = None

  override def open(): Unit = {
    // we should call super.open before running launch engine operation
    super.open()
    runOperation(launchEngineOp)
    waitForEngineLaunched()
  }

  def getEngineNode: Option[ServiceNodeInfo] = {
    withDiscoveryClient(sessionConf) { discoveryClient =>
      engine.getServiceNode(discoveryClient, _client.hostPort)
    }
  }

  private[kyuubi] def openEngineSession(extraEngineLog: Option[OperationLog] = None): Unit = {
    withDiscoveryClient(sessionConf) { discoveryClient =>
      var openEngineSessionConf =
        optimizedConf ++ Map(KYUUBI_SESSION_HANDLE_KEY -> handle.sessionId)

      val (host, port) = engine.getOrCreate(discoveryClient, extraEngineLog)

      def deregisterEngine(): Unit =
        try {
          engine.deregister(discoveryClient, (host, port))
        } catch {
          case e: Throwable =>
            warn(s"Error on de-registering engine [${engine.engineSpace} $host:$port]", e)
        }

      try {
        val passwd =
          if (sessionManager.getConf.get(ENGINE_SECURITY_ENABLED)) {
            InternalSecurityAccessor.get().issueToken()
          } else {
            Option(password).filter(_.nonEmpty).getOrElse("anonymous")
          }
        _client = KyuubiGrpcClient.createClient(user, passwd, host, port, openEngineSessionConf)
        _engineSessionHandle = new GrpcSessionHandle(_client.userId, _client.sessionId)
        logSessionInfo(s"Connected to engine [$host:$port] with ${_engineSessionHandle}]")
      } catch {
        case e: Throwable =>
          error(
            s"Opening engine [${engine.defaultEngineName} $host:$port]" +
              s" for $user session failed",
            e)
          openSessionError = Some(e)
          deregisterEngine()
          throw e
      }
    }
  }

  override protected def runOperation(operation: Operation): OperationHandle = {
    if (operation != launchEngineOp) {
      try {
        waitForEngineLaunched()
      } catch {
        case t: Throwable =>
          operation.close()
          throw t
      }
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

  override val sessionType: SessionType = SessionType.INTERACTIVE

  override def getSessionEvent: Option[KyuubiSessionEvent] = None

  override def checkSessionAccessPathURIs(): Unit = {}
}
