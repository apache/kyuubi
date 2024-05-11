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

package org.apache.kyuubi.engine.flink.session

import scala.collection.JavaConverters.mapAsJavaMap

import org.apache.flink.table.gateway.api.session.SessionEnvironment
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion
import org.apache.flink.table.gateway.service.context.DefaultContext
import org.apache.flink.table.gateway.service.session.SessionManagerImpl

import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_HANDLE_KEY
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.flink.FlinkSQLEngine
import org.apache.kyuubi.engine.flink.operation.FlinkSQLOperationManager
import org.apache.kyuubi.session.{Session, SessionHandle, SessionManager}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion

class FlinkSQLSessionManager(engineContext: DefaultContext)
  extends SessionManager("FlinkSQLSessionManager") {

  override protected def isServer: Boolean = false

  private lazy val shareLevel = ShareLevel.withName(conf.get(ENGINE_SHARE_LEVEL))

  val operationManager = new FlinkSQLOperationManager()
  val sessionManager = new SessionManagerImpl(engineContext)

  override def start(): Unit = {
    super.start()
    sessionManager.start()
  }

  override protected def createSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): Session = {
    val normalizedConf = conf.map { case (k, v) => k.stripPrefix("flink.") -> v }
    normalizedConf.get(KYUUBI_SESSION_HANDLE_KEY).map(SessionHandle.fromUUID)
      .flatMap(getSessionOption).getOrElse {
        val flinkInternalSession = sessionManager.openSession(
          SessionEnvironment.newBuilder
            .setSessionEndpointVersion(SqlGatewayRestAPIVersion.V1)
            .addSessionConfig(mapAsJavaMap(normalizedConf))
            .build)
        val session = new FlinkSessionImpl(
          protocol,
          user,
          password,
          ipAddress,
          normalizedConf,
          this,
          flinkInternalSession)
        session
      }
  }

  override def getSessionOption(sessionHandle: SessionHandle): Option[Session] = {
    val session = super.getSessionOption(sessionHandle)
    session.foreach(s => s.asInstanceOf[FlinkSessionImpl].fSession.touch())
    session
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    try {
      super.getSessionOption(sessionHandle).foreach { s =>
        sessionManager.closeSession(s.asInstanceOf[FlinkSessionImpl].fSession.getSessionHandle)
      }
      super.closeSession(sessionHandle)
    } catch {
      case t: Throwable =>
        warn(s"Error closing session $sessionHandle", t)
    }
    if (shareLevel == ShareLevel.CONNECTION) {
      info("Flink engine stopped due to session stopped and shared level is CONNECTION.")
      stopEngine()
    }
  }

  private def stopEngine(): Unit = {
    FlinkSQLEngine.currentEngine.foreach(_.stop())
  }

  override def stop(): Unit = synchronized {
    sessionManager.stop()
    super.stop()
  }
}
