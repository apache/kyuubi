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

import scala.collection.JavaConverters._
import scala.collection.JavaConverters.mapAsJavaMap

import org.apache.flink.table.gateway.api.session.SessionEnvironment
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion
import org.apache.flink.table.gateway.service.context.DefaultContext
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_HANDLE_KEY
import org.apache.kyuubi.engine.flink.operation.FlinkSQLOperationManager
import org.apache.kyuubi.engine.flink.shim.FlinkSessionManager
import org.apache.kyuubi.session.{Session, SessionHandle, SessionManager}

class FlinkSQLSessionManager(engineContext: DefaultContext)
  extends SessionManager("FlinkSQLSessionManager") {

  override protected def isServer: Boolean = false

  val operationManager = new FlinkSQLOperationManager()
  val sessionManager = new FlinkSessionManager(engineContext)

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
    conf.get(KYUUBI_SESSION_HANDLE_KEY).map(SessionHandle.fromUUID).flatMap(
      getSessionOption).getOrElse {
      val flinkInternalSession = sessionManager.openSession(
        SessionEnvironment.newBuilder
          .setSessionEndpointVersion(SqlGatewayRestAPIVersion.V1)
          .addSessionConfig(mapAsJavaMap(conf))
          .build)
      val sessionConfig = flinkInternalSession.getSessionConfig
      sessionConfig.putAll(conf.asJava)
      val session = new FlinkSessionImpl(
        protocol,
        user,
        password,
        ipAddress,
        conf,
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
    val fSession = super.getSessionOption(sessionHandle)
    fSession.foreach(s =>
      sessionManager.closeSession(s.asInstanceOf[FlinkSessionImpl].fSession.getSessionHandle))
    super.closeSession(sessionHandle)
  }

  override def stop(): Unit = synchronized {
    sessionManager.stop()
    super.stop()
  }
}
