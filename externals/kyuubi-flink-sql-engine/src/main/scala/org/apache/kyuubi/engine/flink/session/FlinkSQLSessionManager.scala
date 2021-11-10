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

import java.util.Collections

import scala.collection.JavaConverters.mapAsJavaMapConverter

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.flink.FlinkSQLEngine
import org.apache.kyuubi.engine.flink.config.EngineEnvironment
import org.apache.kyuubi.engine.flink.config.entries.{ExecutionEntry, ViewEntry}
import org.apache.kyuubi.engine.flink.context.{EngineContext, SessionContext}
import org.apache.kyuubi.engine.flink.operation.FlinkSQLOperationManager
import org.apache.kyuubi.session.{SessionHandle, SessionManager}

class FlinkSQLSessionManager(engineContext: EngineContext)
  extends SessionManager("FlinkSQLSessionManager") {

  override protected def isServer: Boolean = false

  val operationManager = new FlinkSQLOperationManager()

  override def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Predef.Map[String, String]): SessionHandle = {
    val newProperties = collection.mutable.Map[String, String]()

    newProperties ++= conf

    val executionTypeFullKey =
      EngineEnvironment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_TYPE
    val executionType = newProperties.getOrElse(
      executionTypeFullKey,
      ExecutionEntry.EXECUTION_TYPE_VALUE_BATCH)

    newProperties += executionTypeFullKey -> executionType

    // for batch mode we ensure that results are provided in materialized form
    if (executionType.equalsIgnoreCase(ExecutionEntry.EXECUTION_TYPE_VALUE_BATCH)) {
      newProperties += (
        EngineEnvironment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_RESULT_MODE ->
        ExecutionEntry.EXECUTION_RESULT_MODE_VALUE_TABLE)
    }
    // for streaming mode we ensure that results are provided in changelog form
    else {
      newProperties += (
        EngineEnvironment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_RESULT_MODE ->
        ExecutionEntry.EXECUTION_RESULT_MODE_VALUE_CHANGELOG)
    }

    val sessionEnv = EngineEnvironment.enrich(
      engineContext.getEngineEnv,
      newProperties.asJava,
      Collections.emptyMap[String, ViewEntry])

    val sessionContext = new SessionContext(sessionEnv, engineContext)

    val sessionImpl = new FlinkSessionImpl(
      protocol,
      user,
      password,
      ipAddress,
      conf,
      this,
      sessionContext)
    val handle = sessionImpl.handle

    try {
      sessionImpl.open()

      operationManager.setFlinkSession(handle, sessionContext)
      setSession(handle, sessionImpl)
      info(s"$user's session with $handle is opened, current opening sessions" +
        s" $getOpenSessionCount")
      handle
    } catch {
      case e: Exception =>
        sessionImpl.close()
        throw KyuubiSQLException(e)
    }
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    super.closeSession(sessionHandle)
    operationManager.removeFlinkSession(sessionHandle)
    if (conf.get(ENGINE_SHARE_LEVEL) == ShareLevel.CONNECTION.toString) {
      info("Session stopped due to shared level is Connection.")
      stopSession()
    }
  }

  private def stopSession(): Unit = {
    FlinkSQLEngine.currentEngine.foreach(_.stop())
  }
}
