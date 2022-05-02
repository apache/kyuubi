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

import org.apache.flink.table.client.gateway.{Executor, SqlExecutionException}
import org.apache.flink.table.client.gateway.context.SessionContext
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.engine.flink.event.FlinkSessionEvent
import org.apache.kyuubi.events.EventBus
import org.apache.kyuubi.operation.{Operation, OperationHandle}
import org.apache.kyuubi.session.{AbstractSession, SessionHandle, SessionManager}

class FlinkSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: SessionManager,
    override val handle: SessionHandle,
    val sessionContext: SessionContext)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  private val sessionEvent = FlinkSessionEvent(this)

  def executor: Executor = sessionManager.asInstanceOf[FlinkSQLSessionManager].executor

  def sessionId: String = handle.identifier.toString

  private def setModifiableConfig(key: String, value: String): Unit = {
    try {
      sessionContext.set(key, value)
    } catch {
      case e: SqlExecutionException => warn(e.getMessage)
    }
  }

  override def open(): Unit = {
    normalizedConf.foreach {
      case (key, value) => setModifiableConfig(key, value)
    }
    super.open()
    EventBus.post(sessionEvent)
  }

  override protected def runOperation(operation: Operation): OperationHandle = {
    sessionEvent.totalOperations += 1
    super.runOperation(operation)
  }

  override def close(): Unit = {
    sessionEvent.endTime = System.currentTimeMillis()
    EventBus.post(sessionEvent)
    super.close()
  }
}
