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

import org.apache.flink.table.client.gateway.Executor
import org.apache.flink.table.client.gateway.context.SessionContext
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.session.{AbstractSession, SessionHandle, SessionManager}

class FlinkSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: SessionManager)
  extends AbstractSession(protocol, user, password, ipAddress, conf, sessionManager) {

  override val handle: SessionHandle = SessionHandle(protocol)

  private var sessionContext: SessionContext = _

  def getSessionContext: SessionContext = sessionContext

  def setSessionContext(sessionContext: SessionContext): Unit = {
    this.sessionContext = sessionContext
  }

  def getExecutor: Executor = sessionManager.asInstanceOf[FlinkSQLSessionManager].executor

  def getSessionId: String = handle.toString

}
