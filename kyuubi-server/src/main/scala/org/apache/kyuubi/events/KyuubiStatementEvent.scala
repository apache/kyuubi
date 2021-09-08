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

package org.apache.kyuubi.events

import org.apache.kyuubi.Utils
import org.apache.kyuubi.operation.ExecuteStatement
import org.apache.kyuubi.operation.OperationState.OperationState

/**
 *
 * @param user: who connect to kyuubi server
 * @param statementId: the identifier of operationHandler
 * @param statement: the sql that you execute
 * @param remoteIp: the ip of user
 * @param sessionId: the identifier of a session
 * @param createTime: the create time of this statement
 * @param state: store each state that the sql has
 * @param stateTime: the time that the sql's state change
 * @param exception: caught exception if have
 */
case class KyuubiStatementEvent(
    user: String,
    statementId: String,
    statement: String,
    remoteIp: String,
    sessionId: String,
    createTime: Long,
    var state: String,
    var stateTime: Long,
    var exception: String = "") extends ServerEvent {
  override def partitions: Seq[(String, String)] =
    ("day", Utils.getDateFromTimestamp(createTime)) :: Nil
}

object KyuubiStatementEvent {
  def apply(statement: ExecuteStatement,
      statementId: String,
      state: OperationState,
      stateTime: Long): KyuubiStatementEvent = {
    val session = statement.getSession
    new KyuubiStatementEvent(
      session.user,
      statementId,
      statement.statement,
      session.ipAddress,
      session.handle.identifier.toString,
      stateTime,
      state.toString,
      stateTime)
  }
}
