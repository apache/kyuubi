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

/**
 *
 * @param createTime: the create time of this statement
 * @param user: who connect to kyuubi server
 * @param remoteIp: the ip of user
 * @param operationId: the identifier of operation, for statement stored as sessionId/statementId
 * @param operationType: the type of operation: session or statement
 * @param state: store the state that the operate has
 * @param completedTime: the time that the operate end
 * @param operate: the sql that you execute
 * @param elapsedTime: the time that the operate elapsed
 */
case class KyuubiAuditEvent(
    user: String,
    remoteIp: String,
    operationId: String,
    operationType: String,
    createTime: Long,
    state: String,
    completedTime: Long,
    operate: String,
    elapsedTime: String = "") extends KyuubiServerEvent {
  override def partitions: Seq[(String, String)] =
    ("day", Utils.getDateFromTimestamp(createTime)) :: Nil
}

object KyuubiAuditEvent {
  def apply(statementEvent: KyuubiStatementEvent, elapsedTime: Double): KyuubiAuditEvent = {
    new KyuubiAuditEvent(
      statementEvent.user,
      statementEvent.remoteIp,
      s"${statementEvent.sessionId}/${statementEvent.statementId}",
      OperateType.STATEMENT.toString,
      statementEvent.createTime,
      statementEvent.state,
      statementEvent.stateTime,
      statementEvent.statement,
      s"${elapsedTime}s")
  }

  def apply(sessionEvent: KyuubiSessionEvent, state: String,
      elapsedTime: Double): KyuubiAuditEvent = {
    new KyuubiAuditEvent(
      sessionEvent.user,
      sessionEvent.clientIP,
      sessionEvent.sessionId,
      OperateType.SESSION.toString,
      sessionEvent.startTime,
      state,
      sessionEvent.endTime,
      "",
      if (elapsedTime == -1) "" else s"${elapsedTime}s"
    )
  }

  object OperateType extends Enumeration {
    type OperateType = Value

    val STATEMENT, SESSION = Value
  }
}
