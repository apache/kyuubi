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
package org.apache.kyuubi.grpc.events

import org.apache.kyuubi.grpc.session.GrpcSession
import org.apache.kyuubi.grpc.utils.Clock

sealed abstract class SessionStatus(value: Int)

object SessionStatus {
  case object Pending extends SessionStatus(0)
  case object Started extends SessionStatus(1)
  case object Closed extends SessionStatus(2)
}

abstract class SessionEventsManager(session: GrpcSession, clock: Clock) {
  private def sessionId: String = session.sessionKey.sessionId

  private var _status: SessionStatus = SessionStatus.Pending

  protected def status_(sessionStatus: SessionStatus): Unit = {
    _status = sessionStatus
  }

  def status: SessionStatus = _status

  def postStarted(): Unit = {
    assertStatus(List(SessionStatus.Pending), SessionStatus.Started)
    status_(SessionStatus.Started)
  }

  def postClosed(): Unit = {
    assertStatus(List(SessionStatus.Started), SessionStatus.Closed)
    status_(SessionStatus.Closed)
  }

  private def assertStatus(validStatuses: List[SessionStatus], eventStatus: SessionStatus): Unit = {
    if (!validStatuses.contains(status)) {
      throw new IllegalStateException(
        s"""
           |sessionId: $sessionId with status ${status}
           |is not within statuses $validStatuses for event $eventStatus
           |""".stripMargin)
    }
  }
}
