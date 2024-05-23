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

import org.apache.kyuubi.grpc.operation.GrpcOperation
import org.apache.kyuubi.grpc.session.GrpcSession
import org.apache.kyuubi.grpc.utils.Clock

object OperationEventsManager {
  // TODO: make this configurable
  val MAX_STATEMENT_TEXT_SIZE = 65535
}

sealed abstract class OperationStatus(value: Int)

object OperationStatus {
  case object Pending extends OperationStatus(0)
  case object Started extends OperationStatus(1)
  case object Analyzed extends OperationStatus(2)
  case object ReadyForExecution extends OperationStatus(3)
  case object Finished extends OperationStatus(4)
  case object Failed extends OperationStatus(5)
  case object Canceled extends OperationStatus(6)
  case object Closed extends OperationStatus(7)
}
abstract class OperationEventsManager(operation: GrpcOperation, clock: Clock) {
  private def operationId: String = operation.operationKey.operationId

  private def session: GrpcSession = operation.grpcSession

  private def sessionId: String = session.sessionKey.sessionId

  private def sessionStatus = session.sessionEventsManager.status

  protected var _status: OperationStatus = OperationStatus.Pending

  private var error = Option.empty[Boolean]

  private var canceled = Option.empty[Boolean]

  private var producedRowCount = Option.empty[Long]

  private def status: OperationStatus = _status

  private def hasCanceled: Option[Boolean] = canceled

  private def hasError: Option[Boolean] = error

  private def getProduceRowCount: Option[Long] = producedRowCount

  def postStarted(): Unit = {
    assertStatus(List(OperationStatus.Pending), OperationStatus.Started)
  }

  def postAnalyzed(analyzedPlan: Option[Any] = None): Unit = {
    assertStatus(List(OperationStatus.Started, OperationStatus.Analyzed), OperationStatus.Analyzed)
  }

  def postReadyForExecution(): Unit = {
    assertStatus(List(OperationStatus.Analyzed), OperationStatus.ReadyForExecution)
  }

  def postCanceled(): Unit = {
    assertStatus(
      List(
        OperationStatus.Started,
        OperationStatus.Analyzed,
        OperationStatus.ReadyForExecution,
        OperationStatus.Finished,
        OperationStatus.Failed),
      OperationStatus.Canceled)
    canceled = Some(true)
  }

  def postFailed(errorMessage: String): Unit = {
    assertStatus(
      List(
        OperationStatus.Started,
        OperationStatus.Analyzed,
        OperationStatus.ReadyForExecution,
        OperationStatus.Finished),
      OperationStatus.Failed)
    error = Some(true)
  }

  def postFinished(producedRowCountOpt: Option[Long] = None): Unit = {
    assertStatus(
      List(
        OperationStatus.Started,
        OperationStatus.ReadyForExecution),
      OperationStatus.Finished)
    producedRowCount = producedRowCountOpt
  }

  def postClosed(): Unit = {
    assertStatus(
      List(
        OperationStatus.Finished,
        OperationStatus.Failed,
        OperationStatus.Canceled),
      OperationStatus.Closed)
  }

  def status_(operationStatus: OperationStatus): Unit = {
    _status = operationStatus
  }

  private def assertStatus(
      validStatuses: List[OperationStatus],
      eventStatus: OperationStatus): Unit = {
    if (!validStatuses.contains(status)) {
      throw new IllegalStateException(
        s"""
           |operationId: $operationId with status ${status}
           |is not within statuses $validStatuses for event $eventStatus
           |""".stripMargin)
    }
//    if (sessionStatus != SessionStatus.Started) {
//      throw new IllegalStateException(
//        s"""
//           |sessionId: $sessionId with status $sessionStatus
//           |is not Started for event $eventStatus
//           |""".stripMargin)
//    }
    _status = eventStatus
  }
}
