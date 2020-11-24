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

package org.apache.kyuubi.operation

import java.util.concurrent.RejectedExecutionException

import org.apache.hive.service.rpc.thrift.{TCLIService, TExecuteStatementReq, TGetOperationStatusReq, TSessionHandle}
import org.apache.hive.service.rpc.thrift.TOperationState._

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.session.Session

class ExecuteStatement(
    session: Session,
    client: TCLIService.Iface,
    remoteSessionHandle: TSessionHandle,
    override val statement: String,
    override val shouldRunAsync: Boolean)
  extends KyuubiOperation(
    OperationType.EXECUTE_STATEMENT, session, client, remoteSessionHandle) {

  private lazy val statusReq = new TGetOperationStatusReq(_remoteOpHandle)

  override def beforeRun(): Unit = {
    setHasResultSet(true)
    setState(OperationState.PENDING)
  }

  override protected def afterRun(): Unit = {}

  private def executeStatement(): Unit = {
    try {
      val req = new TExecuteStatementReq(remoteSessionHandle, statement)
      req.setRunAsync(shouldRunAsync)
      val resp = client.ExecuteStatement(req)
      verifyTStatus(resp.getStatus)
      _remoteOpHandle = resp.getOperationHandle
    } catch onError()
  }

  private def waitStatementComplete(): Unit = {
    setState(OperationState.RUNNING)
    var statusResp = client.GetOperationStatus(statusReq)
    var isComplete = false
    while (!isComplete) {
      verifyTStatus(statusResp.getStatus)
      val remoteState = statusResp.getOperationState
      info(s"Query[$statementId] in ${remoteState.name()}")
      remoteState match {
        case INITIALIZED_STATE | PENDING_STATE | RUNNING_STATE =>
          statusResp = client.GetOperationStatus(statusReq)

        case FINISHED_STATE =>
          setState(OperationState.FINISHED)
          isComplete = true

        case CLOSED_STATE =>
          setState(OperationState.CLOSED)
          isComplete = true

        case CANCELED_STATE =>
          setState(OperationState.CANCELED)
          isComplete = true

        case TIMEDOUT_STATE =>
          setState(OperationState.TIMEOUT)
          isComplete = true

        case ERROR_STATE =>
          setState(OperationState.ERROR)
          val ke = KyuubiSQLException(statusResp.getErrorMessage)
          setOperationException(ke)
          throw ke

        case UKNOWN_STATE =>
          setState(OperationState.ERROR)
          val ke = KyuubiSQLException(s"UNKNOWN STATE for $statement")
          setOperationException(ke)
          throw ke
      }
    }
  }

  override protected def runInternal(): Unit = {
    if (shouldRunAsync) {
      executeStatement()
      val sessionManager = session.sessionManager
      val asyncOperation = new Runnable {
        override def run(): Unit = waitStatementComplete()
      }
      try {
        val backgroundOperation =
          sessionManager.submitBackgroundOperation(asyncOperation)
        setBackgroundHandle(backgroundOperation)
      } catch {
        case rejected: RejectedExecutionException =>
          setState(OperationState.ERROR)
          val ke = KyuubiSQLException("Error submitting query in background, query rejected",
            rejected)
          setOperationException(ke)
          throw ke
      }
    } else {
      setState(OperationState.RUNNING)
      executeStatement()
      setState(OperationState.FINISHED)
    }
  }
}
