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

  override def beforeRun(): Unit = {
    setHasResultSet(true)
    setState(OperationState.PENDING)
  }

  override protected def afterRun(): Unit = {}

  override protected def runInternal(): Unit = {
    try {
      val req = new TExecuteStatementReq(remoteSessionHandle, statement)
      req.setRunAsync(shouldRunAsync)
      val resp = client.ExecuteStatement(req)
      verifyTStatus(resp.getStatus)
      _remoteOpHandle = resp.getOperationHandle
      if (shouldRunAsync) {
        // TODO we should do this asynchronous too, not just block this
        var isComplete = false
        val statusReq = new TGetOperationStatusReq(_remoteOpHandle)
        var statusResp = client.GetOperationStatus(statusReq)
        while(!isComplete) {
          verifyTStatus(statusResp.getStatus)
          statusResp.getOperationState match {
            case INITIALIZED_STATE | PENDING_STATE | RUNNING_STATE =>
              statusResp = client.GetOperationStatus(statusReq)
            case state @ (FINISHED_STATE | CLOSED_STATE | CANCELED_STATE | TIMEDOUT_STATE) =>
              isComplete = true
              setState(state)
            case ERROR_STATE =>
              throw KyuubiSQLException(statusResp.getErrorMessage)
            case _ =>
              throw KyuubiSQLException(s"UNKNOWN STATE for $statement")
          }
        }
      } else {
        setState(OperationState.FINISHED)
      }
    } catch onError()
  }
}
