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

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.{TFetchOrientation, TFetchResultsReq, TGetOperationStatusReq}
import org.apache.hive.service.rpc.thrift.TOperationState._

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.client.KyuubiSyncThriftClient
import org.apache.kyuubi.events.KyuubiStatementEvent
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.server.EventLoggingService
import org.apache.kyuubi.session.{KyuubiSessionImpl, KyuubiSessionManager, Session}

class ExecuteStatement(
    session: Session,
    client: KyuubiSyncThriftClient,
    override val statement: String,
    override val shouldRunAsync: Boolean,
    queryTimeout: Long)
  extends KyuubiOperation(
    OperationType.EXECUTE_STATEMENT, session, client) {

  val statementEvent: KyuubiStatementEvent =
    KyuubiStatementEvent(this, statementId, state, lastAccessTime)

  private final val _operationLog: OperationLog = if (shouldRunAsync) {
    OperationLog.createServerOperationLog(session.handle, getHandle)
  } else {
    null
  }

  override def getOperationLog: Option[OperationLog] = Option(_operationLog)

  private lazy val statusReq = new TGetOperationStatusReq(_remoteOpHandle)
  private lazy val fetchLogReq = {
    val req = new TFetchResultsReq(_remoteOpHandle, TFetchOrientation.FETCH_NEXT, 1000)
    req.setFetchType(1.toShort)
    req
  }

  EventLoggingService.onEvent(statementEvent)

  override def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(_operationLog)
    setHasResultSet(true)
    setState(OperationState.PENDING)
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  private def executeStatement(): Unit = {
    try {
      MetricsSystem.tracing { ms =>
        ms.incCount(STATEMENT_OPEN)
        ms.incCount(STATEMENT_TOTAL)
      }
      _remoteOpHandle = client.executeStatement(statement, shouldRunAsync, queryTimeout)
    } catch onError()
  }

  private def waitStatementComplete(): Unit = try {
    setState(OperationState.RUNNING)
    var statusResp = client.GetOperationStatus(statusReq)
    var isComplete = false
    while (!isComplete) {
      fetchQueryLog()
      verifyTStatus(statusResp.getStatus)
      val remoteState = statusResp.getOperationState
      info(s"Query[$statementId] in ${remoteState.name()}")
      isComplete = true
      remoteState match {
        case INITIALIZED_STATE | PENDING_STATE | RUNNING_STATE =>
          isComplete = false
          statusResp = client.GetOperationStatus(statusReq)

        case FINISHED_STATE =>
          setState(OperationState.FINISHED)

        case CLOSED_STATE =>
          setState(OperationState.CLOSED)

        case CANCELED_STATE =>
          setState(OperationState.CANCELED)

        case TIMEDOUT_STATE =>
          setState(OperationState.TIMEOUT)

        case ERROR_STATE =>
          setState(OperationState.ERROR)
          val ke = KyuubiSQLException(statusResp.getErrorMessage)
          setOperationException(ke)

        case UKNOWN_STATE =>
          setState(OperationState.ERROR)
          val ke = KyuubiSQLException(s"UNKNOWN STATE for $statement")
          setOperationException(ke)
      }
      sendCredentialsIfNeeded()
    }
    // see if anymore log could be fetched
    fetchQueryLog()
  } catch onError()

  private def sendCredentialsIfNeeded(): Unit = {
    val appUser = session.asInstanceOf[KyuubiSessionImpl].engine.appUser
    val sessionManager = session.sessionManager.asInstanceOf[KyuubiSessionManager]
    sessionManager.credentialsManager.sendCredentialsIfNeeded(
      session.handle.identifier.toString,
      appUser,
      client.sendCredentials)
  }

  private def fetchQueryLog(): Unit = {
    getOperationLog.foreach { logger =>
      try {
        val resp = client.FetchResults(fetchLogReq)
        verifyTStatus(resp.getStatus)
        val logs = resp.getResults.getColumns.get(0).getStringVal.getValues.asScala
        logs.foreach(log => logger.write(log + "\n"))
      } catch {
        case _: Exception => // do nothing
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
      } catch onError("submitting query in background, query rejected")
    } else {
      setState(OperationState.RUNNING)
      executeStatement()
      setState(OperationState.FINISHED)
    }
  }

  override def setState(newState: OperationState): Unit = {
    super.setState(newState)
    statementEvent.state = newState.toString
    statementEvent.stateTime = lastAccessTime
    EventLoggingService.onEvent(statementEvent)
  }

  override def setOperationException(opEx: KyuubiSQLException): Unit = {
    super.setOperationException(opEx)
    statementEvent.exception = opEx.toString
    EventLoggingService.onEvent(statementEvent)
  }

  override def close(): Unit = {
    MetricsSystem.tracing(_.decCount(STATEMENT_OPEN))
    super.close()
  }
}
