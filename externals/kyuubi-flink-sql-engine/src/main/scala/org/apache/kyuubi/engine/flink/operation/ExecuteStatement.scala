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

package org.apache.kyuubi.engine.flink.operation

import java.util
import java.util.concurrent.{RejectedExecutionException, ScheduledExecutorService, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.google.common.annotations.VisibleForTesting
import org.apache.flink.table.client.gateway.{Executor, ResultDescriptor, TypedResult}
import org.apache.flink.table.operations.QueryOperation
import org.apache.flink.types.Row

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.engine.flink.result.{ColumnInfo, ResultKind, ResultSet}
import org.apache.kyuubi.operation.{OperationState, OperationType}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.util.ThreadUtils

class ExecuteStatement(
    session: Session,
    override val statement: String,
    override val shouldRunAsync: Boolean,
    queryTimeout: Long)
  extends FlinkOperation(OperationType.EXECUTE_STATEMENT, session) with Logging {

  private val operationLog: OperationLog =
    OperationLog.createOperationLog(session, getHandle)

  private var resultDescriptor: ResultDescriptor = _

  private var columnInfos: util.List[ColumnInfo] = _

  private var statementTimeoutCleaner: Option[ScheduledExecutorService] = None

  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  @VisibleForTesting
  override def setExecutor(executor: Executor): Unit = super.setExecutor(executor)

  def setSessionId(sessionId: String): Unit = {
    this.sessionId = sessionId
  }

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(operationLog)
    setState(OperationState.PENDING)
    setHasResultSet(true)
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  override protected def runInternal(): Unit = {
    addTimeoutMonitor()
    if (shouldRunAsync) {
      val asyncOperation = new Runnable {
        override def run(): Unit = {
          OperationLog.setCurrentOperationLog(operationLog)
        }
      }

      try {
        executeStatement()
        val flinkSQLSessionManager = session.sessionManager
        val backgroundHandle = flinkSQLSessionManager.submitBackgroundOperation(asyncOperation)
        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          setState(OperationState.ERROR)
          val ke =
            KyuubiSQLException("Error submitting query in background, query rejected", rejected)
          setOperationException(ke)
          throw ke
      }
    } else {
      executeStatement()
    }
  }

  private def executeStatement(): Unit = {
    try {
      setState(OperationState.RUNNING)

      columnInfos = new util.ArrayList[ColumnInfo]

      val operation = executor.parseStatement(sessionId, statement)
      resultDescriptor = executor.executeQuery(sessionId, operation.asInstanceOf[QueryOperation])
      resultDescriptor.getResultSchema.getColumns.asScala.foreach { column =>
        columnInfos.add(ColumnInfo.create(column.getName, column.getDataType.getLogicalType))
      }

      val resultID = resultDescriptor.getResultId

      val rows = new ArrayBuffer[Row]()
      var loop = true
      while (loop) {
        Thread.sleep(50) // slow the processing down

        val result = executor.snapshotResult(sessionId, resultID, 2)
        result.getType match {
          case TypedResult.ResultType.PAYLOAD =>
            rows.clear()
            (1 to result.getPayload).foreach { page =>
              rows ++= executor.retrieveResultPage(resultID, page).asScala
            }
          case TypedResult.ResultType.EOS => loop = false
          case TypedResult.ResultType.EMPTY =>
        }
      }

      resultSet = ResultSet.builder
        .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
        .columns(columnInfos)
        .data(rows.toArray[Row])
        .build
      setState(OperationState.FINISHED)
    } catch {
      onError(cancel = true)
    } finally {
      statementTimeoutCleaner.foreach(_.shutdown())
    }
  }

  private def addTimeoutMonitor(): Unit = {
    if (queryTimeout > 0) {
      val timeoutExecutor =
        ThreadUtils.newDaemonSingleThreadScheduledExecutor("query-timeout-thread")
      val action: Runnable = () => cleanup(OperationState.TIMEOUT)
      timeoutExecutor.schedule(action, queryTimeout, TimeUnit.SECONDS)
      statementTimeoutCleaner = Some(timeoutExecutor)
    }
  }
}
