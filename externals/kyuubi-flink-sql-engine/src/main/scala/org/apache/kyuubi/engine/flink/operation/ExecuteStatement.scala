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
import org.apache.calcite.rel.metadata.{DefaultRelMetadataProvider, JaninoRelMetadataProvider, RelMetadataQueryBase}
import org.apache.flink.table.api.{DataTypes, ResultKind}
import org.apache.flink.table.catalog.Column
import org.apache.flink.table.client.gateway.{Executor, TypedResult}
import org.apache.flink.table.operations.{Operation, QueryOperation}
import org.apache.flink.table.operations.command.{ResetOperation, SetOperation}
import org.apache.flink.types.Row

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.engine.flink.result.{OperationUtil, ResultSet}
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
          executeStatement()
        }
      }

      try {
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

      // set the thread variable THREAD_PROVIDERS
      RelMetadataQueryBase.THREAD_PROVIDERS.set(
        JaninoRelMetadataProvider.of(DefaultRelMetadataProvider.INSTANCE))
      val operation = executor.parseStatement(sessionId, statement)
      operation match {
        case queryOperation: QueryOperation => runQueryOperation(queryOperation)
        case setOperation: SetOperation => runSetOperation(setOperation)
        case resetOperation: ResetOperation => runResetOperation(resetOperation)
        case operation: Operation => runOperation(operation)
      }
    } catch {
      onError(cancel = true)
    } finally {
      statementTimeoutCleaner.foreach(_.shutdown())
    }
  }

  private def runQueryOperation(operation: QueryOperation): Unit = {
    val resultDescriptor = executor.executeQuery(sessionId, operation)

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
      .columns(resultDescriptor.getResultSchema.getColumns)
      .data(rows.toArray[Row])
      .build
    setState(OperationState.FINISHED)
  }

  private def runSetOperation(setOperation: SetOperation): Unit = {
    if (setOperation.getKey.isPresent) {
      val key: String = setOperation.getKey.get.trim

      if (setOperation.getValue.isPresent) {
        val newValue: String = setOperation.getValue.get.trim
        executor.setSessionProperty(sessionId, key, newValue)
      }

      val value = executor.getSessionConfigMap(sessionId).getOrDefault(key, "")
      resultSet = ResultSet.builder
        .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
        .columns(
          Column.physical("key", DataTypes.STRING()),
          Column.physical("value", DataTypes.STRING()))
        .data(Array(Row.of(key, value)))
        .build
    } else {
      // show all properties if set without key
      val properties: util.Map[String, String] = executor.getSessionConfigMap(sessionId)

      val entries = ArrayBuffer.empty[Row]
      properties.forEach((key, value) => entries.append(Row.of(key, value)))

      if (entries.nonEmpty) {
        val prettyEntries = entries.sortBy(_.getField(0).asInstanceOf[String])
        resultSet = ResultSet.builder
          .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
          .columns(
            Column.physical("key", DataTypes.STRING()),
            Column.physical("value", DataTypes.STRING()))
          .data(prettyEntries.toArray)
          .build
      } else {
        resultSet = ResultSet.builder
          .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
          .columns(
            Column.physical("key", DataTypes.STRING()),
            Column.physical("value", DataTypes.STRING()))
          .data(Array[Row]())
          .build
      }
    }
    setState(OperationState.FINISHED)
  }

  private def runResetOperation(resetOperation: ResetOperation): Unit = {
    if (resetOperation.getKey.isPresent) {
      // reset the given property
      executor.resetSessionProperty(sessionId, resetOperation.getKey.get())
    } else {
      // reset all properties
      executor.resetSessionProperties(sessionId)
    }
    resultSet = OperationUtil.successResultSet()
    setState(OperationState.FINISHED)
  }

  private def runOperation(operation: Operation): Unit = {
    val result = executor.executeOperation(sessionId, operation)
    result.await()
    resultSet = ResultSet.fromTableResult(result)
    setState(OperationState.FINISHED)
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
