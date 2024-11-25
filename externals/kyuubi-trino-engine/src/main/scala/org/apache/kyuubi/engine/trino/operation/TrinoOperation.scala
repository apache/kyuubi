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

package org.apache.kyuubi.engine.trino.operation

import java.io.IOException

import io.trino.client.Column
import io.trino.client.StatementClient

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf.SESSION_PROGRESS_ENABLE
import org.apache.kyuubi.engine.trino.TrinoContext
import org.apache.kyuubi.engine.trino.TrinoProgressMonitor
import org.apache.kyuubi.engine.trino.schema.{SchemaHelper, TrinoTRowSetGenerator}
import org.apache.kyuubi.engine.trino.session.TrinoSessionImpl
import org.apache.kyuubi.operation.AbstractOperation
import org.apache.kyuubi.operation.FetchIterator
import org.apache.kyuubi.operation.FetchOrientation.{FETCH_FIRST, FETCH_NEXT, FETCH_PRIOR, FetchOrientation}
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.operation.OperationStatus
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TFetchResultsResp, TGetResultSetMetadataResp, TProgressUpdateResp}

abstract class TrinoOperation(session: Session) extends AbstractOperation(session) {

  protected val trinoContext: TrinoContext = session.asInstanceOf[TrinoSessionImpl].trinoContext

  protected var trino: StatementClient = _

  protected var schema: List[Column] = _

  protected var iter: FetchIterator[List[Any]] = _

  protected def supportProgress: Boolean = false

  private val progressEnable: Boolean = session.sessionManager.getConf.get(SESSION_PROGRESS_ENABLE)

  override def getStatus: OperationStatus = {
    if (progressEnable && supportProgress) {
      val progressMonitor = new TrinoProgressMonitor(trino)
      setOperationJobProgress(new TProgressUpdateResp(
        progressMonitor.headers,
        progressMonitor.rows,
        progressMonitor.progressedPercentage,
        progressMonitor.executionStatus,
        progressMonitor.footerSummary,
        startTime))
    }
    super.getStatus
  }

  override def getResultSetMetadata: TGetResultSetMetadataResp = {
    val tTableSchema = SchemaHelper.toTTableSchema(schema)
    val resp = new TGetResultSetMetadataResp
    resp.setSchema(tTableSchema)
    resp.setStatus(OK_STATUS)
    resp
  }

  override def getNextRowSetInternal(
      order: FetchOrientation,
      rowSetSize: Int): TFetchResultsResp = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    order match {
      case FETCH_NEXT => iter.fetchNext()
      case FETCH_PRIOR => iter.fetchPrior(rowSetSize)
      case FETCH_FIRST => iter.fetchAbsolute(0)
    }
    val taken = iter.take(rowSetSize)
    val resultRowSet =
      new TrinoTRowSetGenerator().toTRowSet(taken.toSeq, schema, getProtocolVersion)
    resultRowSet.setStartRowOffset(iter.getPosition)
    val resp = new TFetchResultsResp(OK_STATUS)
    resp.setResults(resultRowSet)
    resp.setHasMoreRows(false)
    resp
  }

  override protected def beforeRun(): Unit = {
    setHasResultSet(true)
    setState(OperationState.RUNNING)
  }

  override protected def afterRun(): Unit = {
    withLockRequired {
      if (!isTerminalState(state)) {
        setState(OperationState.FINISHED)
      }
    }
    OperationLog.removeCurrentOperationLog()
  }

  override def cancel(): Unit = {
    cleanup(OperationState.CANCELED)
  }

  override def close(): Unit = {
    cleanup(OperationState.CLOSED)
    try {
      if (trino != null) {
        trino.close()
        trino = null
      }
      getOperationLog.foreach(_.close())
    } catch {
      case e: IOException =>
        error(e.getMessage, e)
    }
  }

  override def shouldRunAsync: Boolean = false

  protected def onError(cancel: Boolean = false): PartialFunction[Throwable, Unit] = {
    // We should use Throwable instead of Exception since `java.lang.NoClassDefFoundError`
    // could be thrown.
    case e: Throwable =>
      if (cancel && trino.isRunning) trino.cancelLeafStage()
      withLockRequired {
        val errMsg = Utils.stringifyException(e)
        if (state == OperationState.TIMEOUT) {
          val ke = KyuubiSQLException(s"Timeout operating $opType: $errMsg")
          setOperationException(ke)
          throw ke
        } else if (isTerminalState(state)) {
          setOperationException(KyuubiSQLException(errMsg))
          warn(s"Ignore exception in terminal state with $statementId: $errMsg")
        } else {
          error(s"Error operating $opType: $errMsg", e)
          val ke = KyuubiSQLException(s"Error operating $opType: $errMsg", e)
          setOperationException(ke)
          setState(OperationState.ERROR)
          throw ke
        }
      }
  }
}
