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
package org.apache.kyuubi.engine.jdbc.operation

import java.sql.{Connection, ResultSet}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.jdbc.dialect.JdbcDialect
import org.apache.kyuubi.engine.jdbc.schema.Schema
import org.apache.kyuubi.engine.jdbc.session.JdbcSessionImpl
import org.apache.kyuubi.engine.jdbc.util.ResultSetFetchIterator
import org.apache.kyuubi.operation.FetchOrientation.{FETCH_NEXT, FetchOrientation}
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TFetchResultsResp

/**
 * Base class that streams the result of a `DatabaseMetaData.getXxx(...)` call against the
 * session connection. Subclasses fix the particular metadata RPC via [[fetchMetaData]].
 */
abstract class MetaDataOperation(session: Session) extends JdbcOperation(session) {

  protected def fetchMetaData(dialect: JdbcDialect, conn: Connection): ResultSet

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)
  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  @volatile private var resultSetIterator: ResultSetFetchIterator = _

  override protected def runInternal(): Unit = {
    setState(OperationState.RUNNING)
    try {
      val connection = session.asInstanceOf[JdbcSessionImpl].sessionConnection
      val resultSet = fetchMetaData(dialect, connection)
      val iterator = new ResultSetFetchIterator(resultSet)
      try {
        // Normalise spec-divergent column labels (e.g. Impala HS2 TABLE_MD -> TABLE_SCHEM).
        // Only applied here, not in ExecuteStatement, so user SELECT aliases are unaffected.
        val schemaHelper = dialect.getSchemaHelper()
        val rawSchema = Schema(iterator.getMetadata)
        schema = Schema(rawSchema.columns.map { c =>
          val normalized = schemaHelper.normalizeMetadataColumnLabel(c.label)
          if (normalized == c.label) c else c.copy(name = normalized, label = normalized)
        })
        resultSetIterator = iterator
        iter = iterator
        setState(OperationState.FINISHED)
      } catch {
        case t: Throwable =>
          closeResultSetIterator(iterator)
          throw t
      }
    } catch onError()
  }

  override def validateFetchOrientation(order: FetchOrientation): Unit = {
    if (order != FETCH_NEXT) {
      throw KyuubiSQLException(
        s"The fetch type $order is not supported for streaming metadata results.")
    }
  }

  override def getNextRowSetInternal(
      order: FetchOrientation,
      rowSetSize: Int): TFetchResultsResp = {
    validateFetchOrientation(order)
    assertState(OperationState.FINISHED)
    iter.fetchNext()
    val taken = iter.take(rowSetSize)
    val resultRowSet = toTRowSet(taken)
    resultRowSet.setStartRowOffset(iter.getFetchStart)
    val hasMoreRows = iter.hasNext
    if (!hasMoreRows) {
      closeResultSetIterator(resultSetIterator)
    }
    val resp = new TFetchResultsResp(OK_STATUS)
    resp.setResults(resultRowSet)
    resp.setHasMoreRows(hasMoreRows)
    resp
  }

  override protected def cleanup(targetState: OperationState): Unit = withLockRequired {
    try {
      super.cleanup(targetState)
    } finally {
      closeResultSetIterator(resultSetIterator)
    }
  }

  private def closeResultSetIterator(iterator: ResultSetFetchIterator): Unit = {
    if (iterator != null) {
      try {
        iterator.close()
      } catch {
        case t: Throwable => warn("Failed to close metadata ResultSet, ignored.", t)
      } finally {
        if (iterator eq resultSetIterator) {
          resultSetIterator = null
        }
      }
    }
  }
}
