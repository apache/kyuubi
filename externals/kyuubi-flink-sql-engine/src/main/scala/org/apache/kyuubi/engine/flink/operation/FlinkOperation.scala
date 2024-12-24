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

import java.io.IOException
import java.time.ZoneId
import java.util.concurrent.TimeoutException

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.mutable.ListBuffer

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.gateway.service.context.SessionContext
import org.apache.flink.table.gateway.service.operation.OperationExecutor
import org.apache.flink.types.Row

import org.apache.kyuubi.{KyuubiSQLException, Utils}
import org.apache.kyuubi.engine.flink.result.ResultSet
import org.apache.kyuubi.engine.flink.schema.{FlinkTRowSetGenerator, RowSet}
import org.apache.kyuubi.engine.flink.session.FlinkSessionImpl
import org.apache.kyuubi.operation.{AbstractOperation, OperationState}
import org.apache.kyuubi.operation.FetchOrientation.{FETCH_FIRST, FETCH_NEXT, FETCH_PRIOR, FetchOrientation}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TFetchResultsResp, TGetResultSetMetadataResp, TTableSchema}

abstract class FlinkOperation(session: Session) extends AbstractOperation(session) {

  protected val flinkSession: org.apache.flink.table.gateway.service.session.Session =
    session.asInstanceOf[FlinkSessionImpl].fSession

  protected val executor: OperationExecutor = flinkSession.createExecutor(
    Configuration.fromMap(flinkSession.getSessionConfig))

  protected val sessionContext: SessionContext = {
    session.asInstanceOf[FlinkSessionImpl].sessionContext
  }

  protected val sessionId: String = session.handle.identifier.toString

  protected var resultSet: ResultSet = _

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
    //  the result set may be null if the operation ends exceptionally
    if (resultSet != null) {
      resultSet.close
    }
    try {
      getOperationLog.foreach(_.close())
    } catch {
      case e: IOException =>
        error(e.getMessage, e)
    }
  }

  override def getResultSetMetadata: TGetResultSetMetadataResp = {
    val tTableSchema = new TTableSchema()
    resultSet.getColumns.asScala.zipWithIndex.foreach { case (f, i) =>
      tTableSchema.addToColumns(RowSet.toTColumnDesc(f, i))
    }
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
      case FETCH_PRIOR => resultSet.getData.fetchPrior(rowSetSize);
      case FETCH_FIRST => resultSet.getData.fetchAbsolute(0);
      case FETCH_NEXT => // ignored because new data are fetched lazily
    }
    val batch = new ListBuffer[Row]
    try {
      // there could be null values at the end of the batch
      // because Flink could return an EOS
      var rows = 0
      while (resultSet.getData.hasNext && rows < rowSetSize) {
        Option(resultSet.getData.next()).foreach { r => batch += r; rows += 1 }
      }
    } catch {
      case e: TimeoutException =>
        // ignore and return the current batch if there's some data
        // otherwise, rethrow the timeout exception
        if (batch.nonEmpty) {
          debug(s"Timeout fetching more data for $opType operation. " +
            s"Returning the current fetched data.")
        } else {
          throw e
        }
    }
    val timeZone = Option(flinkSession.getSessionConfig.get("table.local-time-zone"))
    val zoneId = timeZone match {
      case Some(tz) => ZoneId.of(tz)
      case None => ZoneId.systemDefault()
    }
    val resultRowSet = new FlinkTRowSetGenerator(zoneId).toTRowSet(
      batch.toList,
      resultSet,
      getProtocolVersion)
    val resp = new TFetchResultsResp(OK_STATUS)
    resp.setResults(resultRowSet)
    resp.setHasMoreRows(resultSet.getData.hasNext)
    resp
  }

  override def shouldRunAsync: Boolean = false

  protected def onError(cancel: Boolean = false): PartialFunction[Throwable, Unit] = {
    // We should use Throwable instead of Exception since `java.lang.NoClassDefFoundError`
    // could be thrown.
    case e: Throwable =>
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
          shutdownTimeoutMonitor()
          throw ke
        }
      }
  }

  implicit class RichOptional[A](val optional: java.util.Optional[A]) {
    def asScala: Option[A] = if (optional.isPresent) Some(optional.get) else None
  }
}
