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

import java.time.LocalDate
import java.util
import java.util.concurrent.RejectedExecutionException

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.calcite.rel.metadata.{DefaultRelMetadataProvider, JaninoRelMetadataProvider, RelMetadataQueryBase}
import org.apache.flink.table.api.ResultKind
import org.apache.flink.table.client.gateway.TypedResult
import org.apache.flink.table.data.{GenericArrayData, GenericMapData, RowData}
import org.apache.flink.table.data.binary.{BinaryArrayData, BinaryMapData}
import org.apache.flink.table.operations.{Operation, QueryOperation}
import org.apache.flink.table.operations.command._
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical._
import org.apache.flink.types.Row

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.engine.flink.result.ResultSet
import org.apache.kyuubi.engine.flink.schema.RowSet.toHiveString
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.util.RowSetUtils

class ExecuteStatement(
    session: Session,
    override val statement: String,
    override val shouldRunAsync: Boolean,
    queryTimeout: Long,
    resultMaxRows: Int)
  extends FlinkOperation(session) with Logging {

  private val operationLog: OperationLog =
    OperationLog.createOperationLog(session, getHandle)

  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(operationLog)
    setState(OperationState.PENDING)
    setHasResultSet(true)
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  override protected def runInternal(): Unit = {
    addTimeoutMonitor(queryTimeout)
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
        case setOperation: SetOperation =>
          resultSet = OperationUtils.runSetOperation(setOperation, executor, sessionId)
        case resetOperation: ResetOperation =>
          resultSet = OperationUtils.runResetOperation(resetOperation, executor, sessionId)
        case addJarOperation: AddJarOperation =>
          resultSet = OperationUtils.runAddJarOperation(addJarOperation, executor, sessionId)
        case removeJarOperation: RemoveJarOperation =>
          resultSet = OperationUtils.runRemoveJarOperation(removeJarOperation, executor, sessionId)
        case showJarsOperation: ShowJarsOperation =>
          resultSet = OperationUtils.runShowJarOperation(showJarsOperation, executor, sessionId)
        case operation: Operation => runOperation(operation)
      }
      setState(OperationState.FINISHED)
    } catch {
      onError(cancel = true)
    } finally {
      shutdownTimeoutMonitor()
    }
  }

  private def runQueryOperation(operation: QueryOperation): Unit = {
    var resultId: String = null
    try {
      val resultDescriptor = executor.executeQuery(sessionId, operation)
      val dataTypes = resultDescriptor.getResultSchema.getColumnDataTypes.asScala.toList

      resultId = resultDescriptor.getResultId

      val rows = new ArrayBuffer[Row]()
      var loop = true

      while (loop) {
        Thread.sleep(50) // slow the processing down

        val pageSize = Math.min(500, resultMaxRows)
        val result = executor.snapshotResult(sessionId, resultId, pageSize)
        result.getType match {
          case TypedResult.ResultType.PAYLOAD =>
            (1 to result.getPayload).foreach { page =>
              if (rows.size < resultMaxRows) {
                // FLINK-24461 retrieveResultPage method changes the return type from Row to RowData
                val result = executor.retrieveResultPage(resultId, page).asScala.toList
                result.headOption match {
                  case None =>
                  case Some(r) =>
                    // for flink 1.14
                    if (r.getClass == classOf[Row]) {
                      rows ++= result.asInstanceOf[List[Row]]
                    } else {
                      // for flink 1.15+
                      rows ++= result.map(r => convertToRow(r.asInstanceOf[RowData], dataTypes))
                    }
                }
              } else {
                loop = false
              }
            }
          case TypedResult.ResultType.EOS => loop = false
          case TypedResult.ResultType.EMPTY =>
        }
      }

      resultSet = ResultSet.builder
        .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
        .columns(resultDescriptor.getResultSchema.getColumns)
        .data(rows.slice(0, resultMaxRows).toArray[Row])
        .build
    } finally {
      if (resultId != null) {
        cleanupQueryResult(resultId)
      }
    }
  }

  private def runOperation(operation: Operation): Unit = {
    val result = executor.executeOperation(sessionId, operation)
    result.await()
    resultSet = ResultSet.fromTableResult(result)
  }

  private def cleanupQueryResult(resultId: String): Unit = {
    try {
      executor.cancelQuery(sessionId, resultId)
    } catch {
      case t: Throwable =>
        warn(s"Failed to clean result set $resultId in session $sessionId", t)
    }
  }

  private[this] def convertToRow(r: RowData, dataTypes: List[DataType]): Row = {
    val row = Row.withPositions(r.getRowKind, r.getArity)
    for (i <- 0 until r.getArity) {
      val dataType = dataTypes(i)
      dataType.getLogicalType match {
        case arrayType: ArrayType =>
          val arrayData = r.getArray(i)
          if (arrayData == null) {
            row.setField(i, null)
          }
          arrayData match {
            case d: GenericArrayData =>
              row.setField(i, d.toObjectArray)
            case d: BinaryArrayData =>
              row.setField(i, d.toObjectArray(arrayType.getElementType))
            case _ =>
          }
        case _: BinaryType =>
          row.setField(i, r.getBinary(i))
        case _: BigIntType =>
          row.setField(i, r.getLong(i))
        case _: BooleanType =>
          row.setField(i, r.getBoolean(i))
        case _: VarCharType | _: CharType =>
          row.setField(i, r.getString(i))
        case t: DecimalType =>
          row.setField(i, r.getDecimal(i, t.getPrecision, t.getScale).toBigDecimal)
        case _: DateType =>
          val date = RowSetUtils.formatLocalDate(LocalDate.ofEpochDay(r.getInt(i)))
          row.setField(i, date)
        case t: TimestampType =>
          val ts = RowSetUtils
            .formatLocalDateTime(r.getTimestamp(i, t.getPrecision)
              .toLocalDateTime)
          row.setField(i, ts)
        case _: TinyIntType =>
          row.setField(i, r.getByte(i))
        case _: SmallIntType =>
          row.setField(i, r.getShort(i))
        case _: IntType =>
          row.setField(i, r.getInt(i))
        case _: FloatType =>
          row.setField(i, r.getFloat(i))
        case mapType: MapType =>
          val mapData = r.getMap(i)
          if (mapData != null && mapData.size > 0) {
            val keyType = mapType.getKeyType
            val valueType = mapType.getValueType
            mapData match {
              case d: BinaryMapData =>
                val kvArray = toArray(keyType, valueType, d)
                val map: util.Map[Any, Any] = new util.HashMap[Any, Any]
                for (i <- 0 until kvArray._1.length) {
                  val value: Any = kvArray._2(i)
                  map.put(kvArray._1(i), value)
                }
                row.setField(i, map)
              case d: GenericMapData => // TODO
            }
          } else {
            row.setField(i, null)
          }
        case _: DoubleType =>
          row.setField(i, r.getDouble(i))
        case t: RowType =>
          val v = r.getRow(i, t.getFieldCount)
          row.setField(i, v)
        case t =>
          val hiveString = toHiveString((row.getField(i), t))
          row.setField(i, hiveString)
      }
    }
    row
  }

  private[this] def toArray(
      keyType: LogicalType,
      valueType: LogicalType,
      arrayData: BinaryMapData): (Array[_], Array[_]) = {

    arrayData.keyArray().toObjectArray(keyType) -> arrayData.valueArray().toObjectArray(valueType)
  }

}
