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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.flink.api.common.JobID
import org.apache.flink.table.api.{ResultKind, TableResult}
import org.apache.flink.table.client.gateway.TypedResult
import org.apache.flink.table.data.{GenericArrayData, GenericMapData, RowData}
import org.apache.flink.table.data.binary.{BinaryArrayData, BinaryMapData}
import org.apache.flink.table.operations.{Operation, QueryOperation}
import org.apache.flink.table.operations.command._
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical._
import org.apache.flink.types.Row

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.flink.FlinkEngineUtils._
import org.apache.kyuubi.engine.flink.result.ResultSet
import org.apache.kyuubi.engine.flink.schema.RowSet.toHiveString
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.reflection.DynMethods
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

  var jobId: Option[JobID] = None

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
    executeStatement()
  }

  private def executeStatement(): Unit = {
    try {
      setState(OperationState.RUNNING)
      val operation = executor.parseStatement(sessionId, statement)
      operation match {
        case queryOperation: QueryOperation => runQueryOperation(queryOperation)
        case setOperation: SetOperation =>
          resultSet = OperationUtils.runSetOperation(setOperation, executor, sessionId)
        case resetOperation: ResetOperation =>
          resultSet = OperationUtils.runResetOperation(resetOperation, executor, sessionId)
        case addJarOperation: AddJarOperation if isFlinkVersionAtMost("1.15") =>
          resultSet = OperationUtils.runAddJarOperation(addJarOperation, executor, sessionId)
        case removeJarOperation: RemoveJarOperation =>
          resultSet = OperationUtils.runRemoveJarOperation(removeJarOperation, executor, sessionId)
        case showJarsOperation: ShowJarsOperation if isFlinkVersionAtMost("1.15") =>
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
                val retrieveResultPage = DynMethods.builder("retrieveResultPage")
                  .impl(executor.getClass, classOf[String], classOf[Int])
                  .build(executor)
                val _page = Integer.valueOf(page)
                if (isFlinkVersionEqualTo("1.14")) {
                  val result = retrieveResultPage.invoke[util.List[Row]](resultId, _page)
                  rows ++= result.asScala
                } else if (isFlinkVersionAtLeast("1.15")) {
                  val result = retrieveResultPage.invoke[util.List[RowData]](resultId, _page)
                  rows ++= result.asScala.map(r => convertToRow(r, dataTypes))
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
    // FLINK-24461 executeOperation method changes the return type
    // from TableResult to TableResultInternal
    val executeOperation = DynMethods.builder("executeOperation")
      .impl(executor.getClass, classOf[String], classOf[Operation])
      .build(executor)
    val result = executeOperation.invoke[TableResult](sessionId, operation)
    jobId = result.getJobClient.asScala.map(_.getJobID)
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
                for (i <- kvArray._1.indices) {
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
          val fieldDataTypes = DynMethods.builder("getFieldDataTypes")
            .impl(classOf[DataType], classOf[DataType])
            .buildStatic
            .invoke[util.List[DataType]](dataType)
            .asScala.toList
          val internalRowData = r.getRow(i, t.getFieldCount)
          val internalRow = convertToRow(internalRowData, fieldDataTypes)
          row.setField(i, internalRow)
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
