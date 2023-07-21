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

package org.apache.kyuubi.engine.flink.result

import scala.collection.convert.ImplicitConversions._
import scala.collection.mutable.ListBuffer

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.ResultKind
import org.apache.flink.table.catalog.Column
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.conversion.DataStructureConverters
import org.apache.flink.table.gateway.service.result.ResultFetcher
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row

import org.apache.kyuubi.engine.flink.shim.FlinkResultSet

/** Utility object for building ResultSet. */
object ResultSetUtil {

  private val FETCH_ROWS_PER_SECOND = 1000

  /**
   * Build a ResultSet with a column name and a list of String values.
   *
   * @param strings list of String values
   * @param columnName name of the result column
   * @return a ResultSet with a string column
   */
  def stringListToResultSet(strings: List[String], columnName: String): ResultSet = {
    val rows: Array[Row] = strings.map(s => Row.of(s)).toArray
    ResultSet.builder
      .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
      .columns(Column.physical(columnName, DataTypes.STRING))
      .data(rows)
      .build
  }

  /**
   * Build a simple ResultSet with OK message. Returned when SQL commands are executed successfully.
   * Noted that a new ResultSet is returned each time, because ResultSet is stateful (with its
   * cursor).
   *
   * @return a simple ResultSet with OK message.
   */
  def successResultSet: ResultSet =
    ResultSet.builder
      .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
      .columns(Column.physical("result", DataTypes.STRING))
      .data(Array[Row](Row.of("OK")))
      .build

  def fromResultFetcher(resultFetcher: ResultFetcher, maxRows: Int): ResultSet = {
    val schema = resultFetcher.getResultSchema
    val resultRowData = ListBuffer.newBuilder[RowData]
    var fetched: FlinkResultSet = null
    var token: Long = 0
    var rowNum: Int = 0
    do {
      fetched = new FlinkResultSet(resultFetcher.fetchResults(token, FETCH_ROWS_PER_SECOND))
      val data = fetched.getData
      val slice = data.slice(0, maxRows - rowNum)
      resultRowData ++= slice
      rowNum += slice.size
      token = fetched.getNextToken
      try Thread.sleep(1000L)
      catch {
        case _: InterruptedException => fetched.getNextToken == null
      }
    } while (
      fetched.getNextToken != null &&
        rowNum < maxRows &&
        fetched.getResultType != org.apache.flink.table.gateway.api.results.ResultSet.ResultType.EOS
    )
    val dataTypes = resultFetcher.getResultSchema.getColumnDataTypes
    ResultSet.builder
      .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
      .columns(schema.getColumns)
      .data(resultRowData.result().map(rd => convertToRow(rd, dataTypes.toList)).toArray)
      .build
  }

  def fromResultFetcher(resultFetcher: ResultFetcher): ResultSet = {
    val schema = resultFetcher.getResultSchema
    val resultRowData = ListBuffer.newBuilder[RowData]
    var fetched: FlinkResultSet = null
    var token: Long = 0
    do {
      fetched = new FlinkResultSet(resultFetcher.fetchResults(token, FETCH_ROWS_PER_SECOND))
      resultRowData ++= fetched.getData
      token = fetched.getNextToken
      try Thread.sleep(1000L)
      catch {
        case _: InterruptedException =>
      }
    } while (
      fetched.getNextToken != null &&
        fetched.getResultType != org.apache.flink.table.gateway.api.results.ResultSet.ResultType.EOS
    )
    val dataTypes = resultFetcher.getResultSchema.getColumnDataTypes
    ResultSet.builder
      .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
      .columns(schema.getColumns)
      .data(resultRowData.result().map(rd => convertToRow(rd, dataTypes.toList)).toArray)
      .build
  }

  private[this] def convertToRow(r: RowData, dataTypes: List[DataType]): Row = {
    val converter = DataStructureConverters.getConverter(DataTypes.ROW(dataTypes: _*))
    converter.toExternal(r).asInstanceOf[Row]
  }
}
