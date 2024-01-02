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

package org.apache.kyuubi.engine.result
import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.Duration

import org.apache.kyuubi.engine.result.TRowSetGenerator._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.util.ThreadUtils

trait TRowSetGenerator[SchemaT, RowT, ColumnT]
  extends TColumnValueGenerator[RowT] with TColumnGenerator[RowT] {

  def getColumnSizeFromSchemaType(schema: SchemaT): Int

  def getColumnType(schema: SchemaT, ordinal: Int): ColumnT

  def toTColumn(rows: Seq[RowT], ordinal: Int, typ: ColumnT): TColumn

  def toTColumnValue(row: RowT, ordinal: Int, types: SchemaT): TColumnValue

  def toTRowSet(
      rows: Seq[RowT],
      schema: SchemaT,
      protocolVersion: TProtocolVersion,
      isExecuteInParallel: Boolean = false): TRowSet = {
    if (protocolVersion.getValue < TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      toRowBasedSet(rows, schema)
    } else {
      if (isExecuteInParallel) {
        toColumnBasedSetInParallel(rows, schema)
      } else {
        toColumnBasedSet(rows, schema)
      }
    }
  }

  def toRowBasedSet(rows: Seq[RowT], schema: SchemaT): TRowSet = {
    val rowSize = rows.length
    val tRows = new JArrayList[TRow](rowSize)
    var i = 0
    while (i < rowSize) {
      val row = rows(i)
      var j = 0
      val columnSize = getColumnSizeFromSchemaType(schema)
      val tColumnValues = new JArrayList[TColumnValue](columnSize)
      while (j < columnSize) {
        val columnValue = toTColumnValue(row, j, schema)
        tColumnValues.add(columnValue)
        j += 1
      }
      i += 1
      val tRow = new TRow(tColumnValues)
      tRows.add(tRow)
    }
    new TRowSet(0, tRows)
  }

  def toColumnBasedSet(rows: Seq[RowT], schema: SchemaT): TRowSet = {
    val rowSize = rows.length
    val tRowSet = new TRowSet(0, new JArrayList[TRow](rowSize))
    var i = 0
    val columnSize = getColumnSizeFromSchemaType(schema)
    val tColumns = new JArrayList[TColumn](columnSize)
    while (i < columnSize) {
      val tColumn = toTColumn(rows, i, getColumnType(schema, i))
      tColumns.add(tColumn)
      i += 1
    }
    tRowSet.setColumns(tColumns)
    tRowSet
  }

  def toColumnBasedSetInParallel(rows: Seq[RowT], schema: SchemaT): TRowSet = {
    implicit val ec: ExecutionContextExecutor = tColumnParallelGenerator

    val columnIndexSeq = 0 until getColumnSizeFromSchemaType(schema)
    val tColumnsFutures = columnIndexSeq.map { colIdx =>
      Future {
        (colIdx, toTColumn(rows, colIdx, getColumnType(schema, colIdx)))
      }
    }
    val tColumns = Await.result(Future.sequence(tColumnsFutures), Duration.Inf)
      .sortBy(_._1)
      .map(_._2)
      .asJava
    val tRowSet = new TRowSet(0, new JArrayList[TRow](rows.length))
    tRowSet.setColumns(tColumns)
    tRowSet
  }
}

object TRowSetGenerator {
  private lazy val tColumnParallelGenerator = ExecutionContext.fromExecutor(
    ThreadUtils.newForkJoinPool(prefix = "tcolumn-parallel-generator"))
}
