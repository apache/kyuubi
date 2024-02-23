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

import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

trait TRowSetGenerator[SchemaT, RowT, ColumnT]
  extends TColumnValueGenerator[RowT] with TColumnGenerator[RowT] {

  def getColumnSizeFromSchemaType(schema: SchemaT): Int

  def getColumnType(schema: SchemaT, ordinal: Int): ColumnT

  def toTColumn(rows: Seq[RowT], ordinal: Int, typ: ColumnT): TColumn

  def toTColumnValue(row: RowT, ordinal: Int, types: SchemaT): TColumnValue

  def toTRowSet(rows: Seq[RowT], schema: SchemaT, protocolVersion: TProtocolVersion): TRowSet = {
    if (protocolVersion.getValue < TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      toRowBasedSet(rows, schema)
    } else {
      toColumnBasedSet(rows, schema)
    }
  }

  def toRowBasedSet(rows: Seq[RowT], schema: SchemaT): TRowSet = {
    val tRows = rows.map { row =>
      var i = 0
      val columnSize = getColumnSizeFromSchemaType(schema)
      val tColumnValues = new JArrayList[TColumnValue](columnSize)
      while (i < columnSize) {
        val columnValue = toTColumnValue(row, i, schema)
        tColumnValues.add(columnValue)
        i += 1
      }
      new TRow(tColumnValues)
    }.asJava
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
}
