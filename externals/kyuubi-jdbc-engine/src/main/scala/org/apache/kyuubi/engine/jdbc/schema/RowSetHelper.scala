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
package org.apache.kyuubi.engine.jdbc.schema

import java.util

import org.apache.hive.service.rpc.thrift._

abstract class RowSetHelper {

  def toTRowSet(
      rows: Seq[List[_]],
      columns: List[Column],
      protocolVersion: TProtocolVersion): TRowSet = {
    if (protocolVersion.getValue < TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      toRowBasedSet(rows, columns)
    } else {
      toColumnBasedSet(rows, columns)
    }
  }

  private def toRowBasedSet(rows: Seq[List[_]], columns: List[Column]): TRowSet = {
    val rowSize = rows.length
    val tRows = new util.ArrayList[TRow](rowSize)
    var i = 0
    while (i < rowSize) {
      val row = rows(i)
      val tRow = new TRow()
      val columnSize = row.size
      var j = 0
      while (j < columnSize) {
        val columnValue = toTColumnValue(j, row, columns)
        tRow.addToColVals(columnValue)
        j += 1
      }
      tRows.add(tRow)
      i += 1
    }
    new TRowSet(0, tRows)
  }

  private def toColumnBasedSet(rows: Seq[List[_]], columns: List[Column]): TRowSet = {
    val size = rows.size
    val tRowSet = new TRowSet(0, new java.util.ArrayList[TRow](size))
    val columnSize = columns.length
    var i = 0
    while (i < columnSize) {
      val field = columns(i)
      val tColumn = toTColumn(rows, i, field.sqlType)
      tRowSet.addToColumns(tColumn)
      i += 1
    }
    tRowSet
  }

  protected def toTColumn(
      rows: Seq[Seq[Any]],
      ordinal: Int,
      sqlType: Int): TColumn

  protected def toTColumnValue(ordinal: Int, row: List[Any], types: List[Column]): TColumnValue

  protected def getOrSetAsNull[T](
      rows: Seq[Seq[Any]],
      ordinal: Int,
      nulls: java.util.BitSet,
      defaultVal: T): java.util.List[T] = {
    val size = rows.length
    val ret = new java.util.ArrayList[T](size)
    var idx = 0
    while (idx < size) {
      val row = rows(idx)
      val isNull = row(ordinal) == null
      if (isNull) {
        nulls.set(idx, true)
        ret.add(idx, defaultVal)
      } else {
        ret.add(idx, row(ordinal).asInstanceOf[T])
      }
      idx += 1
    }
    ret
  }
}
