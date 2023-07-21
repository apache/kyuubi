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

package org.apache.kyuubi.engine.chat.schema

import java.util

import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.util.RowSetUtils._

object RowSet {

  def emptyTRowSet(): TRowSet = {
    new TRowSet(0, new java.util.ArrayList[TRow](0))
  }

  def toTRowSet(
      rows: Seq[Array[String]],
      columnSize: Int,
      protocolVersion: TProtocolVersion): TRowSet = {
    if (protocolVersion.getValue < TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      toRowBasedSet(rows, columnSize)
    } else {
      toColumnBasedSet(rows, columnSize)
    }
  }

  def toRowBasedSet(rows: Seq[Array[String]], columnSize: Int): TRowSet = {
    val rowSize = rows.length
    val tRows = new java.util.ArrayList[TRow](rowSize)
    var i = 0
    while (i < rowSize) {
      val row = rows(i)
      val tRow = new TRow()
      var j = 0
      val columnSize = row.length
      while (j < columnSize) {
        val columnValue = stringTColumnValue(j, row)
        tRow.addToColVals(columnValue)
        j += 1
      }
      i += 1
      tRows.add(tRow)
    }
    new TRowSet(0, tRows)
  }

  def toColumnBasedSet(rows: Seq[Array[String]], columnSize: Int): TRowSet = {
    val rowSize = rows.length
    val tRowSet = new TRowSet(0, new util.ArrayList[TRow](rowSize))
    var i = 0
    while (i < columnSize) {
      val tColumn = toTColumn(rows, i)
      tRowSet.addToColumns(tColumn)
      i += 1
    }
    tRowSet
  }

  private def toTColumn(rows: Seq[Array[String]], ordinal: Int): TColumn = {
    val nulls = new java.util.BitSet()
    val values = getOrSetAsNull[String](rows, ordinal, nulls, "")
    TColumn.stringVal(new TStringColumn(values, nulls))
  }

  private def getOrSetAsNull[String](
      rows: Seq[Array[String]],
      ordinal: Int,
      nulls: util.BitSet,
      defaultVal: String): util.List[String] = {
    val size = rows.length
    val ret = new util.ArrayList[String](size)
    var idx = 0
    while (idx < size) {
      val row = rows(idx)
      val isNull = row(ordinal) == null
      if (isNull) {
        nulls.set(idx, true)
        ret.add(idx, defaultVal)
      } else {
        ret.add(idx, row(ordinal))
      }
      idx += 1
    }
    ret
  }

  private def stringTColumnValue(ordinal: Int, row: Array[String]): TColumnValue = {
    val tStringValue = new TStringValue
    if (row(ordinal) != null) tStringValue.setValue(row(ordinal))
    TColumnValue.stringVal(tStringValue)
  }
}
