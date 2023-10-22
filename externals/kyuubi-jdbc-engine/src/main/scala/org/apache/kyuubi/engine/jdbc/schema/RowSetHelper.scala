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

import java.{lang, util}
import java.sql.{Date, Types}
import java.time.LocalDateTime

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.util.RowSetUtils.{bitSetToBuffer, formatDate, formatLocalDateTime}

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
      sqlType: Int): TColumn = {
    sqlType match {
      case Types.BIT =>
        toBitTColumn(rows, ordinal)

      case Types.TINYINT =>
        toTinyIntTColumn(rows, ordinal)

      case Types.SMALLINT =>
        toSmallIntTColumn(rows, ordinal)

      case Types.INTEGER =>
        toIntegerTColumn(rows, ordinal)

      case Types.BIGINT =>
        toBigIntTColumn(rows, ordinal)

      case Types.REAL =>
        toRealTColumn(rows, ordinal)

      case Types.DOUBLE =>
        toDoubleTColumn(rows, ordinal)

      case Types.CHAR =>
        toCharTColumn(rows, ordinal)

      case Types.VARCHAR =>
        toVarcharTColumn(rows, ordinal)

      case _ =>
        toDefaultTColumn(rows, ordinal, sqlType)
    }
  }

  protected def toTColumnValue(ordinal: Int, row: List[Any], types: List[Column]): TColumnValue = {
    types(ordinal).sqlType match {
      case Types.BIT =>
        toBitTColumnValue(row, ordinal)

      case Types.TINYINT =>
        toTinyIntTColumnValue(row, ordinal)

      case Types.SMALLINT =>
        toSmallIntTColumnValue(row, ordinal)

      case Types.INTEGER =>
        toIntegerTColumnValue(row, ordinal)

      case Types.BIGINT =>
        toBigIntTColumnValue(row, ordinal)

      case Types.REAL =>
        toRealTColumnValue(row, ordinal)

      case Types.DOUBLE =>
        toDoubleTColumnValue(row, ordinal)

      case Types.CHAR =>
        toCharTColumnValue(row, ordinal)

      case Types.VARCHAR =>
        toVarcharTColumnValue(row, ordinal)

      case _ =>
        toDefaultTColumnValue(row, ordinal, types)
    }
  }

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

  protected def toDefaultTColumn(rows: Seq[Seq[Any]], ordinal: Int, sqlType: Int): TColumn = {
    val nulls = new java.util.BitSet()
    val rowSize = rows.length
    val values = new util.ArrayList[String](rowSize)
    var i = 0
    while (i < rowSize) {
      val row = rows(i)
      nulls.set(i, row(ordinal) == null)
      val value =
        if (row(ordinal) == null) {
          ""
        } else {
          toHiveString(row(ordinal), sqlType)
        }
      values.add(value)
      i += 1
    }
    TColumn.stringVal(new TStringColumn(values, nulls))
  }

  protected def toBitTColumn(rows: Seq[Seq[Any]], ordinal: Int): TColumn = {
    val nulls = new java.util.BitSet()
    val values = getOrSetAsNull[java.lang.Boolean](rows, ordinal, nulls, true)
    TColumn.boolVal(new TBoolColumn(values, nulls))
  }

  protected def toTinyIntTColumn(rows: Seq[Seq[Any]], ordinal: Int): TColumn = {
    val nulls = new java.util.BitSet()
    val values = getOrSetAsNull[java.lang.Byte](rows, ordinal, nulls, 0.toByte)
    TColumn.byteVal(new TByteColumn(values, nulls))
  }

  protected def toSmallIntTColumn(rows: Seq[Seq[Any]], ordinal: Int): TColumn = {
    val nulls = new java.util.BitSet()
    val values = getOrSetAsNull[java.lang.Short](rows, ordinal, nulls, 0.toShort)
    TColumn.i16Val(new TI16Column(values, nulls))
  }

  protected def toIntegerTColumn(rows: Seq[Seq[Any]], ordinal: Int): TColumn = {
    val nulls = new java.util.BitSet()
    val values = getOrSetAsNull[java.lang.Integer](rows, ordinal, nulls, 0)
    TColumn.i32Val(new TI32Column(values, nulls))
  }

  protected def toBigIntTColumn(rows: Seq[Seq[Any]], ordinal: Int): TColumn = {
    val nulls = new java.util.BitSet()
    val values = getOrSetAsNull[lang.Long](rows, ordinal, nulls, 0L)
    TColumn.i64Val(new TI64Column(values, nulls))
  }

  protected def toRealTColumn(rows: Seq[Seq[Any]], ordinal: Int): TColumn = {
    val nulls = new java.util.BitSet()
    val values = getOrSetAsNull[lang.Float](rows, ordinal, nulls, 0.toFloat)
      .asScala.map(n => java.lang.Double.valueOf(n.toString)).asJava
    TColumn.doubleVal(new TDoubleColumn(values, nulls))
  }

  protected def toDoubleTColumn(rows: Seq[Seq[Any]], ordinal: Int): TColumn = {
    val nulls = new java.util.BitSet()
    val values = getOrSetAsNull[lang.Double](rows, ordinal, nulls, 0.toDouble)
    TColumn.doubleVal(new TDoubleColumn(values, nulls))
  }

  protected def toCharTColumn(rows: Seq[Seq[Any]], ordinal: Int): TColumn = {
    toVarcharTColumn(rows, ordinal)
  }

  protected def toVarcharTColumn(rows: Seq[Seq[Any]], ordinal: Int): TColumn = {
    val nulls = new java.util.BitSet()
    val values = getOrSetAsNull[String](rows, ordinal, nulls, "")
    TColumn.stringVal(new TStringColumn(values, nulls))
  }

  // ==========================================================

  protected def toBitTColumnValue(row: List[Any], ordinal: Int): TColumnValue = {
    val boolValue = new TBoolValue
    if (row(ordinal) != null) boolValue.setValue(row(ordinal).asInstanceOf[Boolean])
    TColumnValue.boolVal(boolValue)
  }

  protected def toTinyIntTColumnValue(row: List[Any], ordinal: Int): TColumnValue = {
    val byteValue = new TByteValue
    if (row(ordinal) != null) byteValue.setValue(row(ordinal).asInstanceOf[Byte])
    TColumnValue.byteVal(byteValue)
  }

  protected def toSmallIntTColumnValue(row: List[Any], ordinal: Int): TColumnValue = {
    val tI16Value = new TI16Value
    if (row(ordinal) != null) tI16Value.setValue(row(ordinal).asInstanceOf[Short])
    TColumnValue.i16Val(tI16Value)
  }

  protected def toIntegerTColumnValue(row: List[Any], ordinal: Int): TColumnValue = {
    val tI32Value = new TI32Value
    if (row(ordinal) != null) tI32Value.setValue(row(ordinal).asInstanceOf[Int])
    TColumnValue.i32Val(tI32Value)
  }

  protected def toBigIntTColumnValue(row: List[Any], ordinal: Int): TColumnValue = {
    val tI64Value = new TI64Value
    if (row(ordinal) != null) tI64Value.setValue(row(ordinal).asInstanceOf[Long])
    TColumnValue.i64Val(tI64Value)
  }

  protected def toRealTColumnValue(row: List[Any], ordinal: Int): TColumnValue = {
    val tDoubleValue = new TDoubleValue
    if (row(ordinal) != null) {
      val doubleValue = java.lang.Double.valueOf(row(ordinal).asInstanceOf[Float].toString)
      tDoubleValue.setValue(doubleValue)
    }
    TColumnValue.doubleVal(tDoubleValue)
  }

  protected def toDoubleTColumnValue(row: List[Any], ordinal: Int): TColumnValue = {
    val tDoubleValue = new TDoubleValue
    if (row(ordinal) != null) tDoubleValue.setValue(row(ordinal).asInstanceOf[Double])
    TColumnValue.doubleVal(tDoubleValue)
  }

  protected def toCharTColumnValue(row: List[Any], ordinal: Int): TColumnValue = {
    toVarcharTColumnValue(row, ordinal)
  }

  protected def toVarcharTColumnValue(row: List[Any], ordinal: Int): TColumnValue = {
    val tStringValue = new TStringValue
    if (row(ordinal) != null) tStringValue.setValue(row(ordinal).asInstanceOf[String])
    TColumnValue.stringVal(tStringValue)
  }

  protected def toDefaultTColumnValue(
      row: List[Any],
      ordinal: Int,
      types: List[Column]): TColumnValue = {
    val tStrValue = new TStringValue
    if (row(ordinal) != null) {
      tStrValue.setValue(
        toHiveString(row(ordinal), types(ordinal).sqlType))
    }
    TColumnValue.stringVal(tStrValue)
  }

  protected def toHiveString(data: Any, sqlType: Int): String = {
    (data, sqlType) match {
      case (date: Date, Types.DATE) =>
        formatDate(date)
      case (dateTime: LocalDateTime, Types.TIMESTAMP) =>
        formatLocalDateTime(dateTime)
      case (decimal: java.math.BigDecimal, Types.DECIMAL) =>
        decimal.toPlainString
      case (other, _) =>
        other.toString
    }
  }
}
