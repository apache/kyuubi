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

import java.sql.Date
import java.sql.Types._
import java.time.LocalDateTime
import java.util

import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId._
import org.apache.kyuubi.util.RowSetUtils.{bitSetToBuffer, formatDate, formatLocalDateTime}

class DefaultJdbcTRowSetGenerator extends JdbcTRowSetGenerator {

  override def toTColumn(rows: Seq[Seq[_]], ordinal: Int, sqlType: Int): TColumn =
    sqlType match {
      case BIT => toBitTColumn(rows, ordinal)
      case TINYINT => toTinyIntTColumn(rows, ordinal)
      case SMALLINT => toSmallIntTColumn(rows, ordinal)
      case INTEGER => toIntegerTColumn(rows, ordinal)
      case BIGINT => toBigIntTColumn(rows, ordinal)
      case REAL => toRealTColumn(rows, ordinal)
      case DOUBLE => toDoubleTColumn(rows, ordinal)
      case CHAR => toCharTColumn(rows, ordinal)
      case VARCHAR => toVarcharTColumn(rows, ordinal)
      case _ => toDefaultTColumn(rows, ordinal, sqlType)
    }

  override def toTColumnValue(ordinal: Int, row: Seq[_], types: Seq[Column]): TColumnValue =
    getColumnType(types, ordinal) match {
      case BIT => toBitTColumnValue(row, ordinal)
      case TINYINT => toTinyIntTColumnValue(row, ordinal)
      case SMALLINT => toSmallIntTColumnValue(row, ordinal)
      case INTEGER => toIntegerTColumnValue(row, ordinal)
      case BIGINT => toBigIntTColumnValue(row, ordinal)
      case REAL => toRealTColumnValue(row, ordinal)
      case DOUBLE => toDoubleTColumnValue(row, ordinal)
      case CHAR => toCharTColumnValue(row, ordinal)
      case VARCHAR => toVarcharTColumnValue(row, ordinal)
      case otherType => toDefaultTColumnValue(row, ordinal, otherType)
    }

  protected def toDefaultTColumn(rows: Seq[Seq[_]], ordinal: Int, sqlType: Int): TColumn = {
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

  protected def toBitTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    toTTypeColumn(BOOLEAN_TYPE, rows, ordinal)

  protected def toTinyIntTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    toTTypeColumn(TINYINT_TYPE, rows, ordinal)

  protected def toSmallIntTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    toTTypeColumn(SMALLINT_TYPE, rows, ordinal)

  protected def toIntegerTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    toTTypeColumn(INT_TYPE, rows, ordinal)

  protected def toBigIntTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    toTTypeColumn(BIGINT_TYPE, rows, ordinal)

  protected def toRealTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    toTTypeColumn(FLOAT_TYPE, rows, ordinal)

  protected def toDoubleTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    toTTypeColumn(DOUBLE_TYPE, rows, ordinal)

  protected def toCharTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    toTTypeColumn(CHAR_TYPE, rows, ordinal)

  protected def toVarcharTColumn(rows: Seq[Seq[_]], ordinal: Int): TColumn =
    toTTypeColumn(STRING_TYPE, rows, ordinal)

  // ==========================================================

  protected def toBitTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    toTTypeColumnVal(BOOLEAN_TYPE, row, ordinal)

  protected def toTinyIntTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    toTTypeColumnVal(TINYINT_TYPE, row, ordinal)

  protected def toSmallIntTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    toTTypeColumnVal(SMALLINT_TYPE, row, ordinal)

  protected def toIntegerTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    toTTypeColumnVal(INT_TYPE, row, ordinal)

  protected def toBigIntTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    toTTypeColumnVal(BIGINT_TYPE, row, ordinal)

  protected def toRealTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    toTTypeColumnVal(FLOAT_TYPE, row, ordinal)

  protected def toDoubleTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    toTTypeColumnVal(DOUBLE_TYPE, row, ordinal)

  protected def toCharTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    toTTypeColumnVal(STRING_TYPE, row, ordinal)

  protected def toVarcharTColumnValue(row: Seq[_], ordinal: Int): TColumnValue =
    toTTypeColumnVal(STRING_TYPE, row, ordinal)

  protected def toDefaultTColumnValue(row: Seq[_], ordinal: Int, sqlType: Int): TColumnValue = {
    val tStrValue = new TStringValue
    if (row(ordinal) != null) {
      tStrValue.setValue(
        toHiveString(row(ordinal), sqlType))
    }
    TColumnValue.stringVal(tStrValue)
  }

  protected def toHiveString(data: Any, sqlType: Int): String =
    (data, sqlType) match {
      case (date: Date, DATE) => formatDate(date)
      case (dateTime: LocalDateTime, TIMESTAMP) => formatLocalDateTime(dateTime)
      case (decimal: java.math.BigDecimal, DECIMAL) => decimal.toPlainString
      case (bigint: java.math.BigInteger, BIGINT) => bigint.toString()
      case (other, _) => other.toString
    }
}
