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
package org.apache.kyuubi.engine.jdbc.doris

import java.sql.{Date, Types}
import java.time.LocalDateTime

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.engine.jdbc.schema.{Column, RowSetHelper}
import org.apache.kyuubi.util.RowSetUtils.{bitSetToBuffer, formatDate, formatLocalDateTime}

class DorisRowSetHelper extends RowSetHelper {

  protected def toTColumn(
      rows: Seq[Seq[Any]],
      ordinal: Int,
      sqlType: Int): TColumn = {
    val nulls = new java.util.BitSet()
    sqlType match {
      case Types.BIT =>
        val values = getOrSetAsNull[java.lang.Boolean](rows, ordinal, nulls, true)
        TColumn.boolVal(new TBoolColumn(values, nulls))

      case Types.TINYINT | Types.SMALLINT | Types.INTEGER =>
        val values = getOrSetAsNull[java.lang.Integer](rows, ordinal, nulls, 0)
        TColumn.i32Val(new TI32Column(values, nulls))

      case Types.BIGINT =>
        val values = getOrSetAsNull[java.lang.Long](rows, ordinal, nulls, 0L)
        TColumn.i64Val(new TI64Column(values, nulls))

      case Types.REAL =>
        val values = getOrSetAsNull[java.lang.Float](rows, ordinal, nulls, 0.toFloat)
          .asScala.map(n => java.lang.Double.valueOf(n.toString)).asJava
        TColumn.doubleVal(new TDoubleColumn(values, nulls))

      case Types.DOUBLE =>
        val values = getOrSetAsNull[java.lang.Double](rows, ordinal, nulls, 0.toDouble)
        TColumn.doubleVal(new TDoubleColumn(values, nulls))

      case Types.CHAR | Types.VARCHAR =>
        val values = getOrSetAsNull[String](rows, ordinal, nulls, "")
        TColumn.stringVal(new TStringColumn(values, nulls))

      case _ =>
        val rowSize = rows.length
        val values = new java.util.ArrayList[String](rowSize)
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
  }

  protected def toTColumnValue(ordinal: Int, row: List[Any], types: List[Column]): TColumnValue = {
    types(ordinal).sqlType match {
      case Types.BIT =>
        val boolValue = new TBoolValue
        if (row(ordinal) != null) boolValue.setValue(row(ordinal).asInstanceOf[Boolean])
        TColumnValue.boolVal(boolValue)

      case Types.TINYINT | Types.SMALLINT | Types.INTEGER =>
        val tI32Value = new TI32Value
        if (row(ordinal) != null) tI32Value.setValue(row(ordinal).asInstanceOf[Int])
        TColumnValue.i32Val(tI32Value)

      case Types.BIGINT =>
        val tI64Value = new TI64Value
        if (row(ordinal) != null) tI64Value.setValue(row(ordinal).asInstanceOf[Long])
        TColumnValue.i64Val(tI64Value)

      case Types.REAL =>
        val tDoubleValue = new TDoubleValue
        if (row(ordinal) != null) {
          val doubleValue = java.lang.Double.valueOf(row(ordinal).asInstanceOf[Float].toString)
          tDoubleValue.setValue(doubleValue)
        }
        TColumnValue.doubleVal(tDoubleValue)

      case Types.DOUBLE =>
        val tDoubleValue = new TDoubleValue
        if (row(ordinal) != null) tDoubleValue.setValue(row(ordinal).asInstanceOf[Double])
        TColumnValue.doubleVal(tDoubleValue)

      case Types.CHAR | Types.VARCHAR =>
        val tStringValue = new TStringValue
        if (row(ordinal) != null) tStringValue.setValue(row(ordinal).asInstanceOf[String])
        TColumnValue.stringVal(tStringValue)

      case _ =>
        val tStrValue = new TStringValue
        if (row(ordinal) != null) {
          tStrValue.setValue(
            toHiveString(row(ordinal), types(ordinal).sqlType))
        }
        TColumnValue.stringVal(tStrValue)
    }
  }

  protected def toHiveString(data: Any, sqlType: Int): String = {
    (data, sqlType) match {
      case (date: Date, Types.DATE) =>
        formatDate(date)
      case (dateTime: LocalDateTime, Types.TIMESTAMP) =>
        formatLocalDateTime(dateTime)
      case (decimal: java.math.BigDecimal, Types.DECIMAL) =>
        decimal.toPlainString
      // TODO support bitmap and hll
      case (other, _) =>
        other.toString
    }
  }
}
