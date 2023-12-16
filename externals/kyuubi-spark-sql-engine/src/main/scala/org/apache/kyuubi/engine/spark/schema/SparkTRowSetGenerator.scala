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

package org.apache.kyuubi.engine.spark.schema

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.HiveResult
import org.apache.spark.sql.execution.HiveResult.TimeFormatters
import org.apache.spark.sql.types._

import org.apache.kyuubi.engine.schema.AbstractTRowSetGenerator
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId._
import org.apache.kyuubi.util.RowSetUtils.bitSetToBuffer

class SparkTRowSetGenerator
  extends AbstractTRowSetGenerator[StructType, Row, DataType] {

  // reused time formatters in single RowSet generation, see KYUUBI-5811
  private val tf = HiveResult.getTimeFormatters

  override def getColumnSizeFromSchemaType(schema: StructType): Int = schema.length

  override def getColumnType(schema: StructType, ordinal: Int): DataType = schema(ordinal).dataType

  override def isColumnNullAt(row: Row, ordinal: Int): Boolean = row.isNullAt(ordinal)

  override def getColumnAs[T](row: Row, ordinal: Int): T = row.getAs[T](ordinal)

  override def toTColumn(rows: Seq[Row], ordinal: Int, typ: DataType): TColumn = {
    val timeFormatters: TimeFormatters = tf
    val nulls = new java.util.BitSet()
    typ match {
      case BooleanType => toTTypeColumn(BOOLEAN_TYPE, rows, ordinal)
      case ByteType => toTTypeColumn(BINARY_TYPE, rows, ordinal)
      case ShortType => toTTypeColumn(TINYINT_TYPE, rows, ordinal)
      case IntegerType => toTTypeColumn(INT_TYPE, rows, ordinal)
      case LongType => toTTypeColumn(BIGINT_TYPE, rows, ordinal)
      case FloatType => toTTypeColumn(FLOAT_TYPE, rows, ordinal)
      case DoubleType => toTTypeColumn(DOUBLE_TYPE, rows, ordinal)
      case StringType => toTTypeColumn(STRING_TYPE, rows, ordinal)
      case BinaryType => toTTypeColumn(ARRAY_TYPE, rows, ordinal)
      case _ =>
        var i = 0
        val rowSize = rows.length
        val values = new java.util.ArrayList[String](rowSize)
        while (i < rowSize) {
          val row = rows(i)
          nulls.set(i, row.isNullAt(ordinal))
          values.add(RowSet.toHiveString(row.get(ordinal) -> typ, timeFormatters = timeFormatters))
          i += 1
        }
        TColumn.stringVal(new TStringColumn(values, nulls))
    }
  }

  override def toTColumnValue(ordinal: Int, row: Row, types: StructType): TColumnValue = {
    val timeFormatters: TimeFormatters = tf
    getColumnType(types, ordinal) match {
      case BooleanType => toTTypeColumnVal(BOOLEAN_TYPE, row, ordinal)
      case ByteType => toTTypeColumnVal(BINARY_TYPE, row, ordinal)
      case ShortType => toTTypeColumnVal(TINYINT_TYPE, row, ordinal)
      case IntegerType => toTTypeColumnVal(INT_TYPE, row, ordinal)
      case LongType => toTTypeColumnVal(BIGINT_TYPE, row, ordinal)
      case FloatType => toTTypeColumnVal(FLOAT_TYPE, row, ordinal)
      case DoubleType => toTTypeColumnVal(DOUBLE_TYPE, row, ordinal)
      case StringType => toTTypeColumnVal(STRING_TYPE, row, ordinal)
      case _ =>
        val tStrValue = new TStringValue
        if (!row.isNullAt(ordinal)) {
          tStrValue.setValue(RowSet.toHiveString(
            row.get(ordinal) -> types(ordinal).dataType,
            timeFormatters = timeFormatters))
        }
        TColumnValue.stringVal(tStrValue)
    }
  }

}
