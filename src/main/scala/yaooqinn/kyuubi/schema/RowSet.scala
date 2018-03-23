/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.schema

import scala.collection.JavaConverters._

import org.apache.hive.service.cli.thrift._
import org.apache.spark.sql.{Row, SparkSQLUtils}
import org.apache.spark.sql.types._

/**
 * A result set of Spark's [[Row]]s with its [[StructType]] as its schema, with the ability of
 * transform to [[TRowSet]].
 */
case class RowSet(types: StructType, rows: Iterator[Row]) {

  def toTRowSet: TRowSet = new TRowSet(0, toTRows(rows).asJava)

  private[this] def toTRows(rows: Iterator[Row]): List[TRow] = {
    rows.map(toTRow).toList
  }

  private[this] def toTRow(row: Row): TRow = {
    val tRow = new TRow()
    (0 until row.length).map(i => toTColumnValue(i, row)).foreach(tRow.addToColVals)
    tRow
  }

  private[this] def toTColumnValue(ordinal: Int, row: Row): TColumnValue =
    types(ordinal).dataType match {
      case BooleanType =>
        val boolValue = new TBoolValue
        if (!row.isNullAt(ordinal)) boolValue.setValue(row.getBoolean(ordinal))
        TColumnValue.boolVal(boolValue)

      case ByteType =>
        val byteValue = new TByteValue
        if (!row.isNullAt(ordinal)) byteValue.setValue(row.getByte(ordinal))
        TColumnValue.byteVal(byteValue)

      case ShortType =>
        val tI16Value = new TI16Value
        if (!row.isNullAt(ordinal)) tI16Value.setValue(row.getShort(ordinal))
        TColumnValue.i16Val(tI16Value)

      case IntegerType =>
        val tI32Value = new TI32Value
        if (!row.isNullAt(ordinal)) tI32Value.setValue(row.getInt(ordinal))
        TColumnValue.i32Val(tI32Value)

      case LongType =>
        val tI64Value = new TI64Value
        if (!row.isNullAt(ordinal)) tI64Value.setValue(row.getLong(ordinal))
        TColumnValue.i64Val(tI64Value)

      case FloatType =>
        val tDoubleValue = new TDoubleValue
        if (!row.isNullAt(ordinal)) tDoubleValue.setValue(row.getFloat(ordinal))
        TColumnValue.doubleVal(tDoubleValue)

      case DoubleType =>
        val tDoubleValue = new TDoubleValue
        if (!row.isNullAt(ordinal)) tDoubleValue.setValue(row.getDouble(ordinal))
        TColumnValue.doubleVal(tDoubleValue)

      case StringType =>
        val tStringValue = new TStringValue
        if (!row.isNullAt(ordinal)) tStringValue.setValue(row.getString(ordinal))
        TColumnValue.stringVal(tStringValue)

      case DecimalType() =>
        val tStrValue = new TStringValue
        if (!row.isNullAt(ordinal)) tStrValue.setValue(row.getDecimal(ordinal).toString)
        TColumnValue.stringVal(tStrValue)

      case DateType =>
        val tStringValue = new TStringValue
        if (!row.isNullAt(ordinal)) tStringValue.setValue(row.getDate(ordinal).toString)
        TColumnValue.stringVal(tStringValue)

      case TimestampType =>
        val tStringValue = new TStringValue
        if (!row.isNullAt(ordinal)) tStringValue.setValue(row.getTimestamp(ordinal).toString)
        TColumnValue.stringVal(tStringValue)

      case BinaryType =>
        val tStringValue = new TStringValue
        if (!row.isNullAt(ordinal)) tStringValue.setValue(row.getAs[Array[Byte]](ordinal).toString)
        TColumnValue.stringVal(tStringValue)

      case _: ArrayType | _: StructType | _: MapType =>
        val tStrValue = new TStringValue
        if (!row.isNullAt(ordinal)) {
          tStrValue.setValue(
            SparkSQLUtils.toHiveString((row.get(ordinal), types(ordinal).dataType)))
        }
        TColumnValue.stringVal(tStrValue)
    }
}
