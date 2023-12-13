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
package org.apache.kyuubi.engine.flink.schema

import java.time.{Instant, ZonedDateTime, ZoneId}

import scala.collection.JavaConverters._

import org.apache.flink.table.data.StringData
import org.apache.flink.table.types.logical._
import org.apache.flink.types.Row

import org.apache.kyuubi.engine.flink.result.ResultSet
import org.apache.kyuubi.engine.flink.schema.RowSet.{toHiveString, TIMESTAMP_LZT_FORMATTER}
import org.apache.kyuubi.engine.schema.AbstractTRowSetGenerator
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId._
import org.apache.kyuubi.util.RowSetUtils.bitSetToBuffer

class FlinkTRowSetGenerator(zoneId: ZoneId)
  extends AbstractTRowSetGenerator[ResultSet, Row, LogicalType] {
  override def getColumnSizeFromSchemaType(schema: ResultSet): Int = schema.columns.size

  override def getColumnType(schema: ResultSet, ordinal: Int): LogicalType =
    schema.columns.get(ordinal).getDataType.getLogicalType

  override def isColumnNullAt(row: Row, ordinal: Int): Boolean = row.getField(ordinal) == null

  override def getColumnAs[T](row: Row, ordinal: Int): T = row.getFieldAs[T](ordinal)

  override def toTColumnValue(ordinal: Int, row: Row, types: ResultSet): TColumnValue = {
    getColumnType(types, ordinal) match {
      case _: BooleanType => toTTypeColumnVal(BOOLEAN_TYPE, row, ordinal)
      case _: TinyIntType => toTTypeColumnVal(BINARY_TYPE, row, ordinal)
      case _: SmallIntType => toTTypeColumnVal(TINYINT_TYPE, row, ordinal)
      case _: IntType => toTTypeColumnVal(INT_TYPE, row, ordinal)
      case _: BigIntType => toTTypeColumnVal(BIGINT_TYPE, row, ordinal)
      case _: DoubleType => toTTypeColumnVal(DOUBLE_TYPE, row, ordinal)
      case _: FloatType => toTTypeColumnVal(FLOAT_TYPE, row, ordinal)
      case t @ (_: VarCharType | _: CharType) =>
        val tStringValue = new TStringValue
        val fieldValue = row.getField(ordinal)
        fieldValue match {
          case value: String =>
            tStringValue.setValue(value)
          case value: StringData =>
            tStringValue.setValue(value.toString)
          case null =>
            tStringValue.setValue(null)
          case other =>
            throw new IllegalArgumentException(
              s"Unsupported conversion class ${other.getClass} " +
                s"for type ${t.getClass}.")
        }
        TColumnValue.stringVal(tStringValue)
      case _: LocalZonedTimestampType =>
        val tStringValue = new TStringValue
        val fieldValue = row.getField(ordinal)
        tStringValue.setValue(TIMESTAMP_LZT_FORMATTER.format(
          ZonedDateTime.ofInstant(fieldValue.asInstanceOf[Instant], zoneId)))
        TColumnValue.stringVal(tStringValue)
      case t =>
        val tStringValue = new TStringValue
        if (row.getField(ordinal) != null) {
          tStringValue.setValue(toHiveString((row.getField(ordinal), t)))
        }
        TColumnValue.stringVal(tStringValue)
    }
  }

  override def toTColumn(rows: Seq[Row], ordinal: Int, logicalType: LogicalType): TColumn = {
    val nulls = new java.util.BitSet()
    // for each column, determine the conversion class by sampling the first non-value value
    // if there's no row, set the entire column empty
    val sampleField = rows.iterator.map(_.getField(ordinal)).find(_ ne null).orNull
    logicalType match {
      case _: BooleanType => toTTypeColumn(BOOLEAN_TYPE, rows, ordinal)
      case _: TinyIntType => toTTypeColumn(BINARY_TYPE, rows, ordinal)
      case _: SmallIntType => toTTypeColumn(TINYINT_TYPE, rows, ordinal)
      case _: IntType => toTTypeColumn(INT_TYPE, rows, ordinal)
      case _: BigIntType => toTTypeColumn(BIGINT_TYPE, rows, ordinal)
      case _: FloatType => toTTypeColumn(FLOAT_TYPE, rows, ordinal)
      case _: DoubleType => toTTypeColumn(DOUBLE_TYPE, rows, ordinal)
      case t @ (_: VarCharType | _: CharType) =>
        val values: java.util.List[String] = new java.util.ArrayList[String](0)
        sampleField match {
          case _: String =>
            values.addAll(getOrSetAsNull[String](rows, ordinal, nulls, ""))
          case _: StringData =>
            val stringDataValues =
              getOrSetAsNull[StringData](rows, ordinal, nulls, StringData.fromString(""))
            stringDataValues.forEach(e => values.add(e.toString))
          case null =>
            values.addAll(getOrSetAsNull[String](rows, ordinal, nulls, ""))
          case other =>
            throw new IllegalArgumentException(
              s"Unsupported conversion class ${other.getClass} " +
                s"for type ${t.getClass}.")
        }
        TColumn.stringVal(new TStringColumn(values, nulls))
      case _: LocalZonedTimestampType =>
        val values = getOrSetAsNull[Instant](rows, ordinal, nulls, Instant.EPOCH)
          .toArray().map(v =>
            TIMESTAMP_LZT_FORMATTER.format(
              ZonedDateTime.ofInstant(v.asInstanceOf[Instant], zoneId)))
        TColumn.stringVal(new TStringColumn(values.toList.asJava, nulls))
      case _ =>
        var i = 0
        val rowSize = rows.length
        val values = new java.util.ArrayList[String](rowSize)
        while (i < rowSize) {
          val row = rows(i)
          nulls.set(i, row.getField(ordinal) == null)
          val value =
            if (row.getField(ordinal) == null) {
              ""
            } else {
              toHiveString((row.getField(ordinal), logicalType))
            }
          values.add(value)
          i += 1
        }
        TColumn.stringVal(new TStringColumn(values, nulls))
    }
  }

}
