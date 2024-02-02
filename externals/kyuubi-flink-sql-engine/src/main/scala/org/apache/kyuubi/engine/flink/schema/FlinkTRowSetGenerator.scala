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

import org.apache.flink.table.data.StringData
import org.apache.flink.table.types.logical._
import org.apache.flink.types.Row

import org.apache.kyuubi.engine.flink.result.ResultSet
import org.apache.kyuubi.engine.flink.schema.RowSet.{toHiveString, TIMESTAMP_LZT_FORMATTER}
import org.apache.kyuubi.engine.result.TRowSetGenerator
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

class FlinkTRowSetGenerator(zoneId: ZoneId)
  extends TRowSetGenerator[ResultSet, Row, LogicalType] {
  override def getColumnSizeFromSchemaType(schema: ResultSet): Int = schema.columns.size

  override def getColumnType(schema: ResultSet, ordinal: Int): LogicalType =
    schema.columns.get(ordinal).getDataType.getLogicalType

  override def isColumnNullAt(row: Row, ordinal: Int): Boolean = row.getField(ordinal) == null

  override def getColumnAs[T](row: Row, ordinal: Int): T = row.getFieldAs[T](ordinal)

  override def toTColumnValue(row: Row, ordinal: Int, types: ResultSet): TColumnValue = {
    getColumnType(types, ordinal) match {
      case _: BooleanType => asBooleanTColumnValue(row, ordinal)
      case _: TinyIntType => asByteTColumnValue(row, ordinal)
      case _: SmallIntType => asShortTColumnValue(row, ordinal)
      case _: IntType => asIntegerTColumnValue(row, ordinal)
      case _: BigIntType => asLongTColumnValue(row, ordinal)
      case _: DoubleType => asDoubleTColumnValue(row, ordinal)
      case _: FloatType => asFloatTColumnValue(row, ordinal)
      case t @ (_: VarCharType | _: CharType) =>
        asStringTColumnValue(
          row,
          ordinal,
          convertFunc = {
            case value: String => value
            case value: StringData => value.toString
            case null => null
            case other => throw new IllegalArgumentException(
                s"Unsupported conversion class ${other.getClass} for type ${t.getClass}.")
          })
      case _: LocalZonedTimestampType =>
        asStringTColumnValue(
          row,
          ordinal,
          rawValue =>
            TIMESTAMP_LZT_FORMATTER.format(
              ZonedDateTime.ofInstant(rawValue.asInstanceOf[Instant], zoneId)))
      case t => asStringTColumnValue(row, ordinal, rawValue => toHiveString((rawValue, t)))
    }
  }

  override def toTColumn(rows: Seq[Row], ordinal: Int, logicalType: LogicalType): TColumn = {
    // for each column, determine the conversion class by sampling the first non-value value
    // if there's no row, set the entire column empty
    logicalType match {
      case _: BooleanType => asBooleanTColumn(rows, ordinal)
      case _: TinyIntType => asByteTColumn(rows, ordinal)
      case _: SmallIntType => asShortTColumn(rows, ordinal)
      case _: IntType => asIntegerTColumn(rows, ordinal)
      case _: BigIntType => asLongTColumn(rows, ordinal)
      case _: FloatType => asFloatTColumn(rows, ordinal)
      case _: DoubleType => asDoubleTColumn(rows, ordinal)
      case t @ (_: VarCharType | _: CharType) =>
        val sampleField = rows.iterator.map(_.getField(ordinal)).find(_ ne null).orNull
        sampleField match {
          case _: String => asStringTColumn(rows, ordinal)
          case _: StringData =>
            asStringTColumn(
              rows,
              ordinal,
              convertFunc = (row, ordinal) => getColumnAs[StringData](row, ordinal).toString)
          case null => asStringTColumn(rows, ordinal)
          case other => throw new IllegalArgumentException(
              s"Unsupported conversion class ${other.getClass} for type ${t.getClass}.")
        }
      case _: LocalZonedTimestampType =>
        asStringTColumn(
          rows,
          ordinal,
          TIMESTAMP_LZT_FORMATTER.format(ZonedDateTime.ofInstant(Instant.EPOCH, zoneId)),
          (row, ordinal) =>
            TIMESTAMP_LZT_FORMATTER.format(
              ZonedDateTime.ofInstant(getColumnAs[Instant](row, ordinal), zoneId)))
      case _ =>
        asStringTColumn(
          rows,
          ordinal,
          convertFunc = (row, ordinal) => toHiveString((row.getField(ordinal), logicalType)))
    }
  }

}
