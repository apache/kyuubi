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

import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, TextStyle}
import java.time.temporal.ChronoField
import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.flink.table.catalog.Column
import org.apache.flink.table.types.logical._
import org.apache.flink.types.Row

import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.util.RowSetUtils._

object RowSet {

  def toTColumnDesc(field: Column, pos: Int): TColumnDesc = {
    val tColumnDesc = new TColumnDesc()
    tColumnDesc.setColumnName(field.getName)
    tColumnDesc.setTypeDesc(toTTypeDesc(field.getDataType.getLogicalType))
    tColumnDesc.setComment(field.getComment.orElse(""))
    tColumnDesc.setPosition(pos)
    tColumnDesc
  }

  def toTTypeDesc(typ: LogicalType): TTypeDesc = {
    val typeEntry = new TPrimitiveTypeEntry(toTTypeId(typ))
    typeEntry.setTypeQualifiers(toTTypeQualifiers(typ))
    val tTypeDesc = new TTypeDesc()
    tTypeDesc.addToTypes(TTypeEntry.primitiveEntry(typeEntry))
    tTypeDesc
  }

  def toTTypeQualifiers(typ: LogicalType): TTypeQualifiers = {
    val ret = new TTypeQualifiers()
    val qualifiers = typ match {
      case d: DecimalType =>
        Map(
          TCLIServiceConstants.PRECISION -> TTypeQualifierValue.i32Value(d.getPrecision),
          TCLIServiceConstants.SCALE -> TTypeQualifierValue.i32Value(d.getScale)).asJava
      case v: VarCharType =>
        Map(TCLIServiceConstants.CHARACTER_MAXIMUM_LENGTH ->
          TTypeQualifierValue.i32Value(v.getLength)).asJava
      case ch: CharType =>
        Map(TCLIServiceConstants.CHARACTER_MAXIMUM_LENGTH ->
          TTypeQualifierValue.i32Value(ch.getLength)).asJava
      case _ => Collections.emptyMap[String, TTypeQualifierValue]()
    }
    ret.setQualifiers(qualifiers)
    ret
  }

  def toTTypeId(typ: LogicalType): TTypeId = typ match {
    case _: NullType => TTypeId.NULL_TYPE
    case _: BooleanType => TTypeId.BOOLEAN_TYPE
    case _: FloatType => TTypeId.FLOAT_TYPE
    case _: DoubleType => TTypeId.DOUBLE_TYPE
    case _: VarCharType => TTypeId.VARCHAR_TYPE
    case _: CharType => TTypeId.CHAR_TYPE
    case _: TinyIntType => TTypeId.TINYINT_TYPE
    case _: SmallIntType => TTypeId.SMALLINT_TYPE
    case _: IntType => TTypeId.INT_TYPE
    case _: BigIntType => TTypeId.BIGINT_TYPE
    case _: DecimalType => TTypeId.DECIMAL_TYPE
    case _: DateType => TTypeId.DATE_TYPE
    case _: TimestampType => TTypeId.TIMESTAMP_TYPE
    case _: LocalZonedTimestampType => TTypeId.TIMESTAMPLOCALTZ_TYPE
    case _: ArrayType => TTypeId.ARRAY_TYPE
    case _: MapType => TTypeId.MAP_TYPE
    case _: RowType => TTypeId.STRUCT_TYPE
    case _: BinaryType => TTypeId.BINARY_TYPE
    case _: VarBinaryType => TTypeId.BINARY_TYPE
    case _: TimeType => TTypeId.STRING_TYPE
    case t @ (_: ZonedTimestampType | _: MultisetType |
        _: YearMonthIntervalType | _: DayTimeIntervalType) =>
      throw new IllegalArgumentException(
        "Flink data type `%s` is not supported currently".format(t.asSummaryString()),
        null)
    case other =>
      throw new IllegalArgumentException(s"Unrecognized type name: ${other.asSummaryString()}")
  }

  /**
   * A simpler impl of Flink's toHiveString
   * TODO: support Flink's new data type system
   */
  def toHiveString(dataWithType: (Any, LogicalType)): String = {
    dataWithType match {
      case (null, _) =>
        // Only match nulls in nested type values
        "null"

      case (d: Int, _: DateType) =>
        formatLocalDate(LocalDate.ofEpochDay(d))

      case (ld: LocalDate, _: DateType) =>
        formatLocalDate(ld)

      case (d: Date, _: DateType) =>
        formatInstant(d.toInstant)

      case (ldt: LocalDateTime, _: TimestampType) =>
        formatLocalDateTime(ldt)

      case (ts: Timestamp, _: TimestampType) =>
        formatInstant(ts.toInstant)

      case (decimal: java.math.BigDecimal, _: DecimalType) =>
        decimal.toPlainString

      case (a: Array[_], t: ArrayType) =>
        a.map(v => toHiveString((v, t.getElementType))).toSeq.mkString(
          "[",
          ",",
          "]")

      case (m: Map[_, _], t: MapType) =>
        m.map {
          case (k, v) =>
            toHiveString((k, t.getKeyType)) + ":" + toHiveString((v, t.getValueType))
        }
          .toSeq.mkString("{", ",", "}")

      case (r: Row, t: RowType) =>
        val lb = ListBuffer[String]()
        for (i <- 0 until r.getArity) {
          lb += s"""${t.getTypeAt(i).toString}:${toHiveString((r.getField(i), t.getTypeAt(i)))}"""
        }
        lb.toList.mkString("{", ",", "}")

      case (s: String, _ @(_: VarCharType | _: CharType)) =>
        // Only match string in nested type values
        "\"" + s + "\""

      case (bin: Array[Byte], _ @(_: BinaryType | _: VarBinaryType)) =>
        new String(bin, StandardCharsets.UTF_8)

      case (other, _) =>
        other.toString
    }
  }

  /** should stay in sync with org.apache.kyuubi.jdbc.hive.common.TimestampTZUtil */
  var TIMESTAMP_LZT_FORMATTER: DateTimeFormatter = {
    val builder = new DateTimeFormatterBuilder
    // Date part
    builder.append(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    // Time part
    builder
      .optionalStart
      .appendLiteral(" ")
      .append(DateTimeFormatter.ofPattern("HH:mm:ss"))
      .optionalStart
      .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
      .optionalEnd
      .optionalEnd

    // Zone part
    builder.optionalStart.appendLiteral(" ").optionalEnd
    builder.optionalStart.appendZoneText(TextStyle.NARROW).optionalEnd

    builder.toFormatter
  }
}
