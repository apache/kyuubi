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

import java.{lang, util}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}
import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

import org.apache.flink.table.catalog.Column
import org.apache.flink.table.data.StringData
import org.apache.flink.table.types.logical._
import org.apache.flink.types.Row
import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.engine.flink.result.ResultSet
import org.apache.kyuubi.util.RowSetUtils._

object RowSet {

  def resultSetToTRowSet(
      rows: Seq[Row],
      resultSet: ResultSet,
      protocolVersion: TProtocolVersion): TRowSet = {
    if (protocolVersion.getValue < TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      toRowBaseSet(rows, resultSet)
    } else {
      toColumnBasedSet(rows, resultSet)
    }
  }

  def toRowBaseSet(rows: Seq[Row], resultSet: ResultSet): TRowSet = {
    val rowSize = rows.size
    val tRows = new util.ArrayList[TRow](rowSize)
    var i = 0
    while (i < rowSize) {
      val row = rows(i)
      val tRow = new TRow()
      val columnSize = row.getArity
      var j = 0
      while (j < columnSize) {
        val columnValue = toTColumnValue(j, row, resultSet)
        tRow.addToColVals(columnValue)
        j += 1
      }
      tRows.add(tRow)
      i += 1
    }

    new TRowSet(0, tRows)
  }

  def toColumnBasedSet(rows: Seq[Row], resultSet: ResultSet): TRowSet = {
    val size = rows.length
    val tRowSet = new TRowSet(0, new util.ArrayList[TRow](size))
    val columnSize = resultSet.getColumns.size()
    var i = 0
    while (i < columnSize) {
      val field = resultSet.getColumns.get(i)
      val tColumn = toTColumn(rows, i, field.getDataType.getLogicalType)
      tRowSet.addToColumns(tColumn)
      i += 1
    }
    tRowSet
  }

  private def toTColumnValue(
      ordinal: Int,
      row: Row,
      resultSet: ResultSet): TColumnValue = {

    val column = resultSet.getColumns.get(ordinal)
    val logicalType = column.getDataType.getLogicalType

    logicalType match {
      case _: BooleanType =>
        val boolValue = new TBoolValue
        if (row.getField(ordinal) != null) {
          boolValue.setValue(row.getField(ordinal).asInstanceOf[Boolean])
        }
        TColumnValue.boolVal(boolValue)
      case _: TinyIntType =>
        val tByteValue = new TByteValue
        if (row.getField(ordinal) != null) {
          tByteValue.setValue(row.getField(ordinal).asInstanceOf[Byte])
        }
        TColumnValue.byteVal(tByteValue)
      case _: SmallIntType =>
        val tI16Value = new TI16Value
        if (row.getField(ordinal) != null) {
          tI16Value.setValue(row.getField(ordinal).asInstanceOf[Short])
        }
        TColumnValue.i16Val(tI16Value)
      case _: IntType =>
        val tI32Value = new TI32Value
        if (row.getField(ordinal) != null) {
          tI32Value.setValue(row.getField(ordinal).asInstanceOf[Int])
        }
        TColumnValue.i32Val(tI32Value)
      case _: BigIntType =>
        val tI64Value = new TI64Value
        if (row.getField(ordinal) != null) {
          tI64Value.setValue(row.getField(ordinal).asInstanceOf[Long])
        }
        TColumnValue.i64Val(tI64Value)
      case _: FloatType =>
        val tDoubleValue = new TDoubleValue
        if (row.getField(ordinal) != null) {
          val doubleValue = lang.Double.valueOf(row.getField(ordinal).asInstanceOf[Float].toString)
          tDoubleValue.setValue(doubleValue)
        }
        TColumnValue.doubleVal(tDoubleValue)
      case _: DoubleType =>
        val tDoubleValue = new TDoubleValue
        if (row.getField(ordinal) != null) {
          tDoubleValue.setValue(row.getField(ordinal).asInstanceOf[Double])
        }
        TColumnValue.doubleVal(tDoubleValue)
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
      case t =>
        val tStringValue = new TStringValue
        if (row.getField(ordinal) != null) {
          tStringValue.setValue(toHiveString((row.getField(ordinal), t)))
        }
        TColumnValue.stringVal(tStringValue)
    }
  }

  implicit private def bitSetToBuffer(bitSet: java.util.BitSet): ByteBuffer = {
    ByteBuffer.wrap(bitSet.toByteArray)
  }

  private def toTColumn(rows: Seq[Row], ordinal: Int, logicalType: LogicalType): TColumn = {
    val nulls = new java.util.BitSet()
    // for each column, determine the conversion class by sampling the first non-value value
    // if there's no row, set the entire column empty
    val sampleField = rows.iterator.map(_.getField(ordinal)).find(_ ne null).orNull
    logicalType match {
      case _: BooleanType =>
        val values = getOrSetAsNull[lang.Boolean](rows, ordinal, nulls, true)
        TColumn.boolVal(new TBoolColumn(values, nulls))
      case _: TinyIntType =>
        val values = getOrSetAsNull[lang.Byte](rows, ordinal, nulls, 0.toByte)
        TColumn.byteVal(new TByteColumn(values, nulls))
      case _: SmallIntType =>
        val values = getOrSetAsNull[lang.Short](rows, ordinal, nulls, 0.toShort)
        TColumn.i16Val(new TI16Column(values, nulls))
      case _: IntType =>
        val values = getOrSetAsNull[lang.Integer](rows, ordinal, nulls, 0)
        TColumn.i32Val(new TI32Column(values, nulls))
      case _: BigIntType =>
        val values = getOrSetAsNull[lang.Long](rows, ordinal, nulls, 0L)
        TColumn.i64Val(new TI64Column(values, nulls))
      case _: FloatType =>
        val values = getOrSetAsNull[lang.Float](rows, ordinal, nulls, 0.0f)
          .asScala.map(n => lang.Double.valueOf(n.toString)).asJava
        TColumn.doubleVal(new TDoubleColumn(values, nulls))
      case _: DoubleType =>
        val values = getOrSetAsNull[lang.Double](rows, ordinal, nulls, 0.0)
        TColumn.doubleVal(new TDoubleColumn(values, nulls))
      case t @ (_: VarCharType | _: CharType) =>
        val values: util.List[String] = new util.ArrayList[String](0)
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

  private def getOrSetAsNull[T](
      rows: Seq[Row],
      ordinal: Int,
      nulls: java.util.BitSet,
      defaultVal: T): java.util.List[T] = {
    val size = rows.length
    val ret = new java.util.ArrayList[T](size)
    var idx = 0
    while (idx < size) {
      val row = rows(idx)
      val isNull = row.getField(ordinal) == null
      if (isNull) {
        nulls.set(idx, true)
        ret.add(idx, defaultVal)
      } else {
        ret.add(idx, row.getFieldAs[T](ordinal))
      }
      idx += 1
    }
    ret
  }

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
    case _: ArrayType => TTypeId.ARRAY_TYPE
    case _: MapType => TTypeId.MAP_TYPE
    case _: RowType => TTypeId.STRUCT_TYPE
    case _: BinaryType => TTypeId.BINARY_TYPE
    case t @ (_: ZonedTimestampType | _: LocalZonedTimestampType | _: MultisetType |
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

      case (bin: Array[Byte], _: BinaryType) =>
        new String(bin, StandardCharsets.UTF_8)

      case (other, _) =>
        other.toString
    }
  }
}
