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

package org.apache.kyuubi.schema

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{Instant, LocalDate, ZoneId}
import java.time.chrono.IsoChronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, ResolverStyle}
import java.time.temporal.ChronoField
import java.util.{Date, Locale}

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.hive.service.rpc.thrift._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object RowSet {

  def toTRowSet(
      rows: Seq[Row],
      schema: StructType,
      protocolVersion: TProtocolVersion,
      timeZone: ZoneId): TRowSet = {
    if (protocolVersion.getValue < TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      toRowBasedSet(rows, schema, timeZone)
    } else {
      toColumnBasedSet(rows, schema, timeZone)
    }
  }

  def toRowBasedSet(rows: Seq[Row], schema: StructType, timeZone: ZoneId): TRowSet = {
    val tRows = rows.map { row =>
      val tRow = new TRow()
      (0 until row.length).map(i => toTColumnValue(i, row, schema, timeZone))
        .foreach(tRow.addToColVals)
      tRow
    }.asJava
    new TRowSet(0, tRows)
  }

  def toColumnBasedSet(rows: Seq[Row], schema: StructType, timeZone: ZoneId): TRowSet = {
    val size = rows.length
    val tRowSet = new TRowSet(0, new java.util.ArrayList[TRow](size))
    schema.zipWithIndex.foreach { case (filed, i) =>
      val tColumn = toTColumn(rows, i, filed.dataType, timeZone)
      tRowSet.addToColumns(tColumn)
    }
    tRowSet
  }

  private def toTColumn(rows: Seq[Row], ordinal: Int, typ: DataType, timeZone: ZoneId): TColumn = {
    val nulls = new java.util.BitSet()
    typ match {
      case BooleanType =>
        val values = getOrSetAsNull[java.lang.Boolean](rows, ordinal, nulls, true)
        TColumn.boolVal(new TBoolColumn(values, nulls))

      case ByteType =>
        val values = getOrSetAsNull[java.lang.Byte](rows, ordinal, nulls, 0.toByte)
        TColumn.byteVal(new TByteColumn(values, nulls))

      case ShortType =>
        val values = getOrSetAsNull[java.lang.Short](rows, ordinal, nulls, 0.toShort)
        TColumn.i16Val(new TI16Column(values, nulls))

      case IntegerType =>
        val values = getOrSetAsNull[java.lang.Integer](rows, ordinal, nulls, 0)
        TColumn.i32Val(new TI32Column(values, nulls))

      case LongType =>
        val values = getOrSetAsNull[java.lang.Long](rows, ordinal, nulls, 0L)
        TColumn.i64Val(new TI64Column(values, nulls))

      case FloatType =>
        val values = getOrSetAsNull[java.lang.Float](rows, ordinal, nulls, 0.toFloat)
          .asScala.map(n => java.lang.Double.valueOf(n.toDouble)).asJava
        TColumn.doubleVal(new TDoubleColumn(values, nulls))

      case DoubleType =>
        val values = getOrSetAsNull[java.lang.Double](rows, ordinal, nulls, 0.toDouble)
        TColumn.doubleVal(new TDoubleColumn(values, nulls))

      case StringType =>
        val values = getOrSetAsNull[java.lang.String](rows, ordinal, nulls, "")
        TColumn.stringVal(new TStringColumn(values, nulls))

      case BinaryType =>
        val values = getOrSetAsNull[Array[Byte]](rows, ordinal, nulls, Array())
          .asScala
          .map(ByteBuffer.wrap)
          .asJava
        TColumn.binaryVal(new TBinaryColumn(values, nulls))

      case _ =>
        val values = rows.zipWithIndex.toList.map { case (row, i) =>
          nulls.set(i, row.isNullAt(ordinal))
          if (row.isNullAt(ordinal)) {
            ""
          } else {
            toHiveString((row.get(ordinal), typ), timeZone)
          }
        }.asJava
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
      val isNull = row.isNullAt(ordinal)
      if (isNull) {
        nulls.set(idx, true)
        ret.add(idx, defaultVal)
      } else {
        ret.add(idx, row.getAs[T](ordinal))
      }
      idx += 1
    }
    ret
  }

  implicit private def bitSetToBuffer(bitSet: java.util.BitSet): ByteBuffer = {
    ByteBuffer.wrap(bitSet.toByteArray)
  }

  private def toTColumnValue(
      ordinal: Int,
      row: Row,
      types: StructType,
      timeZone: ZoneId): TColumnValue = {
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

      case _ =>
        val tStrValue = new TStringValue
        if (!row.isNullAt(ordinal)) {
          tStrValue.setValue(
            toHiveString((row.get(ordinal), types(ordinal).dataType), timeZone))
        }
        TColumnValue.stringVal(tStrValue)
    }
  }

  private def createBuilder(): DateTimeFormatterBuilder = {
    new DateTimeFormatterBuilder().parseCaseInsensitive()
  }

  private lazy val dateFormatter = {
    createBuilder().appendPattern("yyyy-MM-dd")
      .toFormatter(Locale.US)
      .withChronology(IsoChronology.INSTANCE)
  }

  private lazy val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.US)

  private lazy val timestampFormatter: DateTimeFormatter = {
    createBuilder().appendPattern("yyyy-MM-dd HH:mm:ss")
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
      .toFormatter(Locale.US)
      .withChronology(IsoChronology.INSTANCE)
  }

  private lazy val simpleTimestampFormatter = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.US)
  }

  /**
   * A simpler impl of Spark's toHiveString
   */
  def toHiveString(dataWithType: (Any, DataType), timeZone: ZoneId): String = {
    dataWithType match {
      case (null, _) =>
        // Only match nulls in nested type values
        "null"

      case (d: Date, DateType) =>
        simpleDateFormat.format(d)

      case (ld: LocalDate, DateType) =>
        dateFormatter.format(ld)

      case (t: Timestamp, TimestampType) =>
        simpleTimestampFormatter.format(t)

      case (i: Instant, TimestampType) =>
        timestampFormatter.withZone(timeZone).format(i)

      case (bin: Array[Byte], BinaryType) =>
        new String(bin, StandardCharsets.UTF_8)

      case (decimal: java.math.BigDecimal, DecimalType()) =>
        decimal.toPlainString

      case (s: String, StringType) =>
        // Only match string in nested type values
        "\"" + s + "\""

      case (seq: scala.collection.Seq[_], ArrayType(typ, _)) =>
        seq.map(v => (v, typ)).map(e => toHiveString(e, timeZone)).mkString("[", ",", "]")

      case (m: Map[_, _], MapType(kType, vType, _)) =>
        m.map { case (key, value) =>
          toHiveString((key, kType), timeZone) + ":" + toHiveString((value, vType), timeZone)
        }.toSeq.sorted.mkString("{", ",", "}")

      case (struct: Row, StructType(fields)) =>
        struct.toSeq.zip(fields).map { case (v, t) =>
          s""""${t.name}":${toHiveString((v, t.dataType), timeZone)}"""
        }.mkString("{", ",", "}")

      case (other, _) =>
        other.toString
    }
  }
}
