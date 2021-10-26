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
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField
import java.util.{Date, Locale}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.hive.service.rpc.thrift._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.apache.kyuubi.config.KyuubiConf.IntervalStyles._
import org.apache.kyuubi.util.DateTimeUtils._
import org.apache.kyuubi.util.IntervalUtils._

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

  private lazy val simpleDateFormatter = new SimpleDateFormat("yyyy-MM-dd", Locale.US)

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
        simpleDateFormatter.format(d)

      case (ld: LocalDate, DateType) =>
        dateFormatter.format(ld)

      case (t: Timestamp, TimestampType) =>
        simpleTimestampFormatter.format(t)

      case (i: Instant, TimestampType) =>
        timestampFormatter.withZone(timeZone).format(i)

//      case (d: Duration, DayTimeIntervalType(start, end)) =>
//        toDayTimeIntervalString(d.get(ChronoUnit.MICROS), style, start, end)

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

  /**
   * Converts an year-month interval as a number of months to its textual representation
   * which conforms to the ANSI SQL standard.
   *
   * @param months The number of months, positive or negative
   * @param style The style of textual representation of the interval
   * @param startField The start field (YEAR or MONTH) which the interval comprises of.
   * @param endField The end field (YEAR or MONTH) which the interval comprises of.
   * @return Year-month interval string
   *
   * A fork of o.a.s.sql.catalyst.util.IntervalUtils.toYearMonthIntervalString
   */
  def toYearMonthIntervalString(
    months: Int,
    style: IntervalStyle,
    startField: Byte,
    endField: Byte): String = {
    var sign = ""
    var absMonths: Long = months
    if (months < 0) {
      sign = "-"
      absMonths = -absMonths
    }
    val year = s"$sign${absMonths / MONTHS_PER_YEAR}"
    val yearAndMonth = s"$year-${absMonths % MONTHS_PER_YEAR}"
    style match {
      case ANSI =>
        val formatBuilder = new StringBuilder("INTERVAL '")
        if (startField == endField) {
          startField match {
            case YM.YEAR => formatBuilder.append(s"$year' YEAR")
            case YM.MONTH => formatBuilder.append(s"$months' MONTH")
          }
        } else {
          formatBuilder.append(s"$yearAndMonth' YEAR TO MONTH")
        }
        formatBuilder.toString
      case HIVE => s"$yearAndMonth"
    }
  }

  /**
   * Converts a day-time interval as a number of microseconds to its textual representation
   * which conforms to the ANSI SQL standard.
   *
   * @param micros The number of microseconds, positive or negative
   * @param style The style of textual representation of the interval
   * @param startField The start field (DAY, HOUR, MINUTE, SECOND) which the interval comprises of.
   * @param endField The end field (DAY, HOUR, MINUTE, SECOND) which the interval comprises of.
   * @return Day-time interval string
   *
   * A fork of o.a.s.sql.catalyst.util.IntervalUtils.toDayTimeIntervalString
   */
  def toDayTimeIntervalString(
    micros: Long,
    style: IntervalStyle,
    startField: Byte,
    endField: Byte): String = {
    var sign = ""
    var rest = micros
    val from = DT.fieldToString(startField).toUpperCase
    val to = DT.fieldToString(endField).toUpperCase
    val prefix = "INTERVAL '"
    val postfix = s"' ${if (startField == endField) from else s"$from TO $to"}"

    if (micros < 0) {
      if (micros == Long.MinValue) {
        // Especial handling of minimum `Long` value because negate op overflows `Long`.
        // seconds = 106751991 * (24 * 60 * 60) + 4 * 60 * 60 + 54 = 9223372036854
        // microseconds = -9223372036854000000L-775808 == Long.MinValue
        val baseStr = "-106751991 04:00:54.775808000"
        val minIntervalString = style match {
          case ANSI =>
            val firstStr = startField match {
              case DT.DAY => s"-$MAX_DAY"
              case DT.HOUR => s"-$MAX_HOUR"
              case DT.MINUTE => s"-$MAX_MINUTE"
              case DT.SECOND => s"-$MAX_SECOND.775808"
            }
            val followingStr = if (startField == endField) {
              ""
            } else {
              val substrStart = startField match {
                case DT.DAY => 10
                case DT.HOUR => 13
                case DT.MINUTE => 16
              }
              val substrEnd = endField match {
                case DT.HOUR => 13
                case DT.MINUTE => 16
                case DT.SECOND => 26
              }
              baseStr.substring(substrStart, substrEnd)
            }

            s"$prefix$firstStr$followingStr$postfix"
          case HIVE => baseStr
        }
        return minIntervalString
      } else {
        sign = "-"
        rest = -rest
      }
    }
    val intervalString = style match {
      case ANSI =>
        val formatBuilder = new mutable.StringBuilder(sign)
        val formatArgs = new mutable.ArrayBuffer[Long]()
        startField match {
          case DT.DAY =>
            formatBuilder.append(rest / MICROS_PER_DAY)
            rest %= MICROS_PER_DAY
          case DT.HOUR =>
            formatBuilder.append("%02d")
            formatArgs.append(rest / MICROS_PER_HOUR)
            rest %= MICROS_PER_HOUR
          case DT.MINUTE =>
            formatBuilder.append("%02d")
            formatArgs.append(rest / MICROS_PER_MINUTE)
            rest %= MICROS_PER_MINUTE
          case DT.SECOND =>
            val leadZero = if (rest < 10 * MICROS_PER_SECOND) "0" else ""
            formatBuilder.append(s"$leadZero" +
              s"${java.math.BigDecimal.valueOf(rest, 6).stripTrailingZeros.toPlainString}")
        }

        if (startField < DT.HOUR && DT.HOUR <= endField) {
          formatBuilder.append(" %02d")
          formatArgs.append(rest / MICROS_PER_HOUR)
          rest %= MICROS_PER_HOUR
        }
        if (startField < DT.MINUTE && DT.MINUTE <= endField) {
          formatBuilder.append(":%02d")
          formatArgs.append(rest / MICROS_PER_MINUTE)
          rest %= MICROS_PER_MINUTE
        }
        if (startField < DT.SECOND && DT.SECOND <= endField) {
          val leadZero = if (rest < 10 * MICROS_PER_SECOND) "0" else ""
          formatBuilder.append(
            s":$leadZero${java.math.BigDecimal.valueOf(rest, 6).stripTrailingZeros.toPlainString}")
        }
        s"$prefix${formatBuilder.toString.format(formatArgs.toSeq: _*)}$postfix"
      case HIVE =>
        val secondsWithFraction = rest % MICROS_PER_MINUTE
        rest /= MICROS_PER_MINUTE
        val minutes = rest % MINUTES_PER_HOUR
        rest /= MINUTES_PER_HOUR
        val hours = rest % HOURS_PER_DAY
        val days = rest / HOURS_PER_DAY
        val seconds = secondsWithFraction / MICROS_PER_SECOND
        val nanos = (secondsWithFraction % MICROS_PER_SECOND) * NANOS_PER_MICROS
        f"$sign$days $hours%02d:$minutes%02d:$seconds%02d.$nanos%09d"
    }
    intervalString
  }
}
