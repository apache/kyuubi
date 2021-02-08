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

import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.text.Format
import java.time.{Instant, LocalDate, ZoneId}
import java.time.chrono.IsoChronology
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, ResolverStyle}
import java.time.temporal.ChronoField
import java.util.{Locale, TimeZone}

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

object Formatter {

  private val datePattern: String = "yyyy-MM-dd"
  private val timePattern: String = "HH:mm:ss"
  private val timestampPattern: String = s"$datePattern $timePattern"

  private val legacyDateFormatter: Format = FastDateFormat.getInstance(datePattern, Locale.US)

  private val dateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(datePattern, Locale.US)

  private val legacyTimestampFormatter: Format = FastDateFormat.getInstance(timestampPattern,
    TimeZone.getTimeZone(ZoneId.of(SQLConf.get.sessionLocalTimeZone)), Locale.US)

  private val timestampFormatter: DateTimeFormatter = new DateTimeFormatterBuilder()
    .parseCaseInsensitive()
    .append(DateTimeFormatter.ISO_LOCAL_DATE)
    .appendLiteral(' ')
    .appendValue(ChronoField.HOUR_OF_DAY, 2).appendLiteral(':')
    .appendValue(ChronoField.MINUTE_OF_HOUR, 2).appendLiteral(':')
    .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
    .toFormatter(Locale.US)
    .withChronology(IsoChronology.INSTANCE)
    .withResolverStyle(ResolverStyle.STRICT)
    .withZone(ZoneId.of(SQLConf.get.sessionLocalTimeZone))

  /**
   * Formats a datum (based on the given data type) and returns the string representation.
   */
  def formatValue(a: (Any, DataType), nested: Boolean = false): String = a match {
    case (null, _) => if (nested) "null" else "NULL"
    case (b, BooleanType) => b.toString
    case (d: Date, DateType) => format(d)
    case (ld: LocalDate, DateType) => format(ld)
    case (t: Timestamp, TimestampType) => format(t)
    case (i: Instant, TimestampType) => format(i)
    case (bin: Array[Byte], BinaryType) => new String(bin, StandardCharsets.UTF_8)
    case (decimal: java.math.BigDecimal, DecimalType()) => decimal.toPlainString
    case (n, _: NumericType) => n.toString
    case (s: String, StringType) => if (nested) "\"" + s + "\"" else s
    case (interval: CalendarInterval, CalendarIntervalType) => interval.toString
    case (seq: Seq[_], ArrayType(typ, _)) =>
      seq.map(v => (v, typ)).map(e => formatValue(e, true)).mkString("[", ",", "]")
    case (m: Map[_, _], MapType(kType, vType, _)) =>
      m.map { case (key, value) =>
        formatValue((key, kType), true) + ":" + formatValue((value, vType), true)
      }.toSeq.sorted.mkString("{", ",", "}")
    case (struct: Row, StructType(fields)) =>
      struct.toSeq.zip(fields).map { case (v, t) =>
        s""""${t.name}":${formatValue((v, t.dataType), true)}"""
      }.mkString("{", ",", "}")
    // UDT
    case (other, _) => other.toString
  }

  private def format(d: Date): String = legacyDateFormatter.format(d)

  private def format(ld: LocalDate): String = dateFormatter.format(ld)

  // The new formatter will omit the trailing 0 in the timestamp string, but the legacy formatter
  // can't. Here we use the legacy formatter to format the given timestamp up to seconds fractions,
  // and custom implementation to format the fractional part without trailing zeros.
  private def format(ts: Timestamp): String = {
    val formatted = legacyTimestampFormatter.format(ts)
    var nanos = ts.getNanos
    if (nanos == 0) {
      formatted
    } else {
      // Formats non-zero seconds fraction w/o trailing zeros. For example:
      //   formatted = '2020-05:27 15:55:30'
      //   nanos = 001234000
      // Counts the length of the fractional part: 001234000 -> 6
      var fracLen = 9
      while (nanos % 10 == 0) {
        nanos /= 10
        fracLen -= 1
      }
      // Places `nanos` = 1234 after '2020-05:27 15:55:30.'
      val fracOffset = formatted.length + 1
      val totalLen = fracOffset + fracLen
      // The buffer for the final result: '2020-05:27 15:55:30.001234'
      val buf = new Array[Char](totalLen)
      formatted.getChars(0, formatted.length, buf, 0)
      buf(formatted.length) = '.'
      var i = totalLen
      do {
        i -= 1
        buf(i) = ('0' + (nanos % 10)).toChar
        nanos /= 10
      } while (i > fracOffset)
      new String(buf)
    }
  }

  private def format(i: Instant): String = timestampFormatter.format(i)
}
