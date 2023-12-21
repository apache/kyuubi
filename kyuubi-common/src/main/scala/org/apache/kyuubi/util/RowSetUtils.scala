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

package org.apache.kyuubi.util

import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, ZoneId}
import java.time.chrono.IsoChronology
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

private[kyuubi] object RowSetUtils {

  final private val SECOND_PER_MINUTE: Long = 60L
  final private val SECOND_PER_HOUR: Long = SECOND_PER_MINUTE * 60L
  final private val SECOND_PER_DAY: Long = SECOND_PER_HOUR * 24L

  private lazy val dateFormatter = createDateTimeFormatterBuilder()
    .appendPattern("yyyy-MM-dd")
    .toFormatter(Locale.US)
    .withChronology(IsoChronology.INSTANCE)

  private lazy val legacyDateFormatter = FastDateFormat.getInstance("yyyy-MM-dd", Locale.US)

  private lazy val timeFormatter = createDateTimeFormatterBuilder()
    .appendPattern("HH:mm:ss")
    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
    .toFormatter(Locale.US)
    .withChronology(IsoChronology.INSTANCE)

  private lazy val timestampFormatter = createDateTimeFormatterBuilder()
    .appendPattern("yyyy-MM-dd HH:mm:ss")
    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
    .toFormatter(Locale.US)
    .withChronology(IsoChronology.INSTANCE)

  private def createDateTimeFormatterBuilder(): DateTimeFormatterBuilder = {
    new DateTimeFormatterBuilder().parseCaseInsensitive()
  }

  def formatDate(d: Date): String = {
    legacyDateFormatter.format(d)
  }

  def formatLocalDate(ld: LocalDate): String = {
    dateFormatter.format(ld)
  }

  def formatLocalTime(lt: LocalTime): String = {
    timeFormatter.format(lt)
  }

  def formatLocalDateTime(ldt: LocalDateTime): String = {
    timestampFormatter.format(ldt)
  }

  def formatInstant(i: Instant, timeZone: Option[ZoneId] = None): String = {
    timeZone.map(timestampFormatter.withZone(_).format(i))
      .getOrElse(timestampFormatter.format(i))
  }
}
