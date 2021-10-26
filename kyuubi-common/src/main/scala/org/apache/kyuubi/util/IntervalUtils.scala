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

import org.apache.kyuubi.util.DateTimeUtils._

object IntervalUtils {

  val MAX_DAY: Long = Long.MaxValue / MICROS_PER_DAY
  val MAX_HOUR: Long = Long.MaxValue / MICROS_PER_HOUR
  val MAX_MINUTE: Long = Long.MaxValue / MICROS_PER_MINUTE
  val MAX_SECOND: Long = Long.MaxValue / MICROS_PER_SECOND
  val MIN_SECOND: Long = Long.MinValue / MICROS_PER_SECOND

  // A thin fork of YearMonthIntervalType
  object YM {
    val YEAR: Byte = 0
    val MONTH: Byte = 1

    def fieldToString(field: Byte): String = field match {
      case YEAR => "year"
      case MONTH => "month"
      case invalid => throw new IllegalArgumentException(
        s"Invalid YearMonthIntervalType field code: $invalid")
    }
  }

  // A thin fork of DayTimeIntervalType
  object DT {
    val DAY: Byte = 0
    val HOUR: Byte = 1
    val MINUTE: Byte = 2
    val SECOND: Byte = 3

    def fieldToString(field: Byte): String = field match {
      case DAY => "day"
      case HOUR => "hour"
      case MINUTE => "minute"
      case SECOND => "second"
      case invalid => throw new IllegalArgumentException(
        s"Invalid DayTimeIntervalType field code: $invalid")
    }
  }
}
