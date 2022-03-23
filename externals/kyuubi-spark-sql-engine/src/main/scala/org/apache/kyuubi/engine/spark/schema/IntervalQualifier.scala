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

package org.apache.kyuubi.engine.spark.schema

import java.time.Duration
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.types.DataType

object IntervalQualifier extends Enumeration {

  type IntervalQualifier = Value

  val DAY = new Val(0, "DAY")
  val HOUR = new Val(1, "HOUR")
  val MINUTE = new Val(3, "MINUTE")
  val SECOND = new Val(4, "SECOND")
  private final val SECOND_PER_MINUTE: Long = 60L
  private final val SECOND_PER_HOUR: Long = SECOND_PER_MINUTE * 60L
  private final val SECOND_PER_DAY: Long = SECOND_PER_HOUR * 24L

  def toDayTimeIntervalString(d: Duration, typ: DataType): String = {
    val endField = typ.getClass.getDeclaredField("endField")
    endField.setAccessible(true)
    val end = endField.get(typ).asInstanceOf[Byte]
    val to = IntervalQualifier(end)
    var sign = ""
    var rest = d.getSeconds
    if (d.getSeconds < 0) {
      sign = "-"
      rest = -rest
    }
    to match {
      case DAY =>
        val days = TimeUnit.SECONDS.toDays(rest)
        s"$sign$days 00:00:00.000000000"
      case HOUR =>
        val days = TimeUnit.SECONDS.toDays(rest)
        val hours = TimeUnit.SECONDS.toHours(rest % SECOND_PER_DAY)
        f"$sign$days $hours%02d:00:00.000000000"
      case MINUTE =>
        val days = TimeUnit.SECONDS.toDays(rest)
        rest %= SECOND_PER_DAY
        val hours = TimeUnit.SECONDS.toHours(rest)
        val minutes = TimeUnit.SECONDS.toMinutes(rest % SECOND_PER_HOUR)
        f"$sign$days $hours%02d:$minutes%02d:00.000000000"
      case SECOND =>
        val days = TimeUnit.SECONDS.toDays(rest)
        rest %= SECOND_PER_DAY
        val hours = TimeUnit.SECONDS.toHours(rest)
        rest %= SECOND_PER_HOUR
        val minutes = TimeUnit.SECONDS.toMinutes(rest)
        val seconds = rest % SECOND_PER_MINUTE
        f"$sign$days $hours%02d:$minutes%02d:$seconds%02d.${d.getNano}%09d"
    }
  }
}
