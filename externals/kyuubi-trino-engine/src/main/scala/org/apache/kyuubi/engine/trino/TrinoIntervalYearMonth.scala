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

package org.apache.kyuubi.engine.trino

import io.trino.client.IntervalDayTime.formatMillis
import io.trino.client.IntervalYearMonth.toMonths

class TrinoIntervalYearMonth(val month: Int) extends Comparable[TrinoIntervalYearMonth] {

  override def compareTo(o: TrinoIntervalYearMonth): Int = {
    month.compareTo(month)
  }

  override def hashCode(): Int = month.hashCode()

  override def equals(obj: Any): Boolean = {
    if (this == obj) {
      true
    } else {
      obj match {
        case other: TrinoIntervalYearMonth if other.isInstanceOf[TrinoIntervalYearMonth] =>
          this.month == other.month
        case _ => false
      }
    }
  }

  override def toString: String = formatMillis(month)

}

object TrinoIntervalYearMonth {
  def apply(month: Int): TrinoIntervalDayTime = new TrinoIntervalDayTime(month)

  def apply(year: Int, months: Int): TrinoIntervalDayTime = {
    new TrinoIntervalDayTime(toMonths(year, months))
  }
}
