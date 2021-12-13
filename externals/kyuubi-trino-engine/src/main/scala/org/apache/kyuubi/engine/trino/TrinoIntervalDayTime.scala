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
import io.trino.client.IntervalDayTime.toMillis

class TrinoIntervalDayTime(val milliSeconds: Long) extends Comparable[TrinoIntervalDayTime]{

  override def compareTo(o: TrinoIntervalDayTime): Int = {
    milliSeconds.compareTo(milliSeconds)
  }

  override def hashCode(): Int = milliSeconds.hashCode()

  override def equals(obj: Any): Boolean = {
    if (this == obj) {
      true
    } else {
      obj match {
        case other: TrinoIntervalDayTime if other.isInstanceOf[TrinoIntervalDayTime] =>
          this.milliSeconds == other.milliSeconds
        case _ => false
      }
    }
  }

  override def toString: String = formatMillis(milliSeconds)
}

object TrinoIntervalDayTime {
  def apply(milliSeconds: Long): TrinoIntervalDayTime = new TrinoIntervalDayTime(milliSeconds)

  def apply(day: Int, hour: Int, minute: Int, second: Int, millis: Int): TrinoIntervalDayTime = {
    new TrinoIntervalDayTime(toMillis(day, hour, minute, second, millis))
  }
}
