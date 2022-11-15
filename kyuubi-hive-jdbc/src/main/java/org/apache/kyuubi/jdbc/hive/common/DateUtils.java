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

package org.apache.kyuubi.jdbc.hive.common;

import java.util.TimeZone;

/** DateUtils. Thread-safe class */
public class DateUtils {
  public static int parseNumericValueWithRange(
      String fieldName, String strVal, int minValue, int maxValue) throws IllegalArgumentException {
    int result = 0;
    if (strVal != null) {
      result = Integer.parseInt(strVal);
      if (result < minValue || result > maxValue) {
        throw new IllegalArgumentException(
            String.format(
                "%s value %d outside range [%d, %d]", fieldName, result, minValue, maxValue));
      }
    }
    return result;
  }

  // From java.util.Calendar
  private static final String[] FIELD_NAME = {
    "ERA",
    "YEAR",
    "MONTH",
    "WEEK_OF_YEAR",
    "WEEK_OF_MONTH",
    "DAY_OF_MONTH",
    "DAY_OF_YEAR",
    "DAY_OF_WEEK",
    "DAY_OF_WEEK_IN_MONTH",
    "AM_PM",
    "HOUR",
    "HOUR_OF_DAY",
    "MINUTE",
    "SECOND",
    "MILLISECOND",
    "ZONE_OFFSET",
    "DST_OFFSET"
  };

  /**
   * Returns the name of the specified calendar field.
   *
   * @param field the calendar field
   * @return the calendar field name
   * @throws IndexOutOfBoundsException if <code>field</code> is negative, equal to or greater then
   *     <code>FIELD_COUNT</code>.
   */
  public static String getFieldName(int field) {
    return FIELD_NAME[field];
  }

  /**
   * Converts the internal representation of a SQL DATE (int) to the Java type ({@link
   * java.sql.Date}).
   */
  public static java.sql.Date internalToDate(int v) {
    // note that, in this case, can't handle Daylight Saving Time
    final long t = v * MILLIS_PER_DAY;
    return new java.sql.Date(t - TimeZone.getDefault().getOffset(t));
  }

  /**
   * The number of milliseconds in a day.
   *
   * <p>This is the modulo 'mask' used when converting TIMESTAMP values to DATE and TIME values.
   */
  private static final long MILLIS_PER_DAY = 86400000L; // = 24 * 60 * 60 * 1000
}
