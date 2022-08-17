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

package org.apache.kyuubi.spark.connector.common;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.network.util.ByteUnit;

/** Copied from Apache Spark org.apache.spark.network.util.JavaUtils */
public class JavaUtils {

  private static final Map<String, TimeUnit> timeSuffixes;

  static {
    timeSuffixes = new HashMap<>();
    timeSuffixes.put("us", TimeUnit.MICROSECONDS);
    timeSuffixes.put("ms", TimeUnit.MILLISECONDS);
    timeSuffixes.put("s", TimeUnit.SECONDS);
    timeSuffixes.put("m", TimeUnit.MINUTES);
    timeSuffixes.put("min", TimeUnit.MINUTES);
    timeSuffixes.put("h", TimeUnit.HOURS);
    timeSuffixes.put("d", TimeUnit.DAYS);
  }

  private static final Map<String, ByteUnit> byteSuffixes;

  static {
    byteSuffixes = new HashMap<>();
    byteSuffixes.put("b", ByteUnit.BYTE);
    byteSuffixes.put("k", ByteUnit.KiB);
    byteSuffixes.put("kb", ByteUnit.KiB);
    byteSuffixes.put("m", ByteUnit.MiB);
    byteSuffixes.put("mb", ByteUnit.MiB);
    byteSuffixes.put("g", ByteUnit.GiB);
    byteSuffixes.put("gb", ByteUnit.GiB);
    byteSuffixes.put("t", ByteUnit.TiB);
    byteSuffixes.put("tb", ByteUnit.TiB);
    byteSuffixes.put("p", ByteUnit.PiB);
    byteSuffixes.put("pb", ByteUnit.PiB);
  }

  /**
   * Convert a passed time string (e.g. 50s, 100ms, or 250us) to a time count in the given unit. The
   * unit is also considered the default if the given string does not specify a unit.
   */
  public static long timeStringAs(String str, TimeUnit unit) {
    String lower = str.toLowerCase(Locale.ROOT).trim();

    try {
      Matcher m = Pattern.compile("(-?[0-9]+)([a-z]+)?").matcher(lower);
      if (!m.matches()) {
        throw new NumberFormatException("Failed to parse time string: " + str);
      }

      long val = Long.parseLong(m.group(1));
      String suffix = m.group(2);

      // Check for invalid suffixes
      if (suffix != null && !timeSuffixes.containsKey(suffix)) {
        throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
      }

      // If suffix is valid use that, otherwise none was provided and use the default passed
      return unit.convert(val, suffix != null ? timeSuffixes.get(suffix) : unit);
    } catch (NumberFormatException e) {
      String timeError =
          "Time must be specified as seconds (s), "
              + "milliseconds (ms), microseconds (us), minutes (m or min), hour (h), or day (d). "
              + "E.g. 50s, 100ms, or 250us.";

      throw new NumberFormatException(timeError + "\n" + e.getMessage());
    }
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to milliseconds for internal use. If no
   * suffix is provided, the passed number is assumed to be in ms.
   */
  public static long timeStringAsMs(String str) {
    return timeStringAs(str, TimeUnit.MILLISECONDS);
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to seconds for internal use. If no
   * suffix is provided, the passed number is assumed to be in seconds.
   */
  public static long timeStringAsSec(String str) {
    return timeStringAs(str, TimeUnit.SECONDS);
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100kb, or 250mb) to the given. If no suffix is
   * provided, a direct conversion to the provided unit is attempted.
   */
  public static long byteStringAs(String str, ByteUnit unit) {
    String lower = str.toLowerCase(Locale.ROOT).trim();

    try {
      Matcher m = Pattern.compile("([0-9]+)([a-z]+)?").matcher(lower);
      Matcher fractionMatcher = Pattern.compile("([0-9]+\\.[0-9]+)([a-z]+)?").matcher(lower);

      if (m.matches()) {
        long val = Long.parseLong(m.group(1));
        String suffix = m.group(2);

        // Check for invalid suffixes
        if (suffix != null && !byteSuffixes.containsKey(suffix)) {
          throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
        }

        // If suffix is valid use that, otherwise none was provided and use the default passed
        return unit.convertFrom(val, suffix != null ? byteSuffixes.get(suffix) : unit);
      } else if (fractionMatcher.matches()) {
        throw new NumberFormatException(
            "Fractional values are not supported. Input was: " + fractionMatcher.group(1));
      } else {
        throw new NumberFormatException("Failed to parse byte string: " + str);
      }

    } catch (NumberFormatException e) {
      String byteError =
          "Size must be specified as bytes (b), "
              + "kibibytes (k), mebibytes (m), gibibytes (g), tebibytes (t), or pebibytes(p). "
              + "E.g. 50b, 100k, or 250m.";

      throw new NumberFormatException(byteError + "\n" + e.getMessage());
    }
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes for internal use.
   *
   * <p>If no suffix is provided, the passed number is assumed to be in bytes.
   */
  public static long byteStringAsBytes(String str) {
    return byteStringAs(str, ByteUnit.BYTE);
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to kibibytes for internal use.
   *
   * <p>If no suffix is provided, the passed number is assumed to be in kibibytes.
   */
  public static long byteStringAsKb(String str) {
    return byteStringAs(str, ByteUnit.KiB);
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to mebibytes for internal use.
   *
   * <p>If no suffix is provided, the passed number is assumed to be in mebibytes.
   */
  public static long byteStringAsMb(String str) {
    return byteStringAs(str, ByteUnit.MiB);
  }

  /**
   * Convert a passed byte string (e.g. 50b, 100k, or 250m) to gibibytes for internal use.
   *
   * <p>If no suffix is provided, the passed number is assumed to be in gibibytes.
   */
  public static long byteStringAsGb(String str) {
    return byteStringAs(str, ByteUnit.GiB);
  }
}
