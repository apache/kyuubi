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

package org.apache.kyuubi.engine.dataagent.util;

import org.apache.kyuubi.config.ConfigEntry;
import org.apache.kyuubi.config.KyuubiConf;
import org.apache.kyuubi.config.OptionalConfigEntry;

/** Small helpers for reading typed values out of {@link KyuubiConf}. */
public final class ConfUtils {

  private ConfUtils() {}

  /** Return the string value, or throw if the entry is not set. */
  public static String requireString(KyuubiConf conf, OptionalConfigEntry<String> key) {
    scala.Option<String> opt = conf.get(key);
    if (opt.isEmpty()) {
      throw new IllegalArgumentException(key.key() + " is required");
    }
    return opt.get();
  }

  /** Return the string value, or {@code null} if the entry is not set. */
  public static String optionalString(KyuubiConf conf, OptionalConfigEntry<String> key) {
    scala.Option<String> opt = conf.get(key);
    return opt.isDefined() ? opt.get() : null;
  }

  /** Return the value for a raw key, or {@code null} if not set. */
  public static String optionalString(KyuubiConf conf, String key) {
    scala.Option<String> opt = conf.getOption(key);
    return opt.isDefined() ? opt.get() : null;
  }

  public static int intConf(KyuubiConf conf, ConfigEntry<Object> key) {
    return ((Number) conf.get(key)).intValue();
  }

  public static long longConf(KyuubiConf conf, ConfigEntry<Object> key) {
    return ((Number) conf.get(key)).longValue();
  }

  /** Read a millisecond-valued entry and return it as whole seconds. */
  public static long millisAsSeconds(KyuubiConf conf, ConfigEntry<Object> key) {
    return longConf(conf, key) / 1000L;
  }
}
