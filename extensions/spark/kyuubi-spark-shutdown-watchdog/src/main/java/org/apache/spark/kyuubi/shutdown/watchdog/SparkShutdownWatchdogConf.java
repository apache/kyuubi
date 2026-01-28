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

package org.apache.spark.kyuubi.shutdown.watchdog;

import org.apache.spark.SparkConf;

/**
 * Spark configuration helpers for the shutdown watchdog plugin.
 *
 * <p>Implemented in Java to avoid pulling Scala classes into the plugin artifact.
 */
final class SparkShutdownWatchdogConf {

  static final String SHUTDOWN_WATCHDOG_ENABLED_KEY = "spark.kyuubi.shutdown.watchdog.enabled";
  static final boolean SHUTDOWN_WATCHDOG_ENABLED_DEFAULT = true;

  static final String SHUTDOWN_WATCHDOG_TIMEOUT_KEY = "spark.kyuubi.shutdown.watchdog.timeout";
  private static final String SHUTDOWN_WATCHDOG_TIMEOUT_DEFAULT = "0ms";

  private SparkShutdownWatchdogConf() {}

  static boolean isEnabled(SparkConf conf) {
    return conf.getBoolean(SHUTDOWN_WATCHDOG_ENABLED_KEY, SHUTDOWN_WATCHDOG_ENABLED_DEFAULT);
  }

  static long getTimeoutMillis(SparkConf conf) {
    return conf.getTimeAsMs(SHUTDOWN_WATCHDOG_TIMEOUT_KEY, SHUTDOWN_WATCHDOG_TIMEOUT_DEFAULT);
  }
}
