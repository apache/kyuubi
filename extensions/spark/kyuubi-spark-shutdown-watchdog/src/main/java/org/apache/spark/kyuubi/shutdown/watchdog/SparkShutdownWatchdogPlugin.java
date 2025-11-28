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

import java.util.Collections;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.api.plugin.SparkPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Spark plugin entry point that wires the ShutdownWatchdog into the driver lifecycle. */
@SuppressWarnings("unused")
public class SparkShutdownWatchdogPlugin implements SparkPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(SparkShutdownWatchdogPlugin.class);

  @Override
  public DriverPlugin driverPlugin() {
    return new DriverPlugin() {
      private SparkConf sparkConf;

      @Override
      public Map<String, String> init(SparkContext sc, PluginContext pluginContext) {
        this.sparkConf = sc != null ? sc.getConf() : null;
        return Collections.emptyMap();
      }

      @Override
      public void shutdown() {
        if (sparkConf != null) {
          ShutdownWatchdog.startIfNeeded(sparkConf, LOG);
          sparkConf = null;
        } else {
          LOG.warn("Shutdown Watchdog driver plugin invoked without SparkConf.");
        }
      }
    };
  }

  @Override
  public ExecutorPlugin executorPlugin() {
    return new ExecutorPlugin() {
      @Override
      public void init(PluginContext context, Map<String, String> extraConf) {
        // no-op
      }

      @Override
      public void shutdown() {
        // no-op
      }
    };
  }
}
