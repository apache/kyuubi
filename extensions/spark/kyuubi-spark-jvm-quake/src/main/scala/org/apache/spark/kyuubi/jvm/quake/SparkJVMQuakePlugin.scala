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

package org.apache.spark.kyuubi.jvm.quake

import java.util.{Collections, Map => JMap}

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.kyuubi.jvm.quake.SparkJVMQuakeConf._

class SparkJVMQuakePlugin extends SparkPlugin with Logging {

  override def driverPlugin(): DriverPlugin = {
    new DriverPlugin() {
      override def init(sc: SparkContext, pluginContext: PluginContext): JMap[String, String] = {
        val jvmQuakeEnabled = sc.conf.get(DRIVER_JVM_QUAKE_ENABLED)
        val jvmQuakeHeapDumpEnabled = sc.conf.get(DRIVER_JVM_QUAKE_HEAP_DUMP_ENABLED)
        if (jvmQuakeEnabled) {
          SparkJVMQuake.start(sc.conf, jvmQuakeHeapDumpEnabled)
        }
        Collections.emptyMap()
      }

      override def shutdown(): Unit = {
        SparkJVMQuake.stop()
      }
    }
  }

  override def executorPlugin(): ExecutorPlugin = {
    new ExecutorPlugin {
      override def init(context: PluginContext, extraConf: JMap[String, String]): Unit = {
        val jvmQuakeEnabled = context.conf().get(EXECUTOR_JVM_QUAKE_ENABLED)
        val jvmQuakeHeapDumpEnabled = context.conf().get(EXECUTOR_JVM_QUAKE_HEAP_DUMP_ENABLED)
        if (jvmQuakeEnabled) {
          SparkJVMQuake.start(context.conf(), jvmQuakeHeapDumpEnabled)
        }
      }

      override def shutdown(): Unit = {
        SparkJVMQuake.stop()
      }
    }
  }
}
