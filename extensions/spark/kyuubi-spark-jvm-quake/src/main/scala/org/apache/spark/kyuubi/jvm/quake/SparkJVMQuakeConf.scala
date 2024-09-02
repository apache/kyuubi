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

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.ConfigBuilder

object SparkJVMQuakeConf {

  val DRIVER_JVM_QUAKE_ENABLED =
    ConfigBuilder("spark.driver.jvmQuake.enabled")
      .doc("Whether to enable JVM quake on the driver.")
      .version("1.10.0")
      .booleanConf
      .createWithDefault(false)

  val EXECUTOR_JVM_QUAKE_ENABLED =
    ConfigBuilder("spark.executor.jvmQuake.enabled")
      .doc("Whether to enable JVM quake on the executor.")
      .version("1.10.0")
      .booleanConf
      .createWithDefault(false)

  val JVM_QUAKE_RUN_TIME_WEIGHT =
    ConfigBuilder("spark.jvmQuake.runTimeWeight")
      .doc("Weight of run time, This value determines the rate of change of JVM Quake value, " +
        "and the larger this value, the smaller the rate of change of JVM Quake")
      .version("1.10.0")
      .doubleConf
      .createWithDefault(1.0)

  val JVM_QUAKE_HEAP_DUMP_PATH =
    ConfigBuilder("spark.jvmQuake.heapDumpPath")
      .doc("The local path of heap dump, If the directory does not exist, " +
        "it will be automatically created, but corresponding permissions are required")
      .version("1.10.0")
      .stringConf
      .createWithDefault("/tmp/spark_jvm_quake/apps")

  val JVM_QUAKE_KILL_THRESHOLD =
    ConfigBuilder("spark.jvmQuake.killThreshold")
      .doc(s"JVM Quake value is cumulative value of " +
        s"gcTime - runTime * ${JVM_QUAKE_RUN_TIME_WEIGHT.key}, " +
        s"When the JVM Quake reaches this threshold, kill process and dump heap if dump is enabled")
      .version("1.10.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(200)

  val DRIVER_JVM_QUAKE_HEAP_DUMP_ENABLED =
    ConfigBuilder("spark.driver.jvmQuake.heapDump.enabled")
      .doc(s"When true, When the driver JVM Quake reaches the ${JVM_QUAKE_KILL_THRESHOLD.key} " +
        s"threshold, dump the memory to ${JVM_QUAKE_HEAP_DUMP_PATH.key}")
      .version("1.10.0")
      .booleanConf
      .createWithDefault(false)

  val EXECUTOR_JVM_QUAKE_HEAP_DUMP_ENABLED =
    ConfigBuilder("spark.executor.jvmQuake.heapDump.enabled")
      .doc(s"When true, When the executor JVM Quake reaches the ${JVM_QUAKE_KILL_THRESHOLD.key} " +
        s"threshold, dump the memory to ${JVM_QUAKE_HEAP_DUMP_PATH.key}")
      .version("1.10.0")
      .booleanConf
      .createWithDefault(false)

  val JVM_QUAKE_CHECK_INTERVAL =
    ConfigBuilder("spark.jvmQuake.checkInterval")
      .doc("How often will JVM Quake check")
      .version("1.10.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefault(3)

  val JVM_QUAKE_EXIT_CODE =
    ConfigBuilder("spark.jvmQuake.exitCode")
      .doc("Exit code for JVM Quake kill")
      .version("1.10.0")
      .intConf
      .createWithDefault(138)
}
