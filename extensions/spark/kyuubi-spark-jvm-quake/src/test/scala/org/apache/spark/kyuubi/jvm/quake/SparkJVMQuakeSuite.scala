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

import java.io.File
import java.util.UUID

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.kyuubi.jvm.quake.SparkJVMQuakeConf._
import org.scalatest.BeforeAndAfterEach

class SparkJVMQuakeSuite extends SparkFunSuite with BeforeAndAfterEach with Logging {

  test("check JVM Quake status") {
    val conf = new SparkConf
    conf.set("spark.app.id", UUID.randomUUID().toString)
    conf.set(JVM_QUAKE_CHECK_INTERVAL.key, "1")
    conf.set(JVM_QUAKE_HEAP_DUMP_PATH.key, "./extensions/spark/kyuubi-spark-jvm-quake/target")
    var monitor = new SparkJVMQuake(conf, false)
    assert(!new File(monitor.heapPath, monitor.heapDumpFileName).exists())

    monitor = new SparkJVMQuake(conf, true)
    monitor.saveHeap()
    assert(new File(monitor.heapPath, monitor.heapDumpFileName).exists())
  }

}
