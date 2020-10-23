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

package org.apache.kyuubi.engine.spark

import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

class SparkProcessBuilderSuite extends KyuubiFunSuite {
  private val conf = KyuubiConf()
    .set(EMBEDDED_ZK_PORT, 5555)
    .set(EMBEDDED_ZK_TEMP_DIR, "spark_process_test")
    .set("kyuubi.on", "off")
    .toSparkPrefixedConf

  test("spark process builder") {
    val builder = new SparkProcessBuilder("kentyao", conf)
    val commands = builder.toString.split(' ')
    assert(commands(2) === "org.apache.kyuubi.engine.spark.SparkSQLEngine")
    assert(commands.contains("spark.kyuubi.on=off"))
    val pb = new ProcessBuilder(commands.head, "--help")
    assert(pb.start().waitFor() === 0)
    assert(Files.exists(Paths.get(commands.last)))

    val process = builder.start
    assert(process.isAlive)
    process.destroyForcibly()
  }

  test("capture error from spark process builder") {
    val processBuilder = new SparkProcessBuilder("kentyao", conf ++ Map("spark.ui.port" -> "abc"))
    val proc = processBuilder.start
    proc.waitFor(10, TimeUnit.SECONDS)
    while (proc.isAlive) {
      Thread.sleep(100)
    }
    Thread.sleep(5000)
    val error = processBuilder.getError
    assert(error.getMessage.contains(
      "java.lang.IllegalArgumentException: spark.ui.port should be int, but was abc"))
    assert(error.isInstanceOf[KyuubiSQLException])
  }

}
