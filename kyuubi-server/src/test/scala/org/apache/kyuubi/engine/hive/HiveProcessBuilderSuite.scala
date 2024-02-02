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

package org.apache.kyuubi.engine.hive

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_HIVE_EXTRA_CLASSPATH, ENGINE_HIVE_JAVA_OPTIONS, ENGINE_HIVE_MEMORY}
import org.apache.kyuubi.engine.hive.HiveProcessBuilder._

class HiveProcessBuilderSuite extends KyuubiFunSuite {

  test("hive process builder") {
    val conf = KyuubiConf().set("kyuubi.on", "off")
    val builder = new HiveProcessBuilder("kyuubi", true, conf) {
      override def env: Map[String, String] = super.env + (HIVE_HADOOP_CLASSPATH_KEY -> "/hadoop")
    }
    val commands = builder.toString.split('\n')
    assert(commands.head.contains("bin/java"), "wrong exec")
    assert(builder.toString.contains("--conf kyuubi.session.user=kyuubi"))
    assert(commands.exists(ss => ss.contains("kyuubi-hive-sql-engine")), "wrong classpath")
    assert(builder.toString.contains("--conf kyuubi.on=off"))
  }

  test("default engine memory") {
    val conf = KyuubiConf()
      .set(ENGINE_HIVE_EXTRA_CLASSPATH, "/hadoop")
    val builder = new HiveProcessBuilder("kyuubi", true, conf)
    val command = builder.toString
    assert(command.contains("-Xmx1g"))
  }

  test("set engine memory") {
    val conf = KyuubiConf()
      .set(ENGINE_HIVE_MEMORY, "5g")
      .set(ENGINE_HIVE_EXTRA_CLASSPATH, "/hadoop")
    val builder = new HiveProcessBuilder("kyuubi", true, conf)
    val command = builder.toString
    assert(command.contains("-Xmx5g"))
  }

  test("set engine java opts") {
    val conf = KyuubiConf()
      .set(ENGINE_HIVE_EXTRA_CLASSPATH, "/hadoop")
      .set(
        ENGINE_HIVE_JAVA_OPTIONS,
        "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005")
    val builder = new HiveProcessBuilder("kyuubi", true, conf)
    val command = builder.toString
    assert(command.contains("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"))
  }

  test("set engine extra classpath") {
    val conf = KyuubiConf().set(ENGINE_HIVE_EXTRA_CLASSPATH, "/dummy_classpath/*")
    val builder = new HiveProcessBuilder("kyuubi", true, conf)
    val commands = builder.toString
    assert(commands.contains("/dummy_classpath/*"))
  }
}
