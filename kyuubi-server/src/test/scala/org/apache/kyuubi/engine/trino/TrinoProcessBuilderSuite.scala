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

package org.apache.kyuubi.engine.trino

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY

class TrinoProcessBuilderSuite extends KyuubiFunSuite {

  test("trino process builder") {
    val conf = KyuubiConf()
      .set(ENGINE_TRINO_CONNECTION_URL, "dummy_url")
      .set(ENGINE_TRINO_CONNECTION_CATALOG, "dummy_catalog")
    val builder = new TrinoProcessBuilder("kyuubi", true, conf)
    val commands = builder.toString.split("\n")
    assert(commands.head.contains("java"))
    assert(builder.toString.contains(s"--conf ${KYUUBI_SESSION_USER_KEY}=kyuubi"))
    assert(builder.toString.contains(s"--conf ${ENGINE_TRINO_CONNECTION_URL.key}=dummy_url"))
    assert(builder.toString.contains(
      s"--conf ${ENGINE_TRINO_CONNECTION_CATALOG.key}=dummy_catalog"))
  }

  test("capture error from trino process builder") {
    val e1 = intercept[IllegalArgumentException](
      new TrinoProcessBuilder("kyuubi", true, KyuubiConf()).processBuilder)
    assert(e1.getMessage contains
      s"Trino server url can not be null! Please set ${ENGINE_TRINO_CONNECTION_URL.key}")
  }

  test("default engine memory") {
    val conf = KyuubiConf()
      .set(ENGINE_TRINO_CONNECTION_URL, "dummy_url")
      .set(ENGINE_TRINO_CONNECTION_CATALOG, "dummy_catalog")
    val builder = new TrinoProcessBuilder("kyuubi", true, conf)
    val command = builder.toString
    assert(command.contains("-Xmx1g"))
  }

  test("set engine memory") {
    val conf = KyuubiConf()
      .set(ENGINE_TRINO_CONNECTION_URL, "dummy_url")
      .set(ENGINE_TRINO_CONNECTION_CATALOG, "dummy_catalog")
      .set(ENGINE_TRINO_MEMORY, "5g")
    val builder = new TrinoProcessBuilder("kyuubi", true, conf)
    val command = builder.toString
    assert(command.contains("-Xmx5g"))
  }

  test("set engine java options") {
    val conf = KyuubiConf()
      .set(ENGINE_TRINO_CONNECTION_URL, "dummy_url")
      .set(ENGINE_TRINO_CONNECTION_CATALOG, "dummy_catalog")
      .set(
        ENGINE_TRINO_JAVA_OPTIONS,
        "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005")
    val builder = new TrinoProcessBuilder("kyuubi", true, conf)
    val command = builder.toString
    assert(command.contains("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"))
  }

  test("set extra classpath") {
    val conf = KyuubiConf()
      .set(ENGINE_TRINO_CONNECTION_URL, "dummy_url")
      .set(ENGINE_TRINO_CONNECTION_CATALOG, "dummy_catalog")
      .set(ENGINE_TRINO_EXTRA_CLASSPATH, "/dummy_classpath/*")
    val builder = new TrinoProcessBuilder("kyuubi", true, conf)
    val commands = builder.toString
    assert(commands.contains("/dummy_classpath/*"))
  }
}
