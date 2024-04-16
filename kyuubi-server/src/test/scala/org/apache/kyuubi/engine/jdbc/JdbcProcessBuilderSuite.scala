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
package org.apache.kyuubi.engine.jdbc

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_JDBC_CONNECTION_PASSWORD, ENGINE_JDBC_CONNECTION_URL, ENGINE_JDBC_EXTRA_CLASSPATH, ENGINE_JDBC_JAVA_OPTIONS, ENGINE_JDBC_MEMORY}

class JdbcProcessBuilderSuite extends KyuubiFunSuite {

  test("jdbc process builder") {
    val conf = KyuubiConf().set("kyuubi.on", "off")
      .set(ENGINE_JDBC_CONNECTION_URL.key, "")
      .set(ENGINE_JDBC_CONNECTION_PASSWORD.key, "123456")
    val builder = new JdbcProcessBuilder("kyuubi", true, conf)
    val command = builder.toString
    assert(command.contains("bin/java"), "wrong exec")
    assert(command.contains("--conf kyuubi.session.user=kyuubi"))
    assert(command.contains("kyuubi-jdbc-engine"), "wrong classpath")
    assert(command.contains("--conf kyuubi.on=off"))
    assert(command.contains(
      "--conf kyuubi.engine.jdbc.connection.password=*********(redacted)"))
  }

  test("capture error from jdbc process builder") {
    val e1 = intercept[IllegalArgumentException](
      new JdbcProcessBuilder("kyuubi", true, KyuubiConf()).processBuilder)
    assert(e1.getMessage contains
      s"Jdbc server url can not be null! Please set ${ENGINE_JDBC_CONNECTION_URL.key}")
  }

  test("default engine memory") {
    val conf = KyuubiConf()
      .set(ENGINE_JDBC_CONNECTION_URL.key, "")
    val builder = new JdbcProcessBuilder("kyuubi", true, conf)
    val command = builder.toString
    assert(command.contains("-Xmx1g"))
  }

  test("set engine memory") {
    val conf = KyuubiConf()
      .set(ENGINE_JDBC_MEMORY, "5g")
      .set(ENGINE_JDBC_CONNECTION_URL.key, "")
    val builder = new JdbcProcessBuilder("kyuubi", true, conf)
    val command = builder.toString
    assert(command.contains("-Xmx5g"))
  }

  test("set engine java options") {
    val conf = KyuubiConf()
      .set(ENGINE_JDBC_CONNECTION_URL.key, "")
      .set(
        ENGINE_JDBC_JAVA_OPTIONS,
        "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005")
    val builder = new JdbcProcessBuilder("kyuubi", true, conf)
    val command = builder.toString
    assert(command.contains("-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"))
  }

  test("set extra classpath") {
    val conf = KyuubiConf()
      .set(ENGINE_JDBC_CONNECTION_URL.key, "")
      .set(ENGINE_JDBC_EXTRA_CLASSPATH, "/dummy_classpath/*")
    val builder = new JdbcProcessBuilder("kyuubi", true, conf)
    val command = builder.toString
    assert(command.contains("/dummy_classpath/*"))
  }
}
