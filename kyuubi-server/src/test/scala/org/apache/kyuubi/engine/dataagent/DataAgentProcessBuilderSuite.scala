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

package org.apache.kyuubi.engine.dataagent

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

class DataAgentProcessBuilderSuite extends KyuubiFunSuite {

  test("toString includes correct main class") {
    val conf = new KyuubiConf(false)
    val builder = new DataAgentProcessBuilder("testUser", doAsEnabled = false, conf)
    val output = builder.toString
    assert(
      output.contains("org.apache.kyuubi.engine.dataagent.DataAgentEngine"),
      s"Expected main class in toString output: $output")
  }

  test("API key is redacted in toString") {
    val conf = new KyuubiConf(false)
    conf.set(ENGINE_DATA_AGENT_LLM_API_KEY.key, "sk-secret-key-12345")
    val builder = new DataAgentProcessBuilder("testUser", doAsEnabled = false, conf)
    val output = builder.toString
    assert(!output.contains("sk-secret-key-12345"), "API key should not appear in toString output")
    assert(
      output.contains("**REDACTED**") || output.contains("********"),
      s"toString should contain a redaction marker, got: $output")
  }

  test("memory flag uses configured value") {
    val conf = new KyuubiConf(false)
    conf.set(ENGINE_DATA_AGENT_MEMORY.key, "2g")
    val builder = new DataAgentProcessBuilder("testUser", doAsEnabled = false, conf)
    val output = builder.toString
    assert(output.contains("-Xmx2g"), s"Expected -Xmx2g in toString: $output")
  }

  test("default memory is 1g") {
    val conf = new KyuubiConf(false)
    val builder = new DataAgentProcessBuilder("testUser", doAsEnabled = false, conf)
    val output = builder.toString
    assert(output.contains("-Xmx1g"), s"Expected -Xmx1g in toString: $output")
  }

  test("extra classpath is included") {
    val conf = new KyuubiConf(false)
    conf.set(ENGINE_DATA_AGENT_EXTRA_CLASSPATH.key, "/extra/path/lib/*")
    val builder = new DataAgentProcessBuilder("testUser", doAsEnabled = false, conf)
    val output = builder.toString
    assert(output.contains("/extra/path/lib/*"), s"Expected extra classpath in toString: $output")
  }

  test("java options are included") {
    val conf = new KyuubiConf(false)
    conf.set(ENGINE_DATA_AGENT_JAVA_OPTIONS.key, "-Dfoo=bar -Dbaz=qux")
    val builder = new DataAgentProcessBuilder("testUser", doAsEnabled = false, conf)
    val output = builder.toString
    assert(output.contains("-Dfoo=bar"), s"Expected -Dfoo=bar in toString: $output")
    assert(output.contains("-Dbaz=qux"), s"Expected -Dbaz=qux in toString: $output")
  }

  test("proxy user is passed in commands") {
    val conf = new KyuubiConf(false)
    val builder = new DataAgentProcessBuilder("myUser", doAsEnabled = false, conf)
    val output = builder.toString
    assert(output.contains("myUser"), s"Expected proxy user in toString: $output")
  }
}
