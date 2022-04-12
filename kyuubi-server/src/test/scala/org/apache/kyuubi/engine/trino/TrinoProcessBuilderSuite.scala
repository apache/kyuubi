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

class TrinoProcessBuilderSuite extends KyuubiFunSuite {

  test("trino process builder") {
    val conf = KyuubiConf()
      .set(ENGINE_TRINO_CONNECTION_URL, "dummy_url")
      .set(ENGINE_TRINO_CONNECTION_CATALOG, "dumm_catalog")
    val builder = new TrinoProcessBuilder("kyuubi", conf)
    val commands = builder.toString.split("\n")
    assert(commands.head.endsWith("java"))
    assert(commands.contains(s"-D${ENGINE_TRINO_CONNECTION_URL.key}=dummy_url"))
  }

  test("capture error from trino process builder") {
    val e1 = intercept[IllegalArgumentException](
      new TrinoProcessBuilder("kyuubi", KyuubiConf()).processBuilder)
    assert(e1.getMessage contains
      s"Trino server url can not be null! Please set ${ENGINE_TRINO_CONNECTION_URL.key}")
  }
}
