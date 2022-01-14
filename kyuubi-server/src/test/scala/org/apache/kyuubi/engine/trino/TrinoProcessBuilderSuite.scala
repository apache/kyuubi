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
import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_TRINO_CONNECTION_URL

class TrinoProcessBuilderSuite extends KyuubiFunSuite {
  private def conf = KyuubiConf().set("kyuubi.on", "off")

  test("trino process builder") {
    val builder = new TrinoProcessBuilder("kyuubi", conf)
    val commands = builder.toString.split(' ')
    assert(commands.exists(_.endsWith("trino-engine.sh")))
  }

  test("capture error from trino process builder") {
    val e1 = intercept[KyuubiSQLException](new TrinoProcessBuilder("kyuubi", conf).trinoConf)
    assert(e1.getMessage ===
      s"Trino server url can not be null! Please set ${ENGINE_TRINO_CONNECTION_URL.key}")
  }
}
