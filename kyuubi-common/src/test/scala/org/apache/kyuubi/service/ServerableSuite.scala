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

package org.apache.kyuubi.service

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf

class ServerableSuite extends KyuubiFunSuite {

  ignore("Serverable") {
    val serverable1 = new NoopServer()
    val conf = KyuubiConf().set(KyuubiConf.FRONTEND_BIND_PORT, 0)
    serverable1.initialize(conf)
    assert(serverable1.getStartTime === 0)
    assert(serverable1.getConf === conf)
    assert(serverable1.connectionUrl.nonEmpty)
    assert(serverable1.getServiceState === ServiceState.INITIALIZED)
    serverable1.start()
    assert(serverable1.getStartTime !== 0)
    assert(serverable1.getConf === conf)
    assert(serverable1.getServiceState === ServiceState.STARTED)
    serverable1.stop()
    assert(serverable1.getStartTime !== 0)
    assert(serverable1.getConf === conf)
    assert(serverable1.connectionUrl.nonEmpty)
    assert(serverable1.getServiceState === ServiceState.STOPPED)
    serverable1.stop()
  }

  test("invalid port") {
    val conf = KyuubiConf().set(KyuubiConf.FRONTEND_BIND_PORT, 100)
    val e = intercept[KyuubiException](new NoopServer().initialize(conf))
    assert(e.getMessage.contains("Failed to initialize frontend service"))
    assert(e.getCause.getMessage === "Invalid Port number")
  }

  test("error start child services") {
    val conf = KyuubiConf()
      .set(KyuubiConf.FRONTEND_BIND_PORT, 0)
      .set("kyuubi.test.server.should.fail", "true")
    val server = new NoopServer()
    server.initialize(conf)
    val e = intercept[IllegalArgumentException](server.start())
    assert(e.getMessage === "should fail")

    conf
      .set("kyuubi.test.server.should.fail", "false")
      .set("kyuubi.test.backend.should.fail", "true")
    val server1 = new NoopServer()
    server1.initialize(conf)
    val e1 = intercept[KyuubiException](server1.start())
    assert(e1.getMessage === "Failed to Start noop")
    assert(e1.getCause.getMessage === "should fail backend")
  }
}
