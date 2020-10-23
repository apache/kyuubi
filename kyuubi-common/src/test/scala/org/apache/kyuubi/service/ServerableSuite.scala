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

  test("Serverable") {
    val serverable = new NoopServer()
    serverable.stop()
    assert(serverable.getStartTime === 0)
    assert(serverable.getConf === null)
    assert(serverable.getName === "noop")
    intercept[IllegalStateException](serverable.connectionUrl)
    assert(serverable.getServiceState === ServiceState.LATENT)
    intercept[IllegalStateException](serverable.start())

    val conf = KyuubiConf().set(KyuubiConf.FRONTEND_BIND_PORT, 0)
    serverable.initialize(conf)
    serverable.stop()
    assert(serverable.getStartTime === 0)
    assert(serverable.getConf === conf)
    assert(serverable.connectionUrl.nonEmpty)
    assert(serverable.getServiceState === ServiceState.INITIALIZED)
    serverable.start()
    assert(serverable.getStartTime !== 0)
    assert(serverable.getConf === conf)
    assert(serverable.getServiceState === ServiceState.STARTED)
    serverable.stop()
    assert(serverable.getStartTime !== 0)
    assert(serverable.getConf === conf)
    assert(serverable.connectionUrl.nonEmpty)
    assert(serverable.getServiceState === ServiceState.STOPPED)
    serverable.stop()
  }

  test("invalid port") {
    val conf = KyuubiConf().set(KyuubiConf.FRONTEND_BIND_PORT, 100)
    val e = intercept[KyuubiException](new NoopServer().initialize(conf))
    assert(e.getMessage === "Failed to initialize frontend service")
    assert(e.getCause.getMessage === "Invalid Port number")
  }

  test("error start child services") {
    val conf = KyuubiConf()
      .set(KyuubiConf.FRONTEND_BIND_PORT, 0)
      .set("kyuubi.test.should.fail", "true")
    val server = new NoopServer()
    server.initialize(conf)
    val e = intercept[IllegalArgumentException](server.start())
    assert(e.getMessage === "should fail")
  }
}
