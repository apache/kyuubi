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

package org.apache.kyuubi.server

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.ServiceState._

class KyuubiServerSuite extends KyuubiFunSuite {

  test("kyuubi server basic") {
    val server = new KyuubiServer()
    server.stop()
    val conf = KyuubiConf().set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    assert(server.getServices.isEmpty)
    assert(server.getServiceState === LATENT)
    val e = intercept[IllegalStateException](server.connectionUrl)
    assert(e.getMessage === "Illegal Service State: LATENT")
    assert(server.getConf === null)

    server.initialize(conf)
    assert(server.getServiceState === INITIALIZED)
    val backendServices = server.getServices.filter(_.isInstanceOf[KyuubiBackendService])
    assert(backendServices.size == 1)
    val backendService = backendServices(0).asInstanceOf[KyuubiBackendService]
    assert(backendService.getServiceState == INITIALIZED)
    assert(backendService.getServices.forall(_.getServiceState === INITIALIZED))
    assert(server.connectionUrl.split(":").length === 2)
    assert(server.getConf === conf)
    assert(server.getStartTime === 0)
    server.stop()


    server.start()
    assert(server.getServiceState === STARTED)
    assert(backendService.getServiceState == STARTED)
    assert(backendService.getServices.forall(_.getServiceState === STARTED))
    assert(server.getStartTime !== 0)

    server.stop()
    assert(server.getServiceState === STOPPED)
    assert(backendService.getServiceState == STOPPED)
    assert(backendService.getServices.forall(_.getServiceState === STOPPED))
    server.stop()
  }

  test("invalid port") {
    val conf = KyuubiConf().set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 100)
    val e = intercept[KyuubiException](new KyuubiServer().initialize(conf))
    assert(e.getMessage.contains("Failed to initialize frontend service"))
    assert(e.getCause.getMessage === "Invalid Port number")
  }
}
