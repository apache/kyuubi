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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.service.ServiceState._

class KyuubiFlightSqlFrontendServiceSuite extends KyuubiFunSuite {

  test("Flight SQL frontend lifecycle") {
    val server = new KyuubiServer
    val conf = KyuubiConf()
      .set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.FLIGHT_SQL.toString))
      .set(FRONTEND_FLIGHT_SQL_BIND_HOST.key, "localhost")
      .set(FRONTEND_FLIGHT_SQL_BIND_PORT, 0)

    assert(server.getServiceState === LATENT)
    server.initialize(conf)
    assert(server.getServiceState === INITIALIZED)
    assert(server.frontendServices.size === 1)
    val frontend = server.frontendServices.head
    assert(frontend.getServiceState === INITIALIZED)
    assert(frontend.connectionUrl.startsWith("localhost:"))

    server.start()
    assert(server.getServiceState === STARTED)
    assert(frontend.getServiceState === STARTED)
    assert(frontend.connectionUrl.matches("localhost:[0-9]+"))

    server.stop()
    assert(server.getServiceState === STOPPED)
    assert(frontend.getServiceState === STOPPED)
    server.stop()
  }

  test("Flight SQL advertised host") {
    val server = new KyuubiServer
    val conf = KyuubiConf()
      .set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.FLIGHT_SQL.toString))
      .set(FRONTEND_FLIGHT_SQL_BIND_HOST.key, "localhost")
      .set(FRONTEND_FLIGHT_SQL_BIND_PORT, 0)
      .set(FRONTEND_ADVERTISED_HOST, "flight.example")

    try {
      server.initialize(conf)
      assert(server.frontendServices.head.connectionUrl.startsWith("flight.example:"))
    } finally {
      server.stop()
    }
  }

  test("Flight SQL TLS without certificate material fails startup") {
    val server = new KyuubiServer
    val conf = KyuubiConf()
      .set(FRONTEND_PROTOCOLS, Seq(FrontendProtocols.FLIGHT_SQL.toString))
      .set(FRONTEND_FLIGHT_SQL_BIND_HOST.key, "localhost")
      .set(FRONTEND_FLIGHT_SQL_BIND_PORT, 0)
      .set(FRONTEND_FLIGHT_SQL_SSL_ENABLED, true)

    try {
      server.initialize(conf)
      intercept[Exception] {
        server.start()
      }
    } finally {
      server.stop()
    }
  }
}
