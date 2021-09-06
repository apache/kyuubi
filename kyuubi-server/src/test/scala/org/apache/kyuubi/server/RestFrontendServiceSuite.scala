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

import java.util.Locale

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.time.SpanSugar._
import scala.io.Source

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.api.v1.dto.SessionOpenedCount
import org.apache.kyuubi.service.{NoopBackendService, NoopServer}
import org.apache.kyuubi.service.ServiceState._
import org.apache.kyuubi.session.{NoopSessionImpl, NoopSessionManager, SessionManager}

class RestFrontendServiceSuite extends KyuubiFunSuite{

  test("kyuubi rest frontend service basic") {
    val server = new RestNoopServer()
    server.stop()
    val conf = KyuubiConf()
    assert(server.getServices.isEmpty)
    assert(server.getServiceState === LATENT)
    val e = intercept[IllegalStateException](server.connectionUrl)
    assert(e.getMessage === "Illegal Service State: LATENT")
    assert(server.getConf === null)

    server.initialize(conf)
    assert(server.getServiceState === INITIALIZED)
    val frontendService = server.getServices(0).asInstanceOf[RestFrontendService]
    assert(frontendService.getServiceState == INITIALIZED)
    assert(server.connectionUrl.split(":").length === 2)
    assert(server.getConf === conf)
    assert(server.getStartTime === 0)
    server.stop()

    server.start()
    assert(server.getServiceState === STARTED)
    assert(frontendService.getServiceState == STARTED)
    assert(server.getStartTime !== 0)
    logger.info(frontendService.connectionUrl(false))

    server.stop()
    assert(server.getServiceState === STOPPED)
    assert(frontendService.getServiceState == STOPPED)
    server.stop()
  }

  test("kyuubi rest frontend service http basic") {
    withKyuubiRestServer {
      (_, _) =>
        eventually(timeout(10.seconds), interval(50.milliseconds)) {
          val html = Source.fromURL("http://localhost:10099/api/v1/ping").mkString
          assert(html.toLowerCase(Locale.ROOT).equals("pong"))
        }
    }
  }

  test("kyuubi rest frontend service for sessions resource") {
    withKyuubiRestServer {
      (_, _) =>
        val expectedCount = new SessionOpenedCount()
        expectedCount.setOpenSessionCount(1)
        val expectedStr = new ObjectMapper().writeValueAsString(expectedCount)

        eventually(timeout(10.seconds), interval(50.milliseconds)) {
          val html = Source.fromURL("http://localhost:10099/api/v1/sessions/count").mkString
          assert(html.toLowerCase(Locale.ROOT).equalsIgnoreCase(expectedStr))
        }
    }
  }

  def withKyuubiRestServer(f: (RestNoopServer, RestFrontendService) => Unit): Unit = {
    val server = new RestNoopServer()
    server.stop()
    val conf = KyuubiConf()
    conf.set(KyuubiConf.FRONTEND_REST_BIND_HOST, "localhost")

    server.initialize(conf)
    server.start()

    val frontendService = server.getServices(0).asInstanceOf[RestFrontendService]

    try {
      f(server, frontendService)
    } finally {
      server.stop()
    }
  }

  class RestNoopServer extends NoopServer {
    override val backendService: NoopBackendService = new RestMockedBeService
    override val frontendService = new RestFrontendService(backendService)
  }

  class RestMockedBeService extends NoopBackendService {
    override val sessionManager: SessionManager = new RestMockedSessionManager()
  }

  class RestMockedSessionManager extends NoopSessionManager {
    // It's a ugly and temporally implementation will replace it via creation rest API.
    var session = new NoopSessionImpl(null, null, null, null, Map(), this)
    setSession(session.handle, session)

    override protected def isServer: Boolean = true
  }

}
