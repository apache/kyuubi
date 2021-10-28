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

import java.net.URI
import java.util.Locale
import javax.ws.rs.client.WebTarget
import javax.ws.rs.core.{Application, UriBuilder}

import org.glassfish.jersey.client._
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.test.JerseyTest
import org.glassfish.jersey.test.jetty.JettyTestContainerFactory
import org.glassfish.jersey.test.spi.TestContainerFactory
import org.scalatest.time.SpanSugar._
import scala.io.Source

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.RestFrontendServiceSuite.{withKyuubiRestServer, TEST_SERVER_PORT}
import org.apache.kyuubi.server.api.KyuubiScalaObjectMapper
import org.apache.kyuubi.service.NoopServer
import org.apache.kyuubi.service.ServiceState._

class RestFrontendServiceSuite extends KyuubiFunSuite {

  test("kyuubi rest frontend service basic") {
    val server = new RestFrontendServiceSuite.RestNoopServer()
    server.stop()
    val conf = KyuubiConf()
    assert(server.getServices.isEmpty)
    assert(server.getServiceState === LATENT)
    val e = intercept[IllegalStateException](server.frontendServices.head.connectionUrl)
    assert(e.getMessage startsWith "Illegal Service State: LATENT")
    assert(server.getConf === null)

    server.initialize(conf)
    assert(server.getServiceState === INITIALIZED)
    val frontendService = server.frontendServices.head
    assert(frontendService.getServiceState == INITIALIZED)
    assert(server.frontendServices.head.connectionUrl.split(":").length === 2)
    assert(server.getConf === conf)
    assert(server.getStartTime === 0)
    server.stop()

    server.start()
    assert(server.getServiceState === STARTED)
    assert(frontendService.getServiceState == STARTED)
    assert(server.getStartTime !== 0)

    server.stop()
    assert(server.getServiceState === STOPPED)
    assert(frontendService.getServiceState == STOPPED)
    server.stop()
  }

  test("kyuubi rest frontend service http basic") {
    RestFrontendServiceSuite.withKyuubiRestServer {
      (_, host, port, _) =>
        eventually(timeout(10.seconds), interval(50.milliseconds)) {
          val html = Source.fromURL(s"http://$host:$port/api/v1/ping").mkString
          assert(html.toLowerCase(Locale.ROOT).equals("pong"))
        }
    }
  }
}

object RestFrontendServiceSuite {

  class RestNoopServer extends NoopServer {
    override val frontendServices = Seq(new RestFrontendService(this))
  }

  val TEST_SERVER_PORT = KyuubiConf().get(KyuubiConf.FRONTEND_REST_BIND_PORT)

  def withKyuubiRestServer(f: (
    RestFrontendService, String, Int, WebTarget) => Unit): Unit = {

    val server = new RestNoopServer()
    server.stop()
    val conf = KyuubiConf()
    conf.set(KyuubiConf.FRONTEND_REST_BIND_HOST, "localhost")

    server.initialize(conf)
    server.start()

    val restApiBaseSuite = new RestApiBaseSuite
    restApiBaseSuite.setUp()
    val webTarget = restApiBaseSuite.client.target(restApiBaseSuite.getBaseUri())

    try {
      f(server.frontendServices.head, conf.get(KyuubiConf.FRONTEND_REST_BIND_HOST).get,
        TEST_SERVER_PORT, webTarget)
    } finally {
      restApiBaseSuite.tearDown()
      server.stop()
    }
  }

}

class RestApiBaseSuite extends JerseyTest {

  override def configure: Application = {
    new ResourceConfig(getClass)
  }

  override def configureClient(config: ClientConfig): Unit = {
    super.configureClient(config)
    config.register(classOf[KyuubiScalaObjectMapper])
  }

  override def getBaseUri: URI = {
    UriBuilder.fromUri("http://localhost/")
      .port(TEST_SERVER_PORT).build(new Array[AnyRef](0))
  }

  override def getTestContainerFactory: TestContainerFactory = new JettyTestContainerFactory

}

class RestErrorAndExceptionSuite extends KyuubiFunSuite {

  test("test error and exception response") {
    withKyuubiRestServer {
      (_, _, _, webTarget) =>

        // send a not exists request
        var response = webTarget.path("api/v1/pong").request().get()
        assert(404 == response.getStatus)
        assert(response.getStatusInfo.getReasonPhrase.equalsIgnoreCase("not found"))

        // send a exists request but wrong http method
        response = webTarget.path("api/v1/ping").request().post(null)
        assert(405 == response.getStatus)
        assert(response.getStatusInfo.getReasonPhrase.equalsIgnoreCase("method not allowed"))

        // send a request but throws a exception on the server side
        response = webTarget.path("api/v1/exception").request().get()
        assert(500 == response.getStatus)
        assert(response.getStatusInfo.getReasonPhrase.equalsIgnoreCase("server error"))
    }
  }
}
