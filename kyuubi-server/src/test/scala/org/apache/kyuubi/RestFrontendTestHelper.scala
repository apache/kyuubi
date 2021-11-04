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

package org.apache.kyuubi

import javax.ws.rs.client.WebTarget
import javax.ws.rs.core.{Application, UriBuilder}

import org.glassfish.jersey.client.ClientConfig
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.test.JerseyTest
import org.glassfish.jersey.test.jetty.JettyTestContainerFactory
import org.glassfish.jersey.test.spi.TestContainerFactory

import org.apache.kyuubi.RestFrontendTestHelper.RestApiBaseSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.KyuubiRestFrontendService
import org.apache.kyuubi.server.api.KyuubiScalaObjectMapper
import org.apache.kyuubi.service.NoopRestFrontendServer

object RestFrontendTestHelper {

  private class RestApiBaseSuite extends JerseyTest {

    override def configure: Application = new ResourceConfig(getClass)

    override def configureClient(config: ClientConfig): Unit = {
      config.register(classOf[KyuubiScalaObjectMapper])
    }

    override def getTestContainerFactory: TestContainerFactory = new JettyTestContainerFactory
  }
}

trait RestFrontendTestHelper {

  val restFrontendHost: String = "localhost"
  val restFrontendPort: Int = KyuubiConf().get(KyuubiConf.FRONTEND_REST_BIND_PORT)

  def withKyuubiRestServer(
    f: (KyuubiRestFrontendService, String, Int, WebTarget) => Unit): Unit = {

    val server = new NoopRestFrontendServer()
    server.stop()
    val conf = KyuubiConf()
    conf.set(KyuubiConf.FRONTEND_REST_BIND_HOST, restFrontendHost)

    server.initialize(conf)
    server.start()

    val restApiBaseSuite = new RestApiBaseSuite
    restApiBaseSuite.setUp()
    // noinspection HttpUrlsUsage
    val baseUri = UriBuilder
      .fromUri(s"http://$restFrontendHost/")
      .port(restFrontendPort)
      .build()
    val webTarget = restApiBaseSuite.client.target(baseUri)

    try {
      f(server.frontendServices.head,
        conf.get(KyuubiConf.FRONTEND_REST_BIND_HOST).get,
        restFrontendPort,
        webTarget)
    } finally {
      restApiBaseSuite.tearDown()
      server.stop()
    }
  }
}
