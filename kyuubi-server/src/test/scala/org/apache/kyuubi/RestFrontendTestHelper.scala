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

import java.net.URI
import javax.ws.rs.client.WebTarget
import javax.ws.rs.core.{Application, Response, UriBuilder}

import org.glassfish.jersey.client.ClientConfig
import org.glassfish.jersey.media.multipart.MultiPartFeature
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.test.JerseyTest
import org.glassfish.jersey.test.jetty.JettyTestContainerFactory
import org.glassfish.jersey.test.spi.TestContainerFactory

import org.apache.kyuubi.RestFrontendTestHelper.RestApiBaseSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols.FrontendProtocol
import org.apache.kyuubi.server.api.KyuubiScalaObjectMapper
import org.apache.kyuubi.service.AbstractFrontendService

object RestFrontendTestHelper {

  class RestApiBaseSuite extends JerseyTest {

    override def configure: Application = new ResourceConfig(getClass)
      .register(classOf[MultiPartFeature])

    override def configureClient(config: ClientConfig): Unit = {
      config.register(classOf[KyuubiScalaObjectMapper])
        .register(classOf[MultiPartFeature])
    }

    override def getTestContainerFactory: TestContainerFactory = new JettyTestContainerFactory
  }
}

trait RestFrontendTestHelper extends WithKyuubiServer {

  override protected lazy val conf: KyuubiConf = KyuubiConf()

  override protected val frontendProtocols: Seq[FrontendProtocol] =
    FrontendProtocols.REST :: Nil

  protected val restApiBaseSuite: JerseyTest = new RestApiBaseSuite

  override def beforeAll(): Unit = {
    super.beforeAll()
    restApiBaseSuite.setUp()
  }

  override def afterAll(): Unit = {
    restApiBaseSuite.tearDown()
    super.afterAll()
  }

  protected lazy val fe: AbstractFrontendService = server.frontendServices.head

  protected lazy val baseUri: URI = UriBuilder.fromUri(s"http://${fe.connectionUrl}/").build()

  protected lazy val webTarget: WebTarget = restApiBaseSuite.client.target(baseUri)

  protected def v1Call(func: String): Response = {
    webTarget.path("api/v1/" + func).request().get()
  }
}
