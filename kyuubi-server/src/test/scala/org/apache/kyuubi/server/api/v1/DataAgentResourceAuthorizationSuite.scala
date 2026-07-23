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

package org.apache.kyuubi.server.api.v1

import java.util.UUID
import javax.ws.rs.core.MediaType

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{AUTHENTICATION_CUSTOM_CLASS, AUTHENTICATION_METHOD, SERVER_ADMINISTRATORS}
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_NAMESPACE
import org.apache.kyuubi.ha.client.{DataAgentSessionRoute, DiscoveryClientProvider}
import org.apache.kyuubi.server.http.util.HttpAuthUtils.{basicAuthorizationHeader, AUTHORIZATION_HEADER}
import org.apache.kyuubi.service.authentication.AnonymousAuthenticationProviderImpl

class DataAgentResourceAuthorizationSuite extends KyuubiFunSuite with RestFrontendTestHelper {

  override protected lazy val conf: KyuubiConf =
    KyuubiConf()
      .set(AUTHENTICATION_METHOD, Seq("CUSTOM"))
      .set(AUTHENTICATION_CUSTOM_CLASS, classOf[AnonymousAuthenticationProviderImpl].getName)
      .set(SERVER_ADMINISTRATORS, Set("admin"))

  test("a user cannot resolve or remove another user's route") {
    val sessionId = UUID.randomUUID().toString
    val route = DataAgentSessionRoute("missing-engine-space", "missing-engine-ref", "owner")
    DiscoveryClientProvider.withDiscoveryClient(conf) { discovery =>
      DataAgentSessionRoute.register(discovery, conf.get(HA_NAMESPACE), sessionId, route)
    }

    try {
      val response = webTarget.path(s"api/v1/data-agent/sessions/$sessionId")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .header(AUTHORIZATION_HEADER, basicAuthorizationHeader("another-user"))
        .get()
      assert(response.getStatus === 404)

      val persisted = DiscoveryClientProvider.withDiscoveryClient(conf) { discovery =>
        DataAgentSessionRoute.find(discovery, conf.get(HA_NAMESPACE), sessionId)
      }
      assert(persisted.contains(route))
    } finally {
      DiscoveryClientProvider.withDiscoveryClient(conf) { discovery =>
        DataAgentSessionRoute.unregister(discovery, conf.get(HA_NAMESPACE), sessionId)
      }
    }
  }
}
