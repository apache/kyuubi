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

import javax.ws.rs.client.Entity
import javax.ws.rs.core.MediaType

import org.junit.Test

import org.apache.kyuubi.server.RestApiBaseSuite
import org.apache.kyuubi.server.RestFrontendServiceSuite
import org.apache.kyuubi.session.SessionHandle

class SessionsResourceSuite extends RestApiBaseSuite {

  @Test
  def testOpenAndCountSession: Unit = {
    val requestObj = SessionOpenRequest(
      1, "admin", "123456", "localhost", Map("testConfig" -> "testValue"))

    RestFrontendServiceSuite.withKyuubiRestServer {
      (_, _, _) =>
        var response = target(s"api/v1/sessions")
          .request(MediaType.APPLICATION_JSON_TYPE)
          .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))

        assert(200 == response.getStatus)

        val sessionHandle = response.readEntity(classOf[SessionHandle])

        assert(sessionHandle.protocol.getValue == 1)
        assert(sessionHandle.identifier != null)

        // verify the open session count
        response = target("api/v1/sessions/count").request().get()
        val openedSessionCount = response.readEntity(classOf[SessionOpenCount])
        assert(openedSessionCount.openSessionCount == 1)
    }
  }

  @Test
  def testCloseAndCountSession: Unit = {
    val requestObj = SessionOpenRequest(
      1, "admin", "123456", "localhost", Map("testConfig" -> "testValue"))

    RestFrontendServiceSuite.withKyuubiRestServer {
      (_, _, _) =>
        var response = target(s"api/v1/sessions")
          .request(MediaType.APPLICATION_JSON_TYPE)
          .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))

        assert(200 == response.getStatus)

        val sessionHandle = response.readEntity(classOf[SessionHandle])

        assert(sessionHandle.protocol.getValue == 1)
        assert(sessionHandle.identifier != null)

        // close a opened session
        val serializedSessionHandle = s"${sessionHandle.identifier.publicId}|" +
          s"${sessionHandle.identifier.secretId}|${sessionHandle.protocol.getValue}"
        response = target(s"api/v1/sessions/$serializedSessionHandle").request().delete()
        assert(200 == response.getStatus)

        // verify the open session count again
        response = target("api/v1/sessions/count").request().get()
        val openedSessionCount = response.readEntity(classOf[SessionOpenCount])
        assert(openedSessionCount.openSessionCount == 0)
    }
  }

}
