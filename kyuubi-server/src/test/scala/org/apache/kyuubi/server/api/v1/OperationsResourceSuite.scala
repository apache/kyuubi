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
import javax.ws.rs.core.{MediaType, Response}

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.operation.{OperationHandle, OperationState, OperationType}
import org.apache.kyuubi.session.SessionHandle

class OperationsResourceSuite extends KyuubiFunSuite with RestFrontendTestHelper {

  test("test get an operation detail by identifier") {
    val requestObj = SessionOpenRequest(
      1,
      "admin",
      "123456",
      "localhost",
      Map("testConfig" -> "testValue"))

    withKyuubiRestServer { (_, _, _, webTarget) =>
      var response: Response = webTarget.path("api/v1/sessions")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))

      val sessionHandle = response.readEntity(classOf[SessionHandle])
      val serializedSessionHandle = s"${sessionHandle.identifier.publicId}|" +
        s"${sessionHandle.identifier.secretId}|${sessionHandle.protocol.getValue}"

      response = webTarget.path(s"api/v1/sessions/$serializedSessionHandle/operations/catalogs")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE))
      assert(200 == response.getStatus)
      var operationHandle = response.readEntity(classOf[OperationHandle])
      assert(operationHandle.typ == OperationType.GET_CATALOGS)

      val serializedOperationHandle = s"${operationHandle.identifier.publicId}|" +
        s"${operationHandle.identifier.secretId}|${operationHandle.protocol.getValue}|" +
        s"${operationHandle.typ.toString}"

      response = webTarget.path(s"api/v1/operations/$serializedOperationHandle")
        .request(MediaType.APPLICATION_JSON_TYPE).get()
      val operationDetail = response.readEntity(classOf[OperationDetail])
      assert(200 == response.getStatus)
      assert(operationDetail.operationStatus.state == OperationState.FINISHED)

      // Invalid operationHandleStr
      val invalidOperationHandle = s"${operationHandle.identifier.publicId}|" +
        s"${operationHandle.identifier.secretId}|${operationHandle.protocol.getValue}|GET_TYPE_INFO"
      response = webTarget.path(s"api/v1/operations/$invalidOperationHandle")
        .request(MediaType.APPLICATION_JSON_TYPE).get()
      assert(404 == response.getStatus)

    }
  }
}
