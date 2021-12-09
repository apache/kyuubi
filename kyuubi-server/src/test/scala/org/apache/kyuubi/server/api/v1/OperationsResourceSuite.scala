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

import javax.ws.rs.client.{Entity, WebTarget}
import javax.ws.rs.core.{MediaType, Response}

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.operation.{OperationHandle, OperationState}
import org.apache.kyuubi.session.SessionHandle

class OperationsResourceSuite extends KyuubiFunSuite with RestFrontendTestHelper {

  test("test get an operation detail by identifier") {
    withKyuubiRestServer { (_, _, _, webTarget) =>
      val opHandleStr = getOpHandleStr(webTarget, "catalogs")

      var response = webTarget.path(s"api/v1/operations/$opHandleStr")
        .request(MediaType.APPLICATION_JSON_TYPE).get()
      val operationDetail = response.readEntity(classOf[OperationDetail])
      assert(200 == response.getStatus)
      assert(operationDetail.operationStatus.state == OperationState.FINISHED)

      // Invalid operationHandleStr
      val invalidOperationHandle = opHandleStr.replaceAll("GET_CATALOGS", "GET_TYPE_INFO")
      response = webTarget.path(s"api/v1/operations/$invalidOperationHandle")
        .request(MediaType.APPLICATION_JSON_TYPE).get()
      assert(404 == response.getStatus)

    }
  }

  test("test apply an action for an operation") {
    withKyuubiRestServer { (_, _, _, webTarget: WebTarget) =>
      val opHandleStr = getOpHandleStr(webTarget, "catalogs")

      var response = webTarget.path(s"api/v1/operations/$opHandleStr")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .put(Entity.entity(OpActionRequest("cancel"), MediaType.APPLICATION_JSON_TYPE))
      assert(200 == response.getStatus)

      response = webTarget.path(s"api/v1/operations/$opHandleStr")
        .request(MediaType.APPLICATION_JSON_TYPE).get()
      val operationDetail = response.readEntity(classOf[OperationDetail])
      assert(operationDetail.operationStatus.state == OperationState.FINISHED ||
        operationDetail.operationStatus.state == OperationState.CANCELED)

      response = webTarget.path(s"api/v1/operations/$opHandleStr")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .put(Entity.entity(OpActionRequest("close"), MediaType.APPLICATION_JSON_TYPE))
      assert(200 == response.getStatus)

      response = webTarget.path(s"api/v1/operations/$opHandleStr")
        .request(MediaType.APPLICATION_JSON_TYPE).get()
      assert(404 == response.getStatus)

    }
  }

  def getOpHandleStr(webTarget: WebTarget, operationType: String): String = {
    val requestObj = SessionOpenRequest(
      1,
      "admin",
      "123456",
      "localhost",
      Map("testConfig" -> "testValue"))

    var response: Response = webTarget.path("api/v1/sessions")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))
    val sessionHandle = response.readEntity(classOf[SessionHandle])
    val serializedSessionHandle = s"${sessionHandle.identifier.publicId}|" +
      s"${sessionHandle.identifier.secretId}|${sessionHandle.protocol.getValue}"

    response = webTarget.path(s"api/v1/sessions/$serializedSessionHandle/operations/$operationType")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    val operationHandle = response.readEntity(classOf[OperationHandle])

    s"${operationHandle.identifier.publicId}|" +
      s"${operationHandle.identifier.secretId}|${operationHandle.protocol.getValue}|" +
      s"${operationHandle.typ.toString}"

  }
}
