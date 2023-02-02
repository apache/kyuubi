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

package org.apache.kyuubi.server.trino.api.v1

import javax.ws.rs.client.Entity
import javax.ws.rs.core.{MediaType, Response}

import scala.collection.JavaConverters._

import io.trino.client.ProtocolHeaders.TRINO_HEADERS
import io.trino.client.QueryResults

import org.apache.kyuubi.{KyuubiSQLException, TrinoRestFrontendTestHelper}
import org.apache.kyuubi.operation.{OperationHandle, OperationState}
import org.apache.kyuubi.server.trino.api.TrinoContext
import org.apache.kyuubi.server.trino.api.v1.dto.Ok
import org.apache.kyuubi.session.SessionHandle

class StatementResourceSuite extends TrinoRestFrontendTestHelper {

  test("statement test") {
    val response = webTarget.path("v1/statement/test").request().get()
    val result = response.readEntity(classOf[Ok])
    assert(result == new Ok("trino server is running"))
  }

  test("statement submit and get result") {
    val response = webTarget.path("v1/statement")
      .request().post(Entity.entity("select 1", MediaType.TEXT_PLAIN_TYPE))
    checkResult(response)
  }

  test("query cancel") {
    val response = webTarget.path("v1/statement")
      .request().post(Entity.entity("select 1", MediaType.TEXT_PLAIN_TYPE))
    val qr = response.readEntity(classOf[QueryResults])
    val sessionManager = fe.be.sessionManager
    val sessionHandle =
      response.getStringHeaders.get(TRINO_HEADERS.responseSetSession).asScala
        .map(_.split("="))
        .find {
          case Array("sessionId", _) => true
        }
        .map {
          case Array(_, value) => SessionHandle.fromUUID(TrinoContext.urlDecode(value))
        }.get
    sessionManager.getSession(sessionHandle)
    val operationHandle = OperationHandle(qr.getId)
    val operation = sessionManager.operationManager.getOperation(operationHandle)
    assert(response.getStatus == 200)
    val path = qr.getNextUri.getPath
    val nextResponse = webTarget.path(path).request().header(
      TRINO_HEADERS.requestSession(),
      s"sessionId=${TrinoContext.urlEncode(sessionHandle.identifier.toString)}").delete()
    assert(nextResponse.getStatus == 204)
    assert(operation.getStatus.state == OperationState.CLOSED)
    val exception = intercept[KyuubiSQLException](sessionManager.getSession(sessionHandle))
    assert(exception.getMessage === s"Invalid $sessionHandle")

  }

  private def checkResult(response: Response): Unit = {
    assert(response.getStatus == 200)
    val qr = response.readEntity(classOf[QueryResults])
    if (qr.getData.iterator().hasNext) {
      val resultSet = qr.getData.iterator()
      assert(resultSet.next.asScala == List(1))
    }
    if (qr.getNextUri != null) {
      val path = qr.getNextUri.getPath
      val headers = response.getHeaders
      val nextResponse = webTarget.path(path).request().headers(headers).get()
      checkResult(nextResponse)
    }

  }

}
