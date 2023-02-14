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

import io.trino.client.{QueryError, QueryResults}
import io.trino.client.ProtocolHeaders.TRINO_HEADERS

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException, TrinoRestFrontendTestHelper}
import org.apache.kyuubi.operation.{OperationHandle, OperationState}
import org.apache.kyuubi.server.trino.api.{Query, TrinoContext}
import org.apache.kyuubi.server.trino.api.v1.dto.Ok
import org.apache.kyuubi.session.SessionHandle

class StatementResourceSuite extends KyuubiFunSuite with TrinoRestFrontendTestHelper {

  case class TrinoResponse(
      response: Option[Response] = None,
      queryError: Option[QueryError] = None,
      data: List[List[Any]] = List[List[Any]](),
      isEnd: Boolean = false)

  test("statement test") {
    val response = webTarget.path("v1/statement/test").request().get()
    val result = response.readEntity(classOf[Ok])
    assert(result == new Ok("trino server is running"))
  }

  test("statement submit for query error") {

    val response = webTarget.path("v1/statement")
      .request().post(Entity.entity("select a", MediaType.TEXT_PLAIN_TYPE))

    val trinoResponseIter = Iterator.iterate(TrinoResponse(response = Option(response)))(getData)
    val isErr = trinoResponseIter.takeWhile(_.isEnd == false).exists { t =>
      t.queryError != None && t.response == None
    }
    assert(isErr == true)
  }

  test("statement submit and get result") {
    val response = webTarget.path("v1/statement")
      .request().post(Entity.entity("select 1", MediaType.TEXT_PLAIN_TYPE))

    val trinoResponseIter = Iterator.iterate(TrinoResponse(response = Option(response)))(getData)
    val dataSet = trinoResponseIter
      .takeWhile(_.isEnd == false)
      .map(_.data)
      .flatten.toList
    assert(dataSet == List(List(1)))
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
          case Array(Query.KYUUBI_SESSION_ID, _) => true
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
      s"${Query.KYUUBI_SESSION_ID}=${TrinoContext.urlEncode(sessionHandle.identifier.toString)}")
      .delete()
    assert(nextResponse.getStatus == 204)
    assert(operation.getStatus.state == OperationState.CLOSED)
    val exception = intercept[KyuubiSQLException](sessionManager.getSession(sessionHandle))
    assert(exception.getMessage === s"Invalid $sessionHandle")
  }

  private def getData(current: TrinoResponse): TrinoResponse = {
    current.response.map { response =>
      assert(response.getStatus == 200)
      val qr = response.readEntity(classOf[QueryResults])
      val nextData = Option(qr.getData)
        .map(_.asScala.toList.map(_.asScala.toList))
        .getOrElse(List[List[Any]]())
      val nextResponse = Option(qr.getNextUri).map {
        uri =>
          val path = uri.getPath
          val headers = response.getHeaders
          webTarget.path(path).request().headers(headers).get()
      }
      TrinoResponse(nextResponse, Option(qr.getError), nextData)
    }.getOrElse(TrinoResponse(isEnd = true))
  }

}
