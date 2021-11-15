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

import java.util
import javax.ws.rs.client.Entity
import javax.ws.rs.core.{MediaType, Response}

import scala.concurrent.duration._

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.operation.{OperationHandle, OperationType}
import org.apache.kyuubi.session.SessionHandle

class SessionsResourceSuite extends KyuubiFunSuite with RestFrontendTestHelper {

  test("test open and count session") {
    val requestObj = SessionOpenRequest(
      1, "admin", "123456", "localhost", Map("testConfig" -> "testValue"))

    withKyuubiRestServer { (_, _, _, webTarget) =>
      var response = webTarget.path("api/v1/sessions")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))

      assert(200 == response.getStatus)

      val sessionHandle = response.readEntity(classOf[SessionHandle])

      assert(sessionHandle.protocol.getValue == 1)
      assert(sessionHandle.identifier != null)

      // verify the open session count
      response = webTarget.path("api/v1/sessions/count").request().get()
      val openedSessionCount = response.readEntity(classOf[SessionOpenCount])
      assert(openedSessionCount.openSessionCount == 1)
    }
  }

  test("test close and count session") {
    val requestObj = SessionOpenRequest(
      1, "admin", "123456", "localhost", Map("testConfig" -> "testValue"))

    withKyuubiRestServer { (_, _, _, webTarget) =>
      var response = webTarget.path("api/v1/sessions")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))

      assert(200 == response.getStatus)

      val sessionHandle = response.readEntity(classOf[SessionHandle])

      assert(sessionHandle.protocol.getValue == 1)
      assert(sessionHandle.identifier != null)

      // close a opened session
      val serializedSessionHandle = s"${sessionHandle.identifier.publicId}|" +
        s"${sessionHandle.identifier.secretId}|${sessionHandle.protocol.getValue}"
      response = webTarget.path(s"api/v1/sessions/$serializedSessionHandle").request().delete()
      assert(200 == response.getStatus)

      // verify the open session count again
      response = webTarget.path("api/v1/sessions/count").request().get()
      val openedSessionCount = response.readEntity(classOf[SessionOpenCount])
      assert(openedSessionCount.openSessionCount == 0)
    }
  }

  test("test execPoolStatistic") {
    withKyuubiRestServer { (restFe, _, _, webTarget) =>

      val sessionManager = restFe.be.sessionManager
      val future = sessionManager.submitBackgroundOperation(() => Thread.sleep(1000))

      // verify the exec pool statistic
      var response = webTarget.path("api/v1/sessions/execPool/statistic").request().get()
      val execPoolStatistic1 = response.readEntity(classOf[ExecPoolStatistic])
      assert(execPoolStatistic1.execPoolSize == 1 && execPoolStatistic1.execPoolActiveCount == 1)

      future.cancel(true)
      eventually(timeout(3.seconds), interval(200.milliseconds)) {
        response = webTarget.path("api/v1/sessions/execPool/statistic").request().get()
        val statistic = response.readEntity(classOf[ExecPoolStatistic])
        assert(statistic.execPoolSize == 1 && statistic.execPoolActiveCount == 0)
      }

      sessionManager.stop()
      response = webTarget.path("api/v1/sessions/execPool/statistic").request().get()
      val execPoolStatistic3 = response.readEntity(classOf[ExecPoolStatistic])
      assert(execPoolStatistic3.execPoolSize == 0 && execPoolStatistic3.execPoolActiveCount == 0)
    }
  }

  test("test getSessionList") {
    val requestObj = SessionOpenRequest(
      1, "admin", "123456", "localhost", Map("testConfig" -> "testValue"))

    withKyuubiRestServer { (_, _, _, webTarget) =>
      var response = webTarget.path("api/v1/sessions")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))

      // get session list
      var response2 = webTarget.path("api/v1/sessions").request().get()
      assert(200 == response2.getStatus)
      val sessions1 = response2.readEntity(classOf[SessionList])
      assert(sessions1.sessionList.nonEmpty)

      // close a opened session
      val sessionHandle = response.readEntity(classOf[SessionHandle])
      val serializedSessionHandle = s"${sessionHandle.identifier.publicId}|" +
        s"${sessionHandle.identifier.secretId}|${sessionHandle.protocol.getValue}"
      response = webTarget.path(s"api/v1/sessions/$serializedSessionHandle").request().delete()
      assert(200 == response.getStatus)

      // get session list again
      response2 = webTarget.path("api/v1/sessions").request().get()
      assert(200 == response2.getStatus)
      val sessions2 = response2.readEntity(classOf[SessionList])
      assert(sessions2.sessionList.isEmpty)
    }
  }

  test("test getSessionDetail") {
    val requestObj = SessionOpenRequest(
      1, "admin", "123456", "localhost", Map("testConfig" -> "testValue"))

    withKyuubiRestServer { (_, _, _, webTarget) =>
      var response: Response = webTarget.path("api/v1/sessions")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))

      val sessionHandle = response.readEntity(classOf[SessionHandle])
      val serializedSessionHandle = s"${sessionHandle.identifier.publicId}|" +
        s"${sessionHandle.identifier.secretId}|${sessionHandle.protocol.getValue}"

      // get session detail
      response = webTarget.path(s"api/v1/sessions/$serializedSessionHandle").request().get()
      assert(200 == response.getStatus)
      val sessions = response.readEntity(classOf[SessionDetail])
      assert(sessions.configs.nonEmpty)

      // close a opened session
      response = webTarget.path(s"api/v1/sessions/$serializedSessionHandle").request().delete()
      assert(200 == response.getStatus)

      // get session detail again
      response = webTarget.path(s"api/v1/sessions/$serializedSessionHandle").request().get()
      assert(404 == response.getStatus)
    }
  }

  test("test get infoType") {
    val requestObj = SessionOpenRequest(
      1, "admin", "123456", "localhost", Map("testConfig" -> "testValue"))

    withKyuubiRestServer { (_, _, _, webTarget) =>
      var response: Response = webTarget.path("api/v1/sessions")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))

      val sessionHandle = response.readEntity(classOf[SessionHandle])
      val serializedSessionHandle = s"${sessionHandle.identifier.publicId}|" +
        s"${sessionHandle.identifier.secretId}|${sessionHandle.protocol.getValue}"

      response = webTarget.path(s"api/v1/sessions/$serializedSessionHandle/info/13")
        .request().get()
      assert(200 == response.getStatus)
      val sessions = response.readEntity(classOf[InfoDetail])
      assert(sessions.infoType.equals("CLI_SERVER_NAME") && sessions.infoValue.equals("Kyuubi"))
      // Invalid sessionHandleStr
      val handle = "b88d6b56-d200-4bb6-bf0a-5da0ea572e11|0c4aad4e-ccf7-4abd-9305-943d4bfd2d9a|0"
      response = webTarget.path(s"api/v1/sessions/$handle/info/13").request().get()
      assert(404 == response.getStatus)
      response = webTarget.path(s"api/v1/sessions/0/info/13").request().get()
      assert(404 == response.getStatus)

      // Invalid infoType
      response = webTarget.path(s"api/v1/sessions/$serializedSessionHandle/info/0")
        .request().get()
      assert(404 == response.getStatus)
      response = webTarget.path(s"api/v1/sessions/$serializedSessionHandle/info/str")
        .request().get()
      assert(404 == response.getStatus)
    }
  }

  test("test get operationHandle") {
    val requestObj = SessionOpenRequest(
      1, "admin", "123456", "localhost", Map("testConfig" -> "testValue"))

    withKyuubiRestServer { (_, _, _, webTarget) =>
      var response: Response = webTarget.path("api/v1/sessions")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))

      val sessionHandle = response.readEntity(classOf[SessionHandle])
      val serializedSessionHandle = s"${sessionHandle.identifier.publicId}|" +
        s"${sessionHandle.identifier.secretId}|${sessionHandle.protocol.getValue}"

      val path = s"api/v1/sessions/$serializedSessionHandle"

      val statementReq = StatementRequest("show tables", true, 3000)
      response = webTarget
        .path(s"$path/operations/statement").request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(statementReq, MediaType.APPLICATION_JSON_TYPE))
      assert(200 == response.getStatus)
      var operationHandle = response.readEntity(classOf[OperationHandle])
      assert(operationHandle.typ == OperationType.EXECUTE_STATEMENT)

      response = webTarget.path(s"$path/operations/typeinfo").request()
        .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE))
      assert(200 == response.getStatus)
      operationHandle = response.readEntity(classOf[OperationHandle])
      assert(operationHandle.typ == OperationType.GET_TYPE_INFO)

      response = webTarget.path(s"$path/operations/catalogs")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE))
      assert(200 == response.getStatus)
      operationHandle = response.readEntity(classOf[OperationHandle])
      assert(operationHandle.typ == OperationType.GET_CATALOGS)

      val getSchemasReq = GetSchemasRequest("default", "default")
      response = webTarget.path(s"$path/operations/schemas")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(getSchemasReq, MediaType.APPLICATION_JSON_TYPE))
      assert(200 == response.getStatus)
      operationHandle = response.readEntity(classOf[OperationHandle])
      assert(operationHandle.typ == OperationType.GET_SCHEMAS)

      val tableTypes = new util.ArrayList[String]()
      val getTablesReq = GetTablesRequest("default", "default", "default", tableTypes)
      response = webTarget.path(s"$path/operations/tables")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(getTablesReq, MediaType.APPLICATION_JSON_TYPE))
      assert(200 == response.getStatus)
      operationHandle = response.readEntity(classOf[OperationHandle])
      assert(operationHandle.typ == OperationType.GET_TABLES)

      response = webTarget.path(s"$path/operations/tabletypes").request()
        .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE))
      assert(200 == response.getStatus)
      operationHandle = response.readEntity(classOf[OperationHandle])
      assert(operationHandle.typ == OperationType.GET_TABLE_TYPES)

      val getColumnsReq = GetColumnsRequest("default", "default", "default", "default")
      response = webTarget.path(s"$path/operations/columns")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(getColumnsReq, MediaType.APPLICATION_JSON_TYPE))
      assert(200 == response.getStatus)
      operationHandle = response.readEntity(classOf[OperationHandle])
      assert(operationHandle.typ == OperationType.GET_COLUMNS)

      var getFunctionsReq = GetFunctionsRequest("default", "default", "default")
      response = webTarget.path(s"$path/operations/functions")
        .request(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(getFunctionsReq, MediaType.APPLICATION_JSON_TYPE))
      assert(200 == response.getStatus)
      operationHandle = response.readEntity(classOf[OperationHandle])
      assert(operationHandle.typ == OperationType.GET_FUNCTIONS)
    }
  }
}
