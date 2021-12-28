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

import org.apache.hive.service.rpc.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.events.KyuubiSessionEvent
import org.apache.kyuubi.operation.{OperationHandle, OperationType}
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.session.SessionHandle

class SessionsResourceSuite extends KyuubiFunSuite with RestFrontendTestHelper {

  test("open/close and count session") {
    val requestObj = SessionOpenRequest(
      1,
      "admin",
      "123456",
      "localhost",
      Map("testConfig" -> "testValue"))

    var response = webTarget.path("api/v1/sessions")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))

    assert(200 == response.getStatus)

    val sessionHandle = response.readEntity(classOf[SessionHandle])

    assert(sessionHandle.protocol.getValue == 1)
    assert(sessionHandle.identifier != null)

    val statistic = webTarget.path("api/v1/sessions/execPool/statistic").request().get()
    val execPoolStatistic1 = statistic.readEntity(classOf[ExecPoolStatistic])
    assert(execPoolStatistic1.execPoolSize == 1 && execPoolStatistic1.execPoolActiveCount == 1)

    response = webTarget.path("api/v1/sessions/count").request().get()
    val openedSessionCount = response.readEntity(classOf[SessionOpenCount])
    assert(openedSessionCount.openSessionCount == 1)

    // close an opened session
    val serializedSessionHandle = s"${sessionHandle.identifier.publicId}|" +
      s"${sessionHandle.identifier.secretId}|${sessionHandle.protocol.getValue}"
    response = webTarget.path(s"api/v1/sessions/$serializedSessionHandle").request().delete()
    assert(200 == response.getStatus)

    response = webTarget.path("api/v1/sessions/count").request().get()
    val openedSessionCount2 = response.readEntity(classOf[SessionOpenCount])
    assert(openedSessionCount2.openSessionCount == 0)
  }

  test("getSessionList") {
    val requestObj = SessionOpenRequest(
      1,
      "admin",
      "123456",
      "localhost",
      Map("testConfig" -> "testValue"))

    var response = webTarget.path("api/v1/sessions")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(requestObj, MediaType.APPLICATION_JSON_TYPE))

    // get session list
    var response2 = webTarget.path("api/v1/sessions").request().get()
    assert(200 == response2.getStatus)
    val sessions1 = response2.readEntity(classOf[SessionList])
    assert(sessions1.sessionList.nonEmpty)

    // close an opened session
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

  test("get session event") {
    val sessionManager = fe.be.sessionManager
    val sessionHandle = sessionManager.openSession(
      HIVE_CLI_SERVICE_PROTOCOL_V2,
      "admin",
      "123456",
      "localhost",
      Map("testConfig" -> "testValue"))
    val serializedSessionHandle = s"${sessionHandle.identifier.publicId}|" +
      s"${sessionHandle.identifier.secretId}|${sessionHandle.protocol.getValue}"

    KyuubiServer.kyuubiServer = new KyuubiServer
    KyuubiServer.kyuubiServer.initialize(KyuubiConf())

    // get session event
    var response = webTarget.path(s"api/v1/sessions/$serializedSessionHandle").request().get()
    assert(200 == response.getStatus)
    val sessions = response.readEntity(classOf[KyuubiSessionEvent])
    assert(sessions.conf("testConfig").equals("testValue"))

    // close an opened session
    response = webTarget.path(s"api/v1/sessions/$serializedSessionHandle").request().delete()
    assert(200 == response.getStatus)

    // get session detail again
    response = webTarget.path(s"api/v1/sessions/$serializedSessionHandle").request().get()
    assert(404 == response.getStatus)
  }

  test("get infoType") {
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

    response = webTarget.path(s"api/v1/sessions/$serializedSessionHandle/info/13")
      .request().get()
    assert(200 == response.getStatus)
    val sessions = response.readEntity(classOf[InfoDetail])
    assert(sessions.infoType.equals("CLI_SERVER_NAME") &&
      sessions.infoValue.equals("Apache Kyuubi (Incubating)"))
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

  test("submit operation and get operation handle") {
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

    val pathPrefix = s"api/v1/sessions/$serializedSessionHandle"

    val statementReq = StatementRequest("show tables", true, 3000)
    response = webTarget
      .path(s"$pathPrefix/operations/statement").request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(statementReq, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    var operationHandle = response.readEntity(classOf[OperationHandle])
    assert(operationHandle.typ == OperationType.EXECUTE_STATEMENT)

    response = webTarget.path(s"$pathPrefix/operations/typeInfo").request()
      .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    operationHandle = response.readEntity(classOf[OperationHandle])
    assert(operationHandle.typ == OperationType.GET_TYPE_INFO)

    response = webTarget.path(s"$pathPrefix/operations/catalogs")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    operationHandle = response.readEntity(classOf[OperationHandle])
    assert(operationHandle.typ == OperationType.GET_CATALOGS)

    val getSchemasReq = GetSchemasRequest("spark_catalog", "default")
    response = webTarget.path(s"$pathPrefix/operations/schemas")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(getSchemasReq, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    operationHandle = response.readEntity(classOf[OperationHandle])
    assert(operationHandle.typ == OperationType.GET_SCHEMAS)

    val tableTypes = new util.ArrayList[String]()
    val getTablesReq = GetTablesRequest("spark_catalog", "default", "default", tableTypes)
    response = webTarget.path(s"$pathPrefix/operations/tables")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(getTablesReq, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    operationHandle = response.readEntity(classOf[OperationHandle])
    assert(operationHandle.typ == OperationType.GET_TABLES)

    response = webTarget.path(s"$pathPrefix/operations/tableTypes").request()
      .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    operationHandle = response.readEntity(classOf[OperationHandle])
    assert(operationHandle.typ == OperationType.GET_TABLE_TYPES)

    val getColumnsReq = GetColumnsRequest("spark_catalog", "default", "default", "default")
    response = webTarget.path(s"$pathPrefix/operations/columns")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(getColumnsReq, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    operationHandle = response.readEntity(classOf[OperationHandle])
    assert(operationHandle.typ == OperationType.GET_COLUMNS)

    val getFunctionsReq = GetFunctionsRequest("default", "default", "default")
    response = webTarget.path(s"$pathPrefix/operations/functions")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(getFunctionsReq, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    operationHandle = response.readEntity(classOf[OperationHandle])
    assert(operationHandle.typ == OperationType.GET_FUNCTIONS)
  }

  test("close an operation") {
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

    val pathPrefix = s"api/v1/sessions/$serializedSessionHandle"

    response = webTarget.path(s"$pathPrefix/operations/catalogs")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .post(Entity.entity(null, MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    val operationHandle = response.readEntity(classOf[OperationHandle])
    assert(operationHandle.typ == OperationType.GET_CATALOGS)

    val serializedOperationHandle = s"${operationHandle.identifier.publicId}|" +
      s"${operationHandle.identifier.secretId}|${operationHandle.protocol.getValue}|" +
      s"${operationHandle.typ.toString}"

    response = webTarget.path(s"$pathPrefix/operations/$serializedOperationHandle")
      .request(MediaType.APPLICATION_JSON_TYPE).delete()
    assert(200 == response.getStatus)

    // verify operation
    response = webTarget.path(s"api/v1/operations/$serializedOperationHandle/event")
      .request(MediaType.APPLICATION_JSON_TYPE).get()
    assert(404 == response.getStatus)
  }
}
