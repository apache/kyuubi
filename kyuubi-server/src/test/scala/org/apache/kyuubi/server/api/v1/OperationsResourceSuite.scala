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

import org.apache.hive.service.rpc.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.events.KyuubiOperationEvent
import org.apache.kyuubi.operation.{OperationState, OperationType}
import org.apache.kyuubi.operation.OperationState.{FINISHED, OperationState}
import org.apache.kyuubi.operation.OperationType.OperationType

class OperationsResourceSuite extends KyuubiFunSuite with RestFrontendTestHelper {

  test("test get an operation event") {
    val catalogsHandleStr = getOpHandleStr(OperationType.GET_CATALOGS)
    var response = webTarget.path(s"api/v1/operations/$catalogsHandleStr/event")
      .request(MediaType.APPLICATION_JSON_TYPE).get()
    val operationEvent = response.readEntity(classOf[KyuubiOperationEvent])
    assert(200 == response.getStatus)
    checkOpState(catalogsHandleStr, FINISHED)

    val statementHandleStr = getOpHandleStr(OperationType.EXECUTE_STATEMENT)
    response = webTarget.path(s"api/v1/operations/$statementHandleStr/event")
      .request(MediaType.APPLICATION_JSON_TYPE).get()
    val statementEvent = response.readEntity(classOf[KyuubiOperationEvent])
    assert(200 == response.getStatus)
    checkOpState(statementHandleStr, FINISHED)

    // Invalid operationHandleStr
    val invalidOperationHandle =
      statementHandleStr.replaceAll("EXECUTE_STATEMENT", "GET_TYPE_INFO")
    response = webTarget.path(s"api/v1/operations/$invalidOperationHandle/event")
      .request(MediaType.APPLICATION_JSON_TYPE).get()
    assert(404 == response.getStatus)
  }

  test("test apply an action for an operation") {
    val opHandleStr = getOpHandleStr(OperationType.EXECUTE_STATEMENT)

    var response = webTarget.path(s"api/v1/operations/$opHandleStr")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .put(Entity.entity(OpActionRequest("cancel"), MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)

    response = webTarget.path(s"api/v1/operations/$opHandleStr/event")
      .request(MediaType.APPLICATION_JSON_TYPE).get()
    val operationEvent = response.readEntity(classOf[KyuubiOperationEvent])
    assert(operationEvent.state == OperationState.FINISHED.name() ||
      operationEvent.state == OperationState.CANCELED.name())

    response = webTarget.path(s"api/v1/operations/$opHandleStr")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .put(Entity.entity(OpActionRequest("close"), MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)

    response = webTarget.path(s"api/v1/operations/$opHandleStr/event")
      .request(MediaType.APPLICATION_JSON_TYPE).get()
    assert(404 == response.getStatus)
  }

  test("test get result set metadata") {
    val opHandleStr = getOpHandleStr(OperationType.EXECUTE_STATEMENT)
    checkOpState(opHandleStr, FINISHED)
    val response = webTarget.path(s"api/v1/operations/$opHandleStr/resultsetmetadata")
      .request(MediaType.APPLICATION_JSON_TYPE).get()
    assert(200 == response.getStatus)
    val resultSetMetaData = response.readEntity(classOf[ResultSetMetaData])
    assert(resultSetMetaData.columns.head.columnName.equals("database"))
  }

  test("test get operation log") {
    val opHandleStr = getOpHandleStr(OperationType.EXECUTE_STATEMENT)
    checkOpState(opHandleStr, FINISHED)
    val response = webTarget.path(
      s"api/v1/operations/$opHandleStr/log")
      .queryParam("maxrows", "10")
      .request(MediaType.APPLICATION_JSON).get()
    assert(200 == response.getStatus)
    val logRowSet = response.readEntity(classOf[OperationLog])
    assert(logRowSet.logRowSet.exists(_.contains("show tables")))
    assert(logRowSet.rowCount === 10)
  }

  def getOpHandleStr(typ: OperationType): String = {
    val sessionHandle = fe.be.openSession(
      HIVE_CLI_SERVICE_PROTOCOL_V2,
      "admin",
      "123456",
      "localhost",
      Map("testConfig" -> "testValue"))

    val op = typ match {
      case OperationType.EXECUTE_STATEMENT =>
        fe.be.executeStatement(sessionHandle, "show tables", runAsync = true, 3000)
      case OperationType.GET_CATALOGS => fe.be.getCatalogs(sessionHandle)
    }

    s"${op.identifier.publicId}|" +
      s"${op.identifier.secretId}|${op.protocol.getValue}|" +
      s"${op.typ.toString}"
  }

  private def checkOpState(opHandleStr: String, state: OperationState): Unit = {
    eventually(Timeout(5.seconds)) {
      val response = webTarget.path(s"api/v1/operations/$opHandleStr/event")
        .request(MediaType.APPLICATION_JSON_TYPE).get()
      val operationEvent = response.readEntity(classOf[KyuubiOperationEvent])
      assert(operationEvent.state === state.name())
    }
  }
}
