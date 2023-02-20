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
import javax.ws.rs.client.Entity
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.client.api.v1.dto._
import org.apache.kyuubi.events.KyuubiOperationEvent
import org.apache.kyuubi.operation.{ExecuteStatement, OperationState}
import org.apache.kyuubi.operation.OperationState.{FINISHED, OperationState}

class OperationsResourceSuite extends KyuubiFunSuite with RestFrontendTestHelper {

  test("get an operation event") {
    val catalogsHandleStr = getOpHandleStr("")
    checkOpState(catalogsHandleStr, FINISHED)

    val statementHandleStr = getOpHandleStr()
    checkOpState(statementHandleStr, FINISHED)

    // Invalid operationHandleStr
    val invalidOperationHandle = UUID.randomUUID().toString
    val response = webTarget.path(s"api/v1/operations/$invalidOperationHandle/event")
      .request(MediaType.APPLICATION_JSON_TYPE).get()
    assert(404 == response.getStatus)
  }

  test("apply an action for an operation") {
    val sessionHandle = fe.be.openSession(
      HIVE_CLI_SERVICE_PROTOCOL_V2,
      "admin",
      "123456",
      "localhost",
      Map("testConfig" -> "testValue"))
    val sessionManager = fe.be.sessionManager
    val session = sessionManager.getSession(sessionHandle)
    val op = new ExecuteStatement(session, "show tables", Map.empty, true, 3000)
    op.setState(OperationState.RUNNING)
    sessionManager.operationManager.addOperation(op)
    val opHandleStr = op.getHandle.identifier.toString
    var response = webTarget.path(s"api/v1/operations/$opHandleStr")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .put(Entity.entity(new OpActionRequest("cancel"), MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    checkOpState(opHandleStr, OperationState.CANCELED)

    response = webTarget.path(s"api/v1/operations/$opHandleStr")
      .request(MediaType.APPLICATION_JSON_TYPE)
      .put(Entity.entity(new OpActionRequest("close"), MediaType.APPLICATION_JSON_TYPE))
    assert(200 == response.getStatus)
    response = webTarget.path(s"api/v1/operations/$opHandleStr/event")
      .request(MediaType.APPLICATION_JSON_TYPE).get()
    assert(404 == response.getStatus)
  }

  test("get result set metadata") {
    val opHandleStr = getOpHandleStr()
    checkOpState(opHandleStr, FINISHED)
    val response = webTarget.path(s"api/v1/operations/$opHandleStr/resultsetmetadata")
      .request(MediaType.APPLICATION_JSON_TYPE).get()
    assert(200 == response.getStatus)
    val resultSetMetaData = response.readEntity(classOf[ResultSetMetaData])
    assert(resultSetMetaData.getColumns.get(1).getColumnName.equals("tableName"))
  }

  test("get operation log") {
    val opHandleStr = getOpHandleStr()
    checkOpState(opHandleStr, FINISHED)
    val response = webTarget.path(
      s"api/v1/operations/$opHandleStr/log")
      .queryParam("maxrows", "10")
      .request(MediaType.APPLICATION_JSON).get()
    assert(200 == response.getStatus)
    val logRowSet = response.readEntity(classOf[OperationLog])
    assert(logRowSet.getLogRowSet.asScala.exists(_.contains("show tables")))
    assert(logRowSet.getRowCount === 10)
  }

  test("test get result row set") {
    val opHandleStr = getOpHandleStr("select \"test\", 1, 0.32d, true")
    checkOpState(opHandleStr, FINISHED)
    val response = webTarget.path(
      s"api/v1/operations/$opHandleStr/rowset")
      .queryParam("maxrows", "2")
      .queryParam("fetchorientation", "FETCH_NEXT")
      .request(MediaType.APPLICATION_JSON).get()
    assert(200 == response.getStatus)
    val logRowSet = response.readEntity(classOf[ResultRowSet])
    assert("test".equals(logRowSet.getRows.asScala.head.getFields.asScala.head.getValue))
    assert(logRowSet.getRowCount == 1)
  }

  test("test get result row set with default value") {
    val opHandleStr = getOpHandleStr("select \"test\", 1, 0.32d, true")
    checkOpState(opHandleStr, FINISHED)
    val response = webTarget.path(
      s"api/v1/operations/$opHandleStr/rowset")
      .request(MediaType.APPLICATION_JSON).get()
    assert(200 == response.getStatus)
    val logRowSet = response.readEntity(classOf[ResultRowSet])
    assert("test".equals(logRowSet.getRows.asScala.head.getFields.asScala.head.getValue))
    assert(logRowSet.getRowCount == 1)
  }

  test("test get result row set with null value") {
    val opHandleStr = getOpHandleStr(
      s"""
         |select
         |cast(null as string) as c1,
         |cast(null as boolean) as c2,
         |cast(null as byte) as c3,
         |cast(null as double) as c4,
         |cast(null as short) as c5,
         |cast(null as int) as c6,
         |cast(null as bigint) as c7
         |""".stripMargin)
    checkOpState(opHandleStr, FINISHED)
    val response = webTarget.path(
      s"api/v1/operations/$opHandleStr/rowset")
      .queryParam("maxrows", "2")
      .queryParam("fetchorientation", "FETCH_NEXT")
      .request(MediaType.APPLICATION_JSON).get()
    assert(200 == response.getStatus)
    val logRowSet = response.readEntity(classOf[ResultRowSet])
    assert(logRowSet.getRows.asScala.head.getFields.asScala.forall(_.getValue == null))
    assert(logRowSet.getRowCount == 1)
  }

  def getOpHandleStr(statement: String = "show tables"): String = {
    val sessionHandle = fe.be.openSession(
      HIVE_CLI_SERVICE_PROTOCOL_V2,
      "admin",
      "123456",
      "localhost",
      Map("testConfig" -> "testValue"))

    val op =
      if (statement.nonEmpty) {
        fe.be.executeStatement(sessionHandle, statement, Map.empty, runAsync = true, 3000)
      } else {
        fe.be.getCatalogs(sessionHandle)
      }

    op.identifier.toString
  }

  private def checkOpState(opHandleStr: String, state: OperationState): Unit = {
    eventually(Timeout(5.seconds)) {
      val response = webTarget.path(s"api/v1/operations/$opHandleStr/event")
        .request(MediaType.APPLICATION_JSON_TYPE).get()
      assert(response.getStatus === 200)
      val operationEvent = response.readEntity(classOf[KyuubiOperationEvent])
      assert(operationEvent.state === state.name())
    }
  }
}
