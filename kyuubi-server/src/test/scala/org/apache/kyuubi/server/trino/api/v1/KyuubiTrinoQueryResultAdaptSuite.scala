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

import java.net.URI
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.events.KyuubiOperationEvent
import org.apache.kyuubi.operation.{FetchOrientation, OperationHandle}
import org.apache.kyuubi.operation.OperationState.{FINISHED, OperationState}
import org.apache.kyuubi.server.trino.api.KyuubiTrinoQueryResultAdapt

class KyuubiTrinoQueryResultAdaptSuite extends KyuubiFunSuite with RestFrontendTestHelper {

  test("test convert") {
    val opHandle = getOpHandle("select 1")
    val opHandleStr = opHandle.identifier.toString
    checkOpState(opHandleStr, FINISHED)

    val metadataResp = fe.be.getResultSetMetadata(opHandle)
    val tRowSet = fe.be.fetchResults(opHandle, FetchOrientation.FETCH_NEXT, 1000, false)
    val status = fe.be.getOperationStatus(opHandle)

    val uri = new URI("sfdsfsdfdsf")
    val results = KyuubiTrinoQueryResultAdapt
      .createQueryResults("/xdfd/xdf", uri, uri, status, Option(metadataResp), Option(tRowSet))

    print(results.toString)
    assert(results.getColumns.get(0).getType.equals("INT_TYPE"))
    assert(results.getData.asScala.last.get(0) == 1)
  }

  test("test convert from table") {
    initSql("CREATE DATABASE IF NOT EXISTS INIT_DB")
    initSql("CREATE TABLE IF NOT EXISTS INIT_DB.test(a int) USING CSV;")
    initSql("INSERT INTO INIT_DB.test VALUES (2)")

    val opHandle = getOpHandle("SELECT * FROM INIT_DB.test")
    val opHandleStr = opHandle.identifier.toString
    checkOpState(opHandleStr, FINISHED)

    val metadataResp = fe.be.getResultSetMetadata(opHandle)
    val tRowSet = fe.be.fetchResults(opHandle, FetchOrientation.FETCH_NEXT, 1000, false)
    val status = fe.be.getOperationStatus(opHandle)

    val uri = new URI("sfdsfsdfdsf")
    val results = KyuubiTrinoQueryResultAdapt
      .createQueryResults("/xdfd/xdf", uri, uri, status, Option(metadataResp), Option(tRowSet))

    print(results.toString)
    assert(results.getColumns.get(0).getType.equals("INT_TYPE"))
    assert(results.getData.asScala.last.get(0) == 2)
  }

  def getOpHandleStr(statement: String = "show tables"): String = {
    getOpHandle(statement).identifier.toString
  }

  def getOpHandle(statement: String = "show tables"): OperationHandle = {
    val sessionHandle = fe.be.openSession(
      HIVE_CLI_SERVICE_PROTOCOL_V9,
      "admin",
      "123456",
      "localhost",
      Map("testConfig" -> "testValue"))

    if (statement.nonEmpty) {
      fe.be.executeStatement(sessionHandle, statement, Map.empty, runAsync = false, 5000)
    } else {
      fe.be.getCatalogs(sessionHandle)
    }
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

  private def initSql(sql: String): Unit = {
    val initOpHandle = getOpHandle(sql)
    val initOpHandleStr = initOpHandle.identifier.toString
    checkOpState(initOpHandleStr, FINISHED)
  }
}
