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

package org.apache.kyuubi.server.trino.api

import java.net.URI
import java.time.ZoneId
import javax.ws.rs.core.MediaType

import scala.collection.JavaConverters._

import io.trino.client.ProtocolHeaders.TRINO_HEADERS
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{KyuubiFunSuite, RestFrontendTestHelper}
import org.apache.kyuubi.events.KyuubiOperationEvent
import org.apache.kyuubi.operation.{FetchOrientation, OperationHandle}
import org.apache.kyuubi.operation.OperationState.{FINISHED, OperationState}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9

class TrinoContextSuite extends KyuubiFunSuite with RestFrontendTestHelper {

  import TrinoContext._

  test("create trino request context with header") {
    val testHeader0 = Map(
      TRINO_HEADERS.requestUser -> List("requestUser"),
      TRINO_HEADERS.requestTimeZone -> List(ZoneId.systemDefault().getId),
      TRINO_HEADERS.requestTransactionId -> List("NONE"),
      TRINO_HEADERS.requestClientCapabilities -> List("requestClientCapabilities"),
      TRINO_HEADERS.requestSource -> List("requestSource"),
      TRINO_HEADERS.requestCatalog -> List("requestCatalog"),
      TRINO_HEADERS.requestSchema -> List("requestSchema"),
      TRINO_HEADERS.requestLanguage -> List("requestLanguage"),
      TRINO_HEADERS.requestTraceToken -> List("requestTraceToken"),
      TRINO_HEADERS.requestClientInfo -> List("requestClientInfo"),
      TRINO_HEADERS.requestClientTags -> List(
        "requestClientTag1,requestClientTag2,requestClientTag2"),
      TRINO_HEADERS.requestSession -> List(
        "",
        s"key0=${urlEncode("value0")}",
        s"key1=${urlEncode("value1")}",
        "badcase"),
      TRINO_HEADERS.requestPreparedStatement -> List(
        "badcase",
        s"key0=${urlEncode("select 1")}",
        s"key1=${urlEncode("select 2")}",
        ""))
    val expectedTrinoContext = new TrinoContext(
      user = "requestUser",
      timeZone = Some(ZoneId.systemDefault().getId),
      clientCapabilities = Some("requestClientCapabilities"),
      source = Some("requestSource"),
      catalog = Some("requestCatalog"),
      schema = Some("requestSchema"),
      language = Some("requestLanguage"),
      traceToken = Some("requestTraceToken"),
      clientInfo = Some("requestClientInfo"),
      clientTags = Set("requestClientTag1", "requestClientTag2"),
      session = Map("key0" -> "value0", "key1" -> "value1"),
      preparedStatement = Map("key0" -> "select 1", "key1" -> "select 2"))
    val actual = TrinoContext(testHeader0)
    assert(actual == expectedTrinoContext)
  }

  test("test convert") {
    val opHandle = getOpHandle("select 1")
    val opHandleStr = opHandle.identifier.toString
    checkOpState(opHandleStr, FINISHED)

    val metadataResp = fe.be.getResultSetMetadata(opHandle)
    val tRowSet = fe.be.fetchResults(opHandle, FetchOrientation.FETCH_NEXT, 1000, false).getResults
    val status = fe.be.getOperationStatus(opHandle, Some(0))

    val uri = new URI("sfdsfsdfdsf")
    val results = TrinoContext
      .createQueryResults("/xdfd/xdf", uri, uri, status, Option(metadataResp), Option(tRowSet))

    print(results.toString)
    assert(results.getColumns.get(0).getType.equals("integer"))
    assert(results.getData.asScala.last.get(0) == 1)
  }

  test("test convert from table") {
    initSql("CREATE DATABASE IF NOT EXISTS INIT_DB")
    initSql(
      "CREATE TABLE IF NOT EXISTS INIT_DB.test(a int, b double, c String," +
        "d BOOLEAN,e DATE,f TIMESTAMP,g ARRAY<String>,h DECIMAL," +
        "i MAP<String,String>) USING PARQUET;")
    initSql(
      "INSERT INTO INIT_DB.test VALUES (1,2.2,'3',true,current_date()," +
        "current_timestamp(),array('1','2'),2.0, map('m','p') )")

    val opHandle = getOpHandle("SELECT * FROM INIT_DB.test")
    val opHandleStr = opHandle.identifier.toString
    checkOpState(opHandleStr, FINISHED)

    val metadataResp = fe.be.getResultSetMetadata(opHandle)
    val tRowSet = fe.be.fetchResults(opHandle, FetchOrientation.FETCH_NEXT, 1000, false).getResults
    val status = fe.be.getOperationStatus(opHandle, Some(0))

    val uri = new URI("sfdsfsdfdsf")
    val results = TrinoContext
      .createQueryResults("/xdfd/xdf", uri, uri, status, Option(metadataResp), Option(tRowSet))

    print(results.toString)
    assert(results.getColumns.get(0).getType.equals("integer"))
    assert(results.getData.asScala.last.get(0) != null)
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
      fe.be.executeStatement(sessionHandle, statement, Map.empty, runAsync = false, 30000)
    } else {
      fe.be.getCatalogs(sessionHandle)
    }
  }

  private def checkOpState(opHandleStr: String, state: OperationState): Unit = {
    eventually(Timeout(30.seconds)) {
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
