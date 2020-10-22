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

package org.apache.kyuubi.service

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.{OperationHandle, OperationType}
import org.apache.kyuubi.service.FrontendService.{FeServiceServerContext, SERVER_VERSION}
import org.apache.kyuubi.service.authentication.PlainSASLHelper
import org.apache.kyuubi.session.SessionHandle

class FrontendServiceSuite extends KyuubiFunSuite {

  private val server = new NoopServer()
  private val conf = KyuubiConf()
    .set(KyuubiConf.FRONTEND_BIND_PORT, 0)
  server.initialize(conf)
  server.start()

  val user: String = System.getProperty("user.name")

  override def afterAll(): Unit = {
    super.afterAll()
    server.stop()
  }

  protected def withThriftClient(f: TCLIService.Iface => Unit): Unit = {
    val hostAndPort = server.connectionUrl.split(":")
    val host = hostAndPort.head
    val port = hostAndPort(1).toInt
    val socket = new TSocket(host, port)
    val transport = PlainSASLHelper.getPlainTransport(Utils.currentUser, "anonymous", socket)

    val protocol = new TBinaryProtocol(transport)
    val client = new TCLIService.Client(protocol)
    transport.open()
    try {
      f(client)
    } finally {
      socket.close()
    }
  }

  protected def withSessionHandle(f: (TCLIService.Iface, TSessionHandle) => Unit): Unit = {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername(user)
      req.setPassword("anonymous")
      val resp = client.OpenSession(req)
      val handle = resp.getSessionHandle

      try {
        f(client, handle)
      } finally {
        val tCloseSessionReq = new TCloseSessionReq(handle)
        try {
          client.CloseSession(tCloseSessionReq)
        } catch {
          case e: Exception => error(s"Failed to close $handle", e)
        }
      }
    }
  }

  private def checkOperationResult(
      client: TCLIService.Iface, handle: TOperationHandle): Unit = {
    val tFetchResultsReq = new TFetchResultsReq()
    tFetchResultsReq.setOperationHandle(handle)
    tFetchResultsReq.setFetchType(0)
    tFetchResultsReq.setOrientation(TFetchOrientation.FETCH_NEXT)
    tFetchResultsReq.setMaxRows(10)
    val resp = client.FetchResults(tFetchResultsReq)
    val expected = handle.getOperationType.toString
    val actual = resp.getResults.getColumns.get(0).getStringVal.getValues.get(0)
    assert(actual === expected)
  }

  test("open session") {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername(user)
      req.setPassword("anonymous")
      val resp = client.OpenSession(req)
      val handle = resp.getSessionHandle
      assert(handle != null)
      assert(resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)

      req.setConfiguration(Map("kyuubi.test.should.fail" -> "true").asJava)
      val resp1 = client.OpenSession(req)
      assert(resp1.getSessionHandle === null)
      assert(resp1.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      val cause = KyuubiSQLException.toCause(resp1.getStatus.getInfoMessages.asScala)
      assert(cause.isInstanceOf[KyuubiSQLException])
      assert(cause.getMessage === "Asked to fail")
    }
  }

  test("fe service server context") {
    withSessionHandle { (_, handle) =>
      val context = new FeServiceServerContext()
      val handle1 = SessionHandle(handle)
      context.setSessionHandle(handle1)
      assert(context.getSessionHandle.toTSessionHandle === handle)
    }
  }

  test("get info") {
    withSessionHandle { (client, handle) =>
      val req = new TGetInfoReq()
      req.setSessionHandle(handle)
      req.setInfoType(TGetInfoType.CLI_DBMS_VER)
      val resp = client.GetInfo(req)
      assert(resp.getInfoValue.getStringValue === org.apache.kyuubi.KYUUBI_VERSION)
      req.setInfoType(TGetInfoType.CLI_SERVER_NAME)
      assert(client.GetInfo(req).getInfoValue.getStringValue === "Kyuubi")
      req.setInfoType(TGetInfoType.CLI_DBMS_NAME)
      assert(client.GetInfo(req).getInfoValue.getStringValue === "Spark SQL")
      req.setInfoType(TGetInfoType.CLI_MAX_COLUMN_NAME_LEN)
      assert(client.GetInfo(req).getInfoValue.getLenValue === 128)
      req.setInfoType(TGetInfoType.CLI_MAX_SCHEMA_NAME_LEN)
      assert(client.GetInfo(req).getInfoValue.getLenValue === 128)
      req.setInfoType(TGetInfoType.CLI_MAX_TABLE_NAME_LEN)
      assert(client.GetInfo(req).getInfoValue.getLenValue === 128)
      req.setInfoType(TGetInfoType.CLI_ACCESSIBLE_PROCEDURES)
      val resp1 = client.GetInfo(req)
      assert(resp1.getInfoValue.getLenValue === 0)
      assert(resp1.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp1.getStatus.getErrorMessage ===
        "Unrecognized GetInfoType value: CLI_ACCESSIBLE_PROCEDURES")
      req.setInfoType(TGetInfoType.CLI_USER_NAME)
      val resp2 = client.GetInfo(req)
      assert(resp2.getInfoValue.getLenValue === 0)
      assert(resp2.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp2.getStatus.getErrorMessage ===
        "Unrecognized GetInfoType value: CLI_USER_NAME")
    }
  }

  test("get catalogs") {
    withSessionHandle { (client, handle) =>
      val req = new TGetCatalogsReq(handle)
      val resp = client.GetCatalogs(req)
      val opHandle = resp.getOperationHandle
      assert(opHandle.getOperationType === TOperationType.GET_CATALOGS)
      assert(resp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      checkOperationResult(client, opHandle)
      req.setSessionHandle(SessionHandle(SERVER_VERSION).toTSessionHandle)

      val resp1 = client.GetCatalogs(req)
      assert(resp1.getOperationHandle === null)
      assert(resp1.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp1.getStatus.getSqlState === null)
      assert(resp1.getStatus.getErrorMessage startsWith "Invalid SessionHandle")
    }
  }

  test("get schemas") {
    withSessionHandle { (client, handle) =>
      val req = new TGetSchemasReq(handle)
      val resp = client.GetSchemas(req)
      val opHandle = resp.getOperationHandle
      assert(opHandle.getOperationType === TOperationType.GET_SCHEMAS)
      assert(resp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      checkOperationResult(client, opHandle)

      req.setSessionHandle(SessionHandle(SERVER_VERSION).toTSessionHandle)
      val resp1 = client.GetSchemas(req)
      assert(resp1.getOperationHandle === null)
      assert(resp1.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp1.getStatus.getSqlState === null)
      assert(resp1.getStatus.getErrorMessage startsWith "Invalid SessionHandle")
    }
  }

  test("get tables") {
    withSessionHandle { (client, handle) =>
      val req = new TGetTablesReq(handle)
      val resp = client.GetTables(req)
      val opHandle = resp.getOperationHandle
      assert(opHandle.getOperationType === TOperationType.GET_TABLES)
      assert(resp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      checkOperationResult(client, opHandle)

      req.setSessionHandle(SessionHandle(SERVER_VERSION).toTSessionHandle)
      val resp1 = client.GetTables(req)
      assert(resp1.getOperationHandle === null)
      assert(resp1.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp1.getStatus.getSqlState === null)
      assert(resp1.getStatus.getErrorMessage startsWith "Invalid SessionHandle")
    }
  }

  test("get table types") {
    withSessionHandle { (client, handle) =>
      val req = new TGetTableTypesReq(handle)
      val resp = client.GetTableTypes(req)
      val opHandle = resp.getOperationHandle
      assert(opHandle.getOperationType === TOperationType.GET_TABLE_TYPES)
      assert(resp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      checkOperationResult(client, opHandle)

      req.setSessionHandle(SessionHandle(SERVER_VERSION).toTSessionHandle)
      val resp1 = client.GetTableTypes(req)
      assert(resp1.getOperationHandle === null)
      assert(resp1.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp1.getStatus.getSqlState === null)
      assert(resp1.getStatus.getErrorMessage startsWith "Invalid SessionHandle")
    }
  }

  test("get columns") {
    withSessionHandle { (client, handle) =>
      val req = new TGetColumnsReq(handle)
      val resp = client.GetColumns(req)
      val opHandle = resp.getOperationHandle
      assert(opHandle.getOperationType === TOperationType.GET_COLUMNS)
      assert(resp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      checkOperationResult(client, opHandle)

      req.setSessionHandle(SessionHandle(SERVER_VERSION).toTSessionHandle)
      val resp1 = client.GetColumns(req)
      assert(resp1.getOperationHandle === null)
      assert(resp1.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp1.getStatus.getSqlState === null)
      assert(resp1.getStatus.getErrorMessage startsWith "Invalid SessionHandle")
    }
  }

  test("get functions") {
    withSessionHandle { (client, handle) =>
      val req = new TGetFunctionsReq(handle, "sum")
      val resp = client.GetFunctions(req)
      val opHandle = resp.getOperationHandle
      assert(opHandle.getOperationType === TOperationType.GET_FUNCTIONS)
      assert(resp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      checkOperationResult(client, opHandle)

      req.setSessionHandle(SessionHandle(SERVER_VERSION).toTSessionHandle)
      val resp1 = client.GetFunctions(req)
      assert(resp1.getOperationHandle === null)
      assert(resp1.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp1.getStatus.getSqlState === null)
      assert(resp1.getStatus.getErrorMessage startsWith "Invalid SessionHandle")
    }
  }

  test("get primary keys") {
    withSessionHandle { (client, handle) =>
      val req = new TGetPrimaryKeysReq(handle)
      val resp = client.GetPrimaryKeys(req)
      assert(resp.getOperationHandle === null)
      assert(resp.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp.getStatus.getSqlState === null)
      assert(resp.getStatus.getErrorMessage startsWith "Feature is not available")
    }
  }


  test("get cross reference") {
    withSessionHandle { (client, handle) =>
      val req = new TGetCrossReferenceReq(handle)
      val resp = client.GetCrossReference(req)
      assert(resp.getOperationHandle === null)
      assert(resp.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp.getStatus.getSqlState === null)
      assert(resp.getStatus.getErrorMessage startsWith "Feature is not available")
    }
  }

  test("get operation status") {
    withSessionHandle { (client, handle) =>
      val opHandle =
        OperationHandle(OperationType.EXECUTE_STATEMENT, SERVER_VERSION)
      val req = new TGetOperationStatusReq(opHandle)
      val resp = client.GetOperationStatus(req)
      assert(resp.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp.getStatus.getSqlState === null)
      assert(resp.getStatus.getErrorMessage startsWith "Invalid OperationHandle")

      val req1 = new TGetTablesReq(handle)
      val resp1 = client.GetTables(req1)
      val opHandle1 = resp1.getOperationHandle
      val req2 = new TGetOperationStatusReq(opHandle1)
      val resp2 = client.GetOperationStatus(req2)
      assert(resp2.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      assert(resp2.getOperationState === TOperationState.FINISHED_STATE)

      req1.setSchemaName("invalid")
      val resp3 = client.GetTables(req1)
      req2.setOperationHandle(resp3.getOperationHandle)
      val resp4 = client.GetOperationStatus(req2)
      assert(resp4.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      assert(resp4.getOperationState === TOperationState.ERROR_STATE)
      assert(resp4.getErrorMessage === "noop operation err")

    }
  }
}
