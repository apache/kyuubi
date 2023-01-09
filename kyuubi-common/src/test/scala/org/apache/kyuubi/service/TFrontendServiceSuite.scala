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

import java.time.Duration

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift._
import org.scalatest.time._

import org.apache.kyuubi.{KyuubiFunSuite, KyuubiSQLException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.operation.{OperationHandle, TClientTestUtils}
import org.apache.kyuubi.service.TFrontendService.FeServiceServerContext
import org.apache.kyuubi.session.{AbstractSession, SessionHandle}

class TFrontendServiceSuite extends KyuubiFunSuite {

  protected val server = new NoopTBinaryFrontendServer()
  protected val conf: KyuubiConf = KyuubiConf()
    .set(KyuubiConf.FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    .set("kyuubi.test.server.should.fail", "false")
    .set(KyuubiConf.SESSION_CHECK_INTERVAL, Duration.ofSeconds(5).toMillis)
    .set(KyuubiConf.SESSION_IDLE_TIMEOUT, Duration.ofSeconds(5).toMillis)
    .set(KyuubiConf.OPERATION_IDLE_TIMEOUT, Duration.ofSeconds(20).toMillis)
    .set(KyuubiConf.SESSION_CONF_RESTRICT_LIST, Seq("spark.*"))
    .set(KyuubiConf.SESSION_CONF_IGNORE_LIST, Seq("session.engine.*"))

  private def withSessionHandle(f: (TCLIService.Iface, TSessionHandle) => Unit): Unit = {
    TClientTestUtils.withSessionHandle(server.frontendServices.head.connectionUrl, Map.empty)(f)
  }

  override def beforeAll(): Unit = {
    server.initialize(conf)
    server.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    server.getServices.foreach(_.stop())
  }

  private def checkOperationResult(
      client: TCLIService.Iface,
      handle: TOperationHandle): Unit = {
    val tFetchResultsReq = new TFetchResultsReq()
    tFetchResultsReq.setOperationHandle(handle)
    tFetchResultsReq.setFetchType(0)
    tFetchResultsReq.setOrientation(TFetchOrientation.FETCH_NEXT)
    tFetchResultsReq.setMaxRows(10)
    val resp = client.FetchResults(tFetchResultsReq)
    val expected = "NoopOperation"
    val actual = resp.getResults.getColumns.get(0).getStringVal.getValues.get(0)
    assert(actual === expected)
    tFetchResultsReq.setFetchType(1)
    val resp1 = client.FetchResults(tFetchResultsReq)
    assert(resp1.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
    assert(resp1.getResults.getColumnCount === 0)
    val invalidHandle = OperationHandle().toTOperationHandle
    invalidHandle.setOperationType(TOperationType.EXECUTE_STATEMENT)
    tFetchResultsReq.setOperationHandle(invalidHandle)
    val errResp = client.FetchResults(tFetchResultsReq)
    errResp.getStatus
    assert(errResp.getResults === null)
    assert(errResp.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
  }

  test("engine connect url use hostname") {

    def newService: TBinaryFrontendService = {
      new TBinaryFrontendService("DummyThriftBinaryFrontendService") {
        override val serverable: Serverable = new NoopTBinaryFrontendServer
        override val discoveryService: Option[Service] = None
      }
    }
    val conf = new KyuubiConf()
      .set(FRONTEND_THRIFT_BINARY_BIND_HOST.key, "localhost")
      .set(FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    val service = newService
    intercept[IllegalStateException](service.connectionUrl)

    conf.set(FRONTEND_CONNECTION_URL_USE_HOSTNAME, false)
    service.initialize(conf)
    // use what user want
    assert(service.connectionUrl.startsWith("localhost"))
    conf.set(FRONTEND_CONNECTION_URL_USE_HOSTNAME, true)
    assert(service.connectionUrl.startsWith("localhost"))

    val service2 = newService
    val conf2 = KyuubiConf()
      .set(FRONTEND_CONNECTION_URL_USE_HOSTNAME, false)
      .set(FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
      .unset(FRONTEND_THRIFT_BINARY_BIND_HOST)
      .unset(FRONTEND_BIND_HOST)
    service2.initialize(conf2)
    // use ip
    assert(service2.connectionUrl.split("\\.")(0).toInt > 0)
  }

  test("open session") {
    TClientTestUtils.withThriftClient(server.frontendServices.head) {
      client =>
        val req = new TOpenSessionReq()
        req.setUsername(Utils.currentUser)
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

        assert(resp1.getStatus.getErrorMessage === "Asked to fail")

    }
  }

  test("close session") {
    withSessionHandle { (client, handle) =>
      val req = new TCloseSessionReq(handle)
      val resp = client.CloseSession(req)
      assert(resp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      val resp1 = client.CloseSession(req)
      assert(resp1.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      val t = KyuubiSQLException.toCause(resp1.getStatus.getInfoMessages.asScala)
      assert(t.isInstanceOf[KyuubiSQLException])
      assert(resp1.getStatus.getErrorMessage === t.getMessage)
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
      assert(client.GetInfo(req).getInfoValue.getStringValue === org.apache.kyuubi.KYUUBI_VERSION)
      req.setInfoType(TGetInfoType.CLI_SERVER_NAME)
      assert(client.GetInfo(req).getInfoValue.getStringValue === "Apache Kyuubi")
      req.setInfoType(TGetInfoType.CLI_DBMS_NAME)
      assert(client.GetInfo(req).getInfoValue.getStringValue === "Apache Kyuubi")
      req.setInfoType(TGetInfoType.CLI_ODBC_KEYWORDS)
      assert(client.GetInfo(req).getInfoValue.getStringValue === "Unimplemented")
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
      req.setSessionHandle(SessionHandle().toTSessionHandle)

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

      req.setSessionHandle(SessionHandle().toTSessionHandle)
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

      req.setSessionHandle(SessionHandle().toTSessionHandle)
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

      req.setSessionHandle(SessionHandle().toTSessionHandle)
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

      req.setSessionHandle(SessionHandle().toTSessionHandle)
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

      req.setSessionHandle(SessionHandle().toTSessionHandle)
      val resp1 = client.GetFunctions(req)
      assert(resp1.getOperationHandle === null)
      assert(resp1.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp1.getStatus.getSqlState === null)
      assert(resp1.getStatus.getErrorMessage startsWith "Invalid SessionHandle")
    }
  }

  test("get type info") {
    withSessionHandle { (client, handle) =>
      val req = new TGetTypeInfoReq()
      req.setSessionHandle(handle)
      val resp = client.GetTypeInfo(req)
      val opHandle = resp.getOperationHandle
      assert(opHandle.getOperationType === TOperationType.GET_TYPE_INFO)
      assert(resp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      checkOperationResult(client, opHandle)

      req.setSessionHandle(SessionHandle().toTSessionHandle)
      val resp1 = client.GetTypeInfo(req)
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
      val opHandle = resp.getOperationHandle
      assert(opHandle.getOperationType === TOperationType.GET_FUNCTIONS)
      assert(resp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      checkOperationResult(client, opHandle)

      req.setSessionHandle(SessionHandle().toTSessionHandle)
      val resp1 = client.GetPrimaryKeys(req)
      assert(resp1.getOperationHandle === null)
      assert(resp1.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp1.getStatus.getSqlState === null)
      assert(resp1.getStatus.getErrorMessage startsWith "Invalid SessionHandle")
    }
  }

  test("get cross reference") {
    withSessionHandle { (client, handle) =>
      val req = new TGetCrossReferenceReq(handle)
      val resp = client.GetCrossReference(req)
      val opHandle = resp.getOperationHandle
      assert(opHandle.getOperationType === TOperationType.GET_FUNCTIONS)
      assert(resp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      checkOperationResult(client, opHandle)

      req.setSessionHandle(SessionHandle().toTSessionHandle)
      val resp1 = client.GetCrossReference(req)
      assert(resp1.getOperationHandle === null)
      assert(resp1.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp1.getStatus.getSqlState === null)
      assert(resp1.getStatus.getErrorMessage startsWith "Invalid SessionHandle")
    }
  }

  test("get query id") {
    withSessionHandle { (client, handle) =>
      val req = new TExecuteStatementReq()
      req.setStatement("select 1")
      req.setSessionHandle(handle)
      req.setRunAsync(false)
      val resp = client.ExecuteStatement(req)
      val opHandle = resp.getOperationHandle
      val req1 = new TGetQueryIdReq(opHandle)
      val resp1 = client.GetQueryId(req1)
      assert(resp1.getQueryId === "noop_query_id")
    }
  }

  test("get operation status") {
    withSessionHandle { (client, handle) =>
      val opHandle = OperationHandle().toTOperationHandle
      opHandle.setOperationType(TOperationType.GET_TABLES)
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
      assert(resp4.getErrorMessage startsWith "org.apache.kyuubi.KyuubiSQLException:" +
        " noop operation err\n\tat org.apache.kyuubi.KyuubiSQLException")
    }
  }

  test("execute statement") {
    withSessionHandle { (client, handle) =>
      val req = new TExecuteStatementReq()
      req.setStatement("select 1")
      req.setSessionHandle(handle)
      req.setRunAsync(false)
      val resp = client.ExecuteStatement(req)
      val opHandle = resp.getOperationHandle
      assert(opHandle.getOperationType === TOperationType.EXECUTE_STATEMENT)
      assert(resp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      checkOperationResult(client, opHandle)
      req.setRunAsync(true)
      val resp1 = client.ExecuteStatement(req)
      val opHandle1 = resp1.getOperationHandle
      checkOperationResult(client, opHandle1)

      req.setSessionHandle(SessionHandle().toTSessionHandle)
      val resp2 = client.ExecuteStatement(req)
      assert(resp2.getOperationHandle === null)
      assert(resp2.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp2.getStatus.getSqlState === null)
      assert(resp2.getStatus.getErrorMessage startsWith "Invalid SessionHandle")
    }
  }

  test("cancel operation") {
    withSessionHandle { (client, handle) =>
      val opHandle = OperationHandle().toTOperationHandle
      opHandle.setOperationType(TOperationType.EXECUTE_STATEMENT)
      val req = new TCancelOperationReq()
      req.setOperationHandle(opHandle)
      val resp = client.CancelOperation(req)
      assert(resp.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp.getStatus.getSqlState === null)
      assert(resp.getStatus.getErrorMessage startsWith "Invalid OperationHandle")

      val req1 = new TGetSchemasReq(handle)
      val resp1 = client.GetSchemas(req1)
      resp1.getOperationHandle
      req.setOperationHandle(resp1.getOperationHandle)
      val resp2 = client.CancelOperation(req)
      assert(resp2.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
    }
  }

  test("close operation") {
    withSessionHandle { (client, handle) =>
      val opHandle = OperationHandle().toTOperationHandle
      opHandle.setOperationType(TOperationType.EXECUTE_STATEMENT)
      val req = new TCloseOperationReq()
      req.setOperationHandle(opHandle)
      val resp = client.CloseOperation(req)
      assert(resp.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp.getStatus.getSqlState === null)
      assert(resp.getStatus.getErrorMessage startsWith "Invalid OperationHandle")

      val req1 = new TGetSchemasReq(handle)
      val resp1 = client.GetSchemas(req1)
      resp1.getOperationHandle
      req.setOperationHandle(resp1.getOperationHandle)
      val resp2 = client.CloseOperation(req)
      assert(resp2.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
    }
  }

  test("get result set meta data") {
    withSessionHandle { (client, handle) =>
      val req = new TGetSchemasReq(handle)
      req.setSessionHandle(handle)
      val operationHandle = client.GetSchemas(req).getOperationHandle
      val req1 = new TGetResultSetMetadataReq(operationHandle)
      val resp = client.GetResultSetMetadata(req1)
      assert(resp.getSchema.getColumns.get(0).getColumnName === "noop")
      assert(resp.getSchema.getColumns.get(0).getComment === "comment")
      assert(resp.getSchema.getColumns.get(0).getPosition === 0)

      val handle1 = OperationHandle().toTOperationHandle
      handle1.setOperationType(TOperationType.GET_SCHEMAS)
      req1.setOperationHandle(handle1)
      val resp1 = client.GetResultSetMetadata(req1)
      assert(resp1.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(resp1.getStatus.getSqlState === null)
      assert(resp1.getStatus.getErrorMessage startsWith "Invalid OperationHandle")
    }
  }

  test("Delegation token is supported by default") {
    withSessionHandle { (client, handle) =>
      val tGetDelegationTokenReq = new TGetDelegationTokenReq()
      tGetDelegationTokenReq.setSessionHandle(handle)
      tGetDelegationTokenReq.setOwner(Utils.currentUser)
      tGetDelegationTokenReq.setRenewer(Utils.currentUser)
      val tGetDelegationTokenResp = client.GetDelegationToken(tGetDelegationTokenReq)
      assert(tGetDelegationTokenResp.getDelegationToken === null)
      assert(tGetDelegationTokenResp.getStatus.getErrorMessage ===
        "Delegation token is not supported")

      val tCancelDelegationTokenReq = new TCancelDelegationTokenReq()
      tCancelDelegationTokenReq.setSessionHandle(handle)
      tCancelDelegationTokenReq.setDelegationToken("")
      val tCancelDelegationTokenResp = client.CancelDelegationToken(tCancelDelegationTokenReq)
      assert(tCancelDelegationTokenResp.getStatus.getErrorMessage ===
        "Delegation token is not supported")

      val tRenewDelegationTokenReq = new TRenewDelegationTokenReq()
      tRenewDelegationTokenReq.setSessionHandle(handle)
      tRenewDelegationTokenReq.setDelegationToken("")
      val tRenewDelegationTokenResp = client.RenewDelegationToken(tRenewDelegationTokenReq)
      assert(tRenewDelegationTokenResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
    }
  }

  test("close expired operations") {
    withSessionHandle { (client, handle) =>
      val req = new TCancelOperationReq()
      val req1 = new TGetSchemasReq(handle)
      val resp1 = client.GetSchemas(req1)

      val sessionManager = server.backendService.sessionManager
      val session = sessionManager
        .getSession(SessionHandle(handle))
        .asInstanceOf[AbstractSession]
      var lastAccessTime = session.lastAccessTime
      assert(sessionManager.getOpenSessionCount == 1)
      assert(session.lastIdleTime > 0)

      resp1.getOperationHandle
      req.setOperationHandle(resp1.getOperationHandle)
      val resp2 = client.CancelOperation(req)
      assert(resp2.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      assert(sessionManager.getOpenSessionCount == 1)
      assert(session.lastIdleTime == 0)
      assert(lastAccessTime < session.lastAccessTime)
      lastAccessTime = session.lastAccessTime

      eventually(timeout(Span(60, Seconds)), interval(Span(1, Seconds))) {
        assert(session.lastIdleTime > lastAccessTime)
      }

      info("operation is terminated")
      assert(lastAccessTime == session.lastAccessTime)
      assert(sessionManager.getOpenSessionCount == 1)

      eventually(timeout(Span(60, Seconds)), interval(Span(1, Seconds))) {
        assert(session.lastAccessTime > lastAccessTime)
      }
      assert(sessionManager.getOpenSessionCount == 0)
    }
  }

  test("test validate and normalize config") {
    val sessionManager = server.backendService.sessionManager
    // test restrict
    intercept[KyuubiSQLException] {
      sessionManager.validateAndNormalizeConf(Map("spark.driver.memory" -> "2G"))
    }

    // test ignore
    val conf = sessionManager.validateAndNormalizeConf(
      Map(
        "session.engine.spark.main.resource" -> "org.apahce.kyuubi.test",
        "session.check.interval" -> "10000"))
    assert(conf.size == 1)
    assert(conf("session.check.interval") == "10000")
  }
}
