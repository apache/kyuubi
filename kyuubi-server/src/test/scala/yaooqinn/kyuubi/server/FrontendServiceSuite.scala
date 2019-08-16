/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.server

import java.net.InetAddress

import scala.collection.JavaConverters._

import org.apache.hive.service.cli.thrift._
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.apache.spark.KyuubiConf._
import org.scalatest.Matchers

import yaooqinn.kyuubi.{KyuubiSQLException, SecuredFunSuite}
import yaooqinn.kyuubi.metrics.MetricsSystem
import yaooqinn.kyuubi.operation.OperationHandle
import yaooqinn.kyuubi.service.{ServiceException, State}
import yaooqinn.kyuubi.session.SessionHandle

class FrontendServiceSuite extends SparkFunSuite with Matchers with SecuredFunSuite {

  private val server: KyuubiServer = new KyuubiServer()
  private val user = KyuubiSparkUtil.getCurrentUserName
  private val conf = new SparkConf(loadDefaults = true).setAppName("fe test")
  KyuubiSparkUtil.setupCommonConfig(conf)
  conf.remove(KyuubiSparkUtil.CATALOG_IMPL)
  conf.setMaster("local").set(FRONTEND_BIND_PORT.key, "0")

  server.init(conf)
  server.start()

  override def afterAll(): Unit = {
    Option(server).foreach(_.stop())
    super.afterAll()
  }

  test("new frontend service") {
    val feService = new FrontendService(server.beService)
    feService.getConf should be(null)
    feService.getStartTime should be(0)
    feService.getServiceState should be(State.NOT_INITED)
    feService.getName should be(classOf[FrontendService].getSimpleName)
    feService.getServerIPAddress should be(null)
    feService.getPortNumber should be(0)
  }

  test("init fe service") {
    val feService = new FrontendService(server.beService)
    feService.init(conf)
    feService.getConf should be(conf)
    feService.getServiceState should be(State.INITED)
    feService.getPortNumber should not be 0
    val conf1 = new SparkConf(loadDefaults = true)
      .set(FRONTEND_BIND_HOST.key, "")
      .set(FRONTEND_BIND_PORT.key, "10009")
    val feService2 = new FrontendService(server.beService)
    feService2.init(conf1)
    feService2.getServerIPAddress should be(InetAddress.getLocalHost)
    intercept[ServiceException](
      feService2.init(conf1)).getMessage should include("10009")
  }

  test("start fe service") {
    val feService = new FrontendService(server.beService)
    intercept[IllegalStateException](feService.start())
    feService.init(conf)
    feService.start()
    feService.getConf should be(conf)
    feService.getStartTime should not be 0
    feService.getServiceState should be(State.STARTED)
  }

  test("stop fe service") {
    val feService = new FrontendService(server.beService)
    feService.stop()
    feService.getServiceState should be(State.NOT_INITED)
    feService.init(conf)
    feService.stop()
    feService.getServiceState should be(State.INITED)
    feService.start()
    feService.stop()
    feService.getServiceState should be(State.STOPPED)
  }

  test("get delegation token") {
    withFEServiceAndHandle { case (fe, handle) =>
      val resp = fe.GetDelegationToken(new TGetDelegationTokenReq(handle, user, user))
      resp.getStatus.getErrorMessage should startWith("Delegation token")
    }
  }

  test("get catalogs") {
    withFEServiceAndHandle { case (fe, handle) =>
      val req = new TGetCatalogsReq(handle)
      val resp = fe.GetCatalogs(req)
      val req2 = new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      val resp2 = fe.FetchResults(req2)
      val rows = resp2.getResults.getRows
      rows.size() should be(0)
      val closeReq = new TCloseSessionReq(handle)
      fe.CloseSession(closeReq)
      val afterCloseResp = fe.GetCatalogs(req)
      afterCloseResp.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
    }
  }

  test("get table types") {
    withFEServiceAndHandle { case (fe, handle) =>
      val req = new TGetTableTypesReq(handle)
      val resp = fe.GetTableTypes(req)
      val req2 = new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      val resp2 = fe.FetchResults(req2)
      val rows = resp2.getResults.getRows
      rows.get(0).getColVals.get(0).getStringVal.getValue should be("MANAGED")
      rows.get(1).getColVals.get(0).getStringVal.getValue should be("VIEW")
      rows.get(2).getColVals.get(0).getStringVal.getValue should be("EXTERNAL")
      val closeReq = new TCloseSessionReq(handle)
      fe.CloseSession(closeReq)
      val afterCloseResp = fe.GetTableTypes(req)
      afterCloseResp.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
    }
  }

  test("get type info") {
    withFEServiceAndHandle { case (fe, handle) =>
      val req = new TGetTypeInfoReq(handle)
      val resp = fe.GetTypeInfo(req)
      val req2 = new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      val resp2 = fe.FetchResults(req2)
      val rows = resp2.getResults.getRows
      rows.size() should be(15)
      rows.get(0).getColVals.get(0).getStringVal.getValue should be("void")
      rows.get(1).getColVals.get(0).getStringVal.getValue should be("boolean")
      val closeReq = new TCloseSessionReq(handle)
      fe.CloseSession(closeReq)
      val afterCloseResp = fe.GetTypeInfo(req)
      afterCloseResp.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
    }
  }

  test("get functions") {
    withFEServiceAndHandle { case (fe, handle) =>
      val req = new TGetFunctionsReq(handle, null)
      val resp = fe.GetFunctions(req)
      val req2 = new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      val resp2 = fe.FetchResults(req2)
      val rows = resp2.getResults.getRows
      assert(rows.size() === 50)
      assert(rows.get(0).getColVals.get(2).getStringVal.getValue.nonEmpty)
      val closeReq = new TCloseSessionReq(handle)
      fe.CloseSession(closeReq)
      val afterCloseResp = fe.GetFunctions(req)
      afterCloseResp.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
    }
  }

  test("get schemas") {
    withFEServiceAndHandle { case (fe, handle) =>
      val schemasReq = new TGetSchemasReq(handle)
      schemasReq.setCatalogName("")
      schemasReq.setSchemaName("*")
      val resp = fe.GetSchemas(schemasReq)
      val req2 = new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      val resp2 = fe.FetchResults(req2)
      val rows = resp2.getResults.getRows
      rows.size() should be(1)
      rows.get(0).getColVals.get(0).getStringVal.getValue should be("default")
      val closeReq = new TCloseSessionReq(handle)
      fe.CloseSession(closeReq)
      val afterCloseResp = fe.GetSchemas(schemasReq)
      afterCloseResp.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
    }
  }

  test("get tables") {
    withFEServiceAndHandle { case (fe, handle) =>
      val req = new TGetTablesReq(handle)
      req.setCatalogName("")
      req.setSchemaName("*")
      req.setTableName("*")
      val kyuubiSession = server.beService.getSessionManager.getSession(new SessionHandle(handle))
      kyuubiSession.sparkSession.sql("create table get_tables(key int, value string) using parquet")
      val resp = fe.GetTables(req)
      val req2 = new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      val resp2 = fe.FetchResults(req2)
      val rows = resp2.getResults.getRows
      rows.get(0).getColVals.get(2).getStringVal.getValue should be("get_tables")

      // schema not match
      val req3 = new TGetTablesReq(handle)
      req3.setSchemaName("a")
      req3.setTableName("*")
      val resp3 = fe.GetTables(req3)
      val req4 = new TFetchResultsReq(resp3.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      val resp4 = fe.FetchResults(req4)
      val rows2 = resp4.getResults.getRows
      rows2.size() should be(0)

      // table name not match
      val req5 = new TGetTablesReq(handle)
      req5.setSchemaName("*")
      req5.setTableName("get_tables_2")
      val resp5 = fe.GetTables(req3)
      val req6 = new TFetchResultsReq(resp5.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      val resp6 = fe.FetchResults(req6)
      val rows3 = resp6.getResults.getRows
      rows3.size() should be(0)

      // table type not match
      val req7 = new TGetTablesReq(handle)
      req7.setCatalogName("")
      req7.setSchemaName("*")
      req7.setTableName("*")
      req7.setTableTypes(Seq("VIEW").asJava)
      val resp7 = fe.GetTables(req7)
      val req8 = new TFetchResultsReq(resp7.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      val resp8 = fe.FetchResults(req8)
      val rows4 = resp8.getResults.getRows
      rows4.size() should be(0)

      val closeReq = new TCloseSessionReq(handle)
      fe.CloseSession(closeReq)
      val afterCloseResp = fe.GetTables(req)
      afterCloseResp.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
    }
  }

  test("get columns") {
    withFEServiceAndHandle { case (fe, handle) =>
      val kyuubiSession = server.beService.getSessionManager.getSession(new SessionHandle(handle))
      val tableName = "get_columns"
      kyuubiSession.sparkSession
        .sql("create table " + tableName + "(key int, value string) using parquet")
      val req = new TGetColumnsReq(handle)
      req.setCatalogName("")
      req.setSchemaName("*")
      req.setTableName(tableName)
      req.setColumnName(null)
      val resp = fe.GetColumns(req)
      val operation = server.beService.getSessionManager.getOperationMgr
        .getOperation(new OperationHandle(resp.getOperationHandle))
      assert(!operation.isTimedOut)
      assert(!operation.shouldRunAsync)
      val req2 = new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      val resp2 = fe.FetchResults(req2)
      val rows = resp2.getResults.getRows
      intercept[KyuubiSQLException](operation.cancel())
      rows.size() should be(2)
      rows.get(0).getColVals.get(1).getStringVal.getValue should be("default")
      rows.get(0).getColVals.get(2).getStringVal.getValue should be(tableName)
      rows.get(0).getColVals.get(3).getStringVal.getValue should be("key")
      rows.get(0).getColVals.get(4).getI32Val.getValue should be(java.sql.Types.INTEGER)

      val req3 = new TGetColumnsReq(handle)
      req3.setCatalogName("")
      req3.setSchemaName("*")
      req3.setTableName(tableName)
      req3.setColumnName("key")
      val resp3 = fe.GetColumns(req3)
      val req4 = new TFetchResultsReq(resp3.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      val resp4 = fe.FetchResults(req4)
      val rows2 = resp4.getResults.getRows
      rows2.size() should be(1)

      val closeReq = new TCloseSessionReq(handle)
      fe.CloseSession(closeReq)
      val afterCloseResp = fe.GetColumns(req)
      afterCloseResp.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
    }
  }

  test("get port num") {
    val feService = new FrontendService(server.beService)
    feService.getPortNumber should be(0)
    feService.init(conf)
    feService.getPortNumber should not be 0
  }

  test("get server ip addr") {
    val feService = new FrontendService(server.beService)
    feService.getServerIPAddress should be(null)
    feService.init(conf)
    feService.getServerIPAddress should not be null
  }

  test("fe service server context") {
    withFEServiceAndHandle { case (fe, handle) =>
      val context = new fe.FeServiceServerContext()
      context.setSessionHandle(new SessionHandle(handle))
      context.getSessionHandle.toTSessionHandle should be(handle)
    }
  }

  test("fe tserver event handler") {
    withFEServiceAndHandle { case (fe, handle) =>
      val handler = new fe.FeTServerEventHandler
      val context = new fe.FeServiceServerContext()
      context.setSessionHandle(new SessionHandle(handle))
      handler.createContext(null, null)
      handler.processContext(context, null, null)
      handler.deleteContext(context, null, null)
    }
  }

  test("fe tserver event handler with metrics") {
    withFEServiceAndHandle { case (fe, handle) =>
      MetricsSystem.init(conf)
      val handler = new fe.FeTServerEventHandler
      val context = new fe.FeServiceServerContext()
      context.setSessionHandle(new SessionHandle(handle))
      handler.createContext(null, null)
      handler.processContext(context, null, null)
      handler.deleteContext(context, null, null)
      MetricsSystem.close()
    }
  }

  test("open session, execute sql and get results") {

    withFEServiceAndHandle { case (fe, handle) =>
      val req2 = new TExecuteStatementReq(handle, "show databases")
      val resp2 = fe.ExecuteStatement(req2)
      resp2.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      val req3 = new TGetOperationStatusReq(resp2.getOperationHandle)
      val resp3 = fe.GetOperationStatus(req3)
      resp3.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      while(fe.GetOperationStatus(req3)
        .getOperationState.getValue < TOperationState.FINISHED_STATE.getValue) {
        Thread.sleep(10)
      }
      Thread.sleep(2000)
      val req4 = new TFetchResultsReq(resp2.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      val resp4 = fe.FetchResults(req4)
      resp4.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      resp4.getResults.getRows.get(0).getColVals.get(0).getStringVal.getValue should be("default")
      assert(!resp4.getResults.isSetColumns)
      val req5 = new TGetResultSetMetadataReq(resp2.getOperationHandle)
      val resp5 = fe.GetResultSetMetadata(req5)
      resp5.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      resp5.getSchema.getColumns.get(0).getColumnName should be("databaseName")
      val req7 = new TCancelOperationReq(resp2.getOperationHandle)
      val resp7 = fe.CancelOperation(req7)
      resp7.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      val req6 = new TCloseOperationReq(resp2.getOperationHandle)
      val resp6 = fe.CloseOperation(req6)
      resp6.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      val req9 = new TCancelOperationReq(resp2.getOperationHandle)
      val resp9 = fe.CancelOperation(req9)
      resp9.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)

      val reqInfo1 = new TGetInfoReq(handle, TGetInfoType.CLI_DBMS_NAME)
      val respInfo1 = fe.GetInfo(reqInfo1)
      respInfo1.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)

      val reqInfo2 = new TGetInfoReq(handle, TGetInfoType.CLI_ACCESSIBLE_PROCEDURES)
      val respInfo2 = fe.GetInfo(reqInfo2)
      respInfo2.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
      respInfo2.getStatus.getErrorMessage should
        include(TGetInfoType.CLI_ACCESSIBLE_PROCEDURES.toString)

      val req8 = new TCloseSessionReq(handle)
      val resp8 = fe.CloseSession(req8)
      resp8.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)

      // after session closed
      val resp10 = fe.CloseSession(req8)
      resp10.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
      resp10.getStatus.getErrorMessage should include("does not exist!")
      val resp11 = fe.ExecuteStatement(req2)
      resp11.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
      fe.GetOperationStatus(req3).getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
      fe.FetchResults(req4).getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
      fe.GetResultSetMetadata(req5)
        .getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
      fe.CancelOperation(req7).getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
      fe.CloseOperation(req6).getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)

      val tOpenSessionReq = new TOpenSessionReq(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
      tOpenSessionReq.setUsername("yaooqinn")
      tOpenSessionReq.setPassword("passwd")
      tOpenSessionReq.setConfiguration(
        Map("hive.server2.proxy.user" -> "kent").asJava)
      val tOpenSessionResp = fe.OpenSession(tOpenSessionReq)
      tOpenSessionResp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
    }
  }

  test("execute statement sync") {
    withFEServiceAndHandle { case (fe, handle) =>
      val req = new TExecuteStatementReq(handle, "show databases")
      req.setRunAsync(false)
      val resp = fe.ExecuteStatement(req)
      val req2 = new TGetOperationStatusReq(resp.getOperationHandle)
      val statusResp = fe.GetOperationStatus(req2)
      statusResp.getOperationState should be(TOperationState.FINISHED_STATE)
      val fReq = new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      val fRes = fe.FetchResults(fReq)
      val rows = fRes.getResults.getRows
      rows.get(0).getColVals.get(0).getStringVal.getValue should be("default")
    }
  }

  test("execute statement async") {
    withFEServiceAndHandle { case (fe, handle) =>
      val req = new TExecuteStatementReq(handle, "show databases")
      req.setRunAsync(true)
      val resp = fe.ExecuteStatement(req)
      val statusReq = new TGetOperationStatusReq(resp.getOperationHandle)
      while(fe.GetOperationStatus(statusReq)
        .getOperationState.getValue < TOperationState.FINISHED_STATE.getValue) {
        Thread.sleep(10)
      }
      Thread.sleep(2000)
      val fReq = new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      val fRes = fe.FetchResults(fReq)
      val rows = fRes.getResults.getRows
      rows.get(0).getColVals.get(0).getStringVal.getValue should be("default")
    }
  }

  test("alter database") {
    val block: (FrontendService, TSessionHandle) => Unit = (fe, handle) => {
      val req = new TExecuteStatementReq(handle,
        "alter database default set dbproperties ('kent'='yao')")
      val resp = fe.ExecuteStatement(req)
      resp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)

      val tFetchResultsReq =
        new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      Thread.sleep(5000)
      val tFetchResultsResp = fe.FetchResults(tFetchResultsReq)
      tFetchResultsResp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      tFetchResultsResp.getResults.getRows.size() should be(0)
    }
    withFEServiceAndHandle(block)
    withFEServiceAndHandleInc(block)
    withFEServiceAndHandleIncAndCal(block)
  }

  test("alter schema") {
    val block: (FrontendService, TSessionHandle) => Unit = (fe, handle) => {
      val req = new TExecuteStatementReq(handle,
        "alter schema default set dbproperties ('kent'='yao')")
      val resp = fe.ExecuteStatement(req)
      resp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)

      val tFetchResultsReq =
        new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)

      Thread.sleep(5000)
      val tFetchResultsResp = fe.FetchResults(tFetchResultsReq)
      tFetchResultsResp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      tFetchResultsResp.getResults.getRows.size() should be(0)
    }
    withFEServiceAndHandle(block)
    withFEServiceAndHandleInc(block)
    withFEServiceAndHandleIncAndCal(block)
  }

  test("alter table name") {
    val block: (FrontendService, TSessionHandle) => Unit = (fe, handle) => {
      val ct = new TExecuteStatementReq(handle,
        "create table if not exists default.src(key int) using parquet")
      fe.ExecuteStatement(ct)
      Thread.sleep(5000)
      val req = new TExecuteStatementReq(handle, "alter table default.src rename to default.src2")
      val resp = fe.ExecuteStatement(req)
      resp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)

      val tFetchResultsReq =
        new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      val dt = new TExecuteStatementReq(handle, "drop table src2")
      fe.ExecuteStatement(dt)
      Thread.sleep(5000)
      val tFetchResultsResp = fe.FetchResults(tFetchResultsReq)
      tFetchResultsResp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      tFetchResultsResp.getResults.getRows.size() should be(0)
    }
    withFEServiceAndHandle(block)
  }

  test("alter table name inc") {
    val block: (FrontendService, TSessionHandle) => Unit = (fe, handle) => {
      val ct = new TExecuteStatementReq(handle,
        "create table if not exists default.src3(key int) using parquet")
      fe.ExecuteStatement(ct)
      Thread.sleep(5000)
      val req = new TExecuteStatementReq(handle, "alter table default.src3 rename to default.src4")
      val resp = fe.ExecuteStatement(req)
      resp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)

      val tFetchResultsReq =
        new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      val dt = new TExecuteStatementReq(handle, "drop table src4")
      fe.ExecuteStatement(dt)
      Thread.sleep(5000)
      val tFetchResultsResp = fe.FetchResults(tFetchResultsReq)
      tFetchResultsResp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      tFetchResultsResp.getResults.getRows.size() should be(0)
    }
    withFEServiceAndHandleInc(block)
  }

  test("alter table set properties") {
    val block: (FrontendService, TSessionHandle) => Unit = (fe, handle) => {
      val ct = new TExecuteStatementReq(handle, "create table default.src(key int) using parquet")
      fe.ExecuteStatement(ct)
      Thread.sleep(5000)
      val req = new TExecuteStatementReq(handle,
        "alter table default.src set tblproperties ('kent'='yao')")
      val resp = fe.ExecuteStatement(req)
      resp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)

      val tFetchResultsReq =
        new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)

      Thread.sleep(5000)
      val tFetchResultsResp = fe.FetchResults(tFetchResultsReq)
      tFetchResultsResp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      tFetchResultsResp.getResults.getRows.size() should be(0)
    }
    withFEServiceAndHandle(block)
    withFEServiceAndHandleInc(block)
  }

  test("alter table unset properties") {
    withFEServiceAndHandle { (fe, handle) =>
      val ct = new TExecuteStatementReq(handle,
        "create table default.src(key int) using parquet tblproperties ('kent'='yao')")
      fe.ExecuteStatement(ct)
      Thread.sleep(5000)
      val req = new TExecuteStatementReq(handle,
        "alter table default.src unset tblproperties if exists ('kent', 'yao')")
      val resp = fe.ExecuteStatement(req)
      resp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)

      val tFetchResultsReq =
        new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)

      Thread.sleep(5000)
      val tFetchResultsResp = fe.FetchResults(tFetchResultsReq)
      tFetchResultsResp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      tFetchResultsResp.getResults.getRows.size() should be(0)
    }
  }

  test("add jar hdfs") {
    withFEServiceAndHandle { (fe, handle) =>
      val req = new TExecuteStatementReq(handle, "add jar hdfs://a/b/test.jar")
      val resp = fe.ExecuteStatement(req)
      resp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)

      val tFetchResultsReq =
        new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)

      Thread.sleep(5000)
      val tFetchResultsResp = fe.FetchResults(tFetchResultsReq)
      tFetchResultsResp.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
    }
  }

  test("add jar local") {
    withFEServiceAndHandle { (fe, handle) =>
      val req = new TExecuteStatementReq(handle, "add jar file://a/b/test.jar")
      val resp = fe.ExecuteStatement(req)
      resp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)

      val tFetchResultsReq =
        new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)

      Thread.sleep(5000)
      val tFetchResultsResp = fe.FetchResults(tFetchResultsReq)
      tFetchResultsResp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      tFetchResultsResp.getResults.getRows.get(0).getColVals.get(0).getI32Val.getValue should be(0)
    }
  }

  test("create temporary function") {
    withFEServiceAndHandle { (fe, handle) =>
      val req = new TExecuteStatementReq(handle,
        "create temporary function testfunc as 'testClass' using jar 'hdfs://a/b/test.jar'")
      val resp = fe.ExecuteStatement(req)
      resp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)

      val tFetchResultsReq =
        new TFetchResultsReq(resp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)

      Thread.sleep(5000)
      val tFetchResultsResp = fe.FetchResults(tFetchResultsReq)
      tFetchResultsResp.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
    }
  }

  test("select") {
    val block: (FrontendService, TSessionHandle) => Unit = (fe, handle) => {
      val kyuubiSession = server.beService.getSessionManager.getSession(new SessionHandle(handle))
      kyuubiSession.sparkSession.sql(
        "create table if not exists default.select_tbl(key int) using parquet")
      val ct = new TExecuteStatementReq(handle, "select * from default.select_tbl")
      val tExecuteStatementResp = fe.ExecuteStatement(ct)
      val statusReq = new TGetOperationStatusReq(tExecuteStatementResp.getOperationHandle)

      while(fe.GetOperationStatus(statusReq)
        .getOperationState.getValue < TOperationState.FINISHED_STATE.getValue) {
        Thread.sleep(10)
      }
      Thread.sleep(2000)

      val tFetchResultsReq = new TFetchResultsReq(
        tExecuteStatementResp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)

      val tFetchResultsResp = fe.FetchResults(tFetchResultsReq)
      tFetchResultsResp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      tFetchResultsResp.getResults.getRows.size() should be(0)
    }
    withFEServiceAndHandleIncAndCal(block)
    withFEServiceAndHandleInc(block)
    withFEServiceAndHandle(block)
    withFEServiceAndHandleAndResultLimit(block)
  }

  test("select with exception") {
    val block: (FrontendService, TSessionHandle) => Unit = (fe, handle) => {
      val kyuubiSession = server.beService.getSessionManager.getSession(new SessionHandle(handle))
      kyuubiSession.sparkSession.sql(
        "create table if not exists default.select_tbl(key int) using parquet")
      val ct = new TExecuteStatementReq(handle, "select * from default.select_tbl")
      val tExecuteStatementResp = fe.ExecuteStatement(ct)
      val statusReq = new TGetOperationStatusReq(tExecuteStatementResp.getOperationHandle)

      while(fe.GetOperationStatus(statusReq)
        .getOperationState.getValue < TOperationState.FINISHED_STATE.getValue) {
        Thread.sleep(10)
      }
      Thread.sleep(2000)

      val tFetchResultsReq = new TFetchResultsReq(
        tExecuteStatementResp.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)

      val tFetchResultsResp = fe.FetchResults(tFetchResultsReq)
      tFetchResultsResp.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
    }

    withFEServiceAndHandleAndException(block)
  }

  def withFEServiceAndHandle(block: (FrontendService, TSessionHandle) => Unit): Unit = {
    val feService = server.feService
    val req = new TOpenSessionReq(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
    req.setUsername(user)
    val resp = feService.OpenSession(req)
    resp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
    val handle = resp.getSessionHandle
    block(feService, handle)
  }

  def withFEServiceAndHandleAndResultLimit(
      block: (FrontendService, TSessionHandle) => Unit): Unit = {
    val feService = server.feService
    val req = new TOpenSessionReq(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
    req.setUsername(user)
    req.setConfiguration(
      Map("set:hivevar:" + KyuubiConf.OPERATION_RESULT_LIMIT.key -> "1").asJava)
    val resp = feService.OpenSession(req)
    resp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
    val handle = resp.getSessionHandle
    block(feService, handle)
  }

  def withFEServiceAndHandleAndException(
      block: (FrontendService, TSessionHandle) => Unit): Unit = {
    val feService = server.feService
    val req = new TOpenSessionReq(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
    req.setUsername(user)
    req.setConfiguration(
      Map("set:hivevar:" + KyuubiConf.OPERATION_RESULT_LIMIT.key -> "invaild put").asJava)
    val resp = feService.OpenSession(req)
    resp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
    val handle = resp.getSessionHandle
    block(feService, handle)
  }

  def withFEServiceAndHandleInc(block: (FrontendService, TSessionHandle) => Unit): Unit = {
    val feService = server.feService
    val req = new TOpenSessionReq(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
    req.setUsername(user)
    req.setConfiguration(
      Map("set:hivevar:" + KyuubiConf.OPERATION_INCREMENTAL_COLLECT.key -> "true").asJava)
    val resp = feService.OpenSession(req)
    resp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
    val handle = resp.getSessionHandle
    block(feService, handle)
  }

  def withFEServiceAndHandleIncAndCal(block: (FrontendService, TSessionHandle) => Unit): Unit = {
    val feService = server.feService
    val req = new TOpenSessionReq(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
    req.setUsername(user)
    req.setConfiguration(
      Map("set:hivevar:" + KyuubiConf.OPERATION_INCREMENTAL_COLLECT.key -> "true",
        "set:hivevar:" + KyuubiConf.OPERATION_INCREMENTAL_RDD_PARTITIONS_LIMIT.key -> "-1")
        .asJava)
    val resp = feService.OpenSession(req)
    resp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
    val handle = resp.getSessionHandle
    block(feService, handle)
  }
}
