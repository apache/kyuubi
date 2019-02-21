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

import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hive.service.cli.thrift._
import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.apache.spark.KyuubiConf._
import org.scalatest.Matchers

import yaooqinn.kyuubi.SecuredFunSuite
import yaooqinn.kyuubi.service.{ServiceException, State}
import yaooqinn.kyuubi.session.SessionHandle

class FrontendServiceSuite extends SparkFunSuite with Matchers with SecuredFunSuite {

  private val beService = new BackendService()
  private val sessionHandle = new SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)
  private def tHandle: TSessionHandle = sessionHandle.toTSessionHandle
  private val user = KyuubiSparkUtil.getCurrentUserName
  private val catalog = "test_catalog"
  private val tbl = "test_tbl"
  private val schema = "test_schema"
  private val col = "test_col"
  private val conf = new SparkConf(loadDefaults = true).setAppName("fe test")
  KyuubiSparkUtil.setupCommonConfig(conf)
  conf.remove(KyuubiSparkUtil.CATALOG_IMPL)
  conf.setMaster("local").set(FRONTEND_BIND_PORT.key, "0")

  override def beforeAll(): Unit = {
    beService.init(conf)
    beService.start()
    super.beforeAll()
  }
  override def afterAll(): Unit = {
    beService.stop()
    super.afterAll()
  }

  test("test new fe service") {
    val feService = new FrontendService(beService)
    feService.getConf should be(null)
    feService.getStartTime should be(0)
    feService.getServiceState should be(State.NOT_INITED)
    feService.getName should be(classOf[FrontendService].getSimpleName)
    feService.getServerIPAddress should be(null)
    feService.getPortNumber should be(0)
    val catalogsResp = feService.GetCatalogs(new TGetCatalogsReq(tHandle))
    catalogsResp.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
    catalogsResp.getStatus.getErrorMessage should be("Method Not Implemented!")
    val columnsReq = new TGetColumnsReq(tHandle)
    columnsReq.setCatalogName(catalog)
    columnsReq.setSchemaName(schema)
    columnsReq.setTableName(tbl)
    columnsReq.setColumnName(col)
    val columnsResp = feService.GetColumns(columnsReq)
    columnsResp.getStatus.getErrorMessage should be("Method Not Implemented!")
    val getDelegationTokenResp =
      feService.GetDelegationToken(new TGetDelegationTokenReq(tHandle, user, user))
    getDelegationTokenResp.getStatus.getErrorMessage should be("Delegation token is not supported")
  }

  test("init fe service") {
    val feService = new FrontendService(beService)
    feService.init(conf)
    feService.getConf should be(conf)
    feService.getServiceState should be(State.INITED)
    feService.getPortNumber should not be 0
    val conf1 = new SparkConf(loadDefaults = true)
      .set(FRONTEND_BIND_HOST.key, "")
        .set(FRONTEND_BIND_PORT.key, "10009")
    val feService2 = new FrontendService(beService)
    feService2.init(conf1)
    feService2.getServerIPAddress should be(InetAddress.getLocalHost)
    intercept[ServiceException](
      feService2.init(conf1)).getMessage should include("10009")
  }

  test("start fe service") {
    val feService = new FrontendService(beService)
    intercept[IllegalStateException](feService.start())
    feService.init(conf)
    feService.start()
    feService.getConf should be(conf)
    feService.getStartTime should not be 0
    feService.getServiceState should be(State.STARTED)
  }

  test("stop fe service") {
    val feService = new FrontendService(beService)
    feService.stop()
    feService.getServiceState should be(State.NOT_INITED)
    feService.init(conf)
    feService.stop()
    feService.getServiceState should be(State.INITED)
    feService.start()
    feService.stop()
    feService.getServiceState should be(State.STOPPED)
  }

  test("get catalogs") {
    val feService = new FrontendService(beService)
    val req = new TGetCatalogsReq(tHandle)
    val resp = feService.GetCatalogs(req)
    resp.getStatus.getErrorMessage should be("Method Not Implemented!")
  }

  test("get schemas") {
    val feService = new FrontendService(beService)
    val schemasReq = new TGetSchemasReq(tHandle)
    schemasReq.setCatalogName(catalog)
    schemasReq.setSchemaName(schema)
    val resp = feService.GetSchemas(schemasReq)
    resp.getStatus.getErrorMessage should be("Method Not Implemented!")
  }

  test("get tables") {
    val feService = new FrontendService(beService)
    val req = new TGetTablesReq(tHandle)
    req.setCatalogName(catalog)
    req.setSchemaName(schema)
    req.setTableName(tbl)
    val resp = feService.GetTables(req)
    resp.getStatus.getErrorMessage should be("Method Not Implemented!")
  }

  test("get columns") {
    val feService = new FrontendService(beService)
    val req = new TGetColumnsReq(tHandle)
    req.setCatalogName(catalog)
    req.setSchemaName(schema)
    req.setTableName(tbl)
    req.setColumnName(col)
    val resp = feService.GetColumns(req)
    resp.getStatus.getErrorMessage should be("Method Not Implemented!")
  }

  test("get type info") {
    val feService = new FrontendService(beService)
    val req = new TGetTypeInfoReq(tHandle)
    val resp = feService.GetTypeInfo(req)
    resp.getStatus.getErrorMessage should be("Method Not Implemented!")
  }

  test("get port num") {
    val feService = new FrontendService(beService)
    feService.getPortNumber should be(0)
    feService.init(conf)
    feService.getPortNumber should not be 0
  }

  test("get server ip addr") {
    val feService = new FrontendService(beService)
    feService.getServerIPAddress should be(null)
    feService.init(conf)
    feService.getServerIPAddress should not be null
  }

  test("fe service server context") {
    val feService = new FrontendService(beService)
    val context = new feService.FeServiceServerContext()
    context.setSessionHandle(sessionHandle)
    context.getSessionHandle should be(sessionHandle)
  }

  test("fe tserver event handler") {
    val feService = new FrontendService(beService)
    val handler = new feService.FeTServerEventHandler
    val context = new feService.FeServiceServerContext()
    context.setSessionHandle(sessionHandle)
    handler.createContext(null, null)
    handler.processContext(context, null, null)
    handler.deleteContext(context, null, null)
  }

  test("open session, execute sql and get results") {
    val feService = new FrontendService(beService)
    try {
      feService.init(conf)
      feService.start()
      val req = new TOpenSessionReq(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
      val resp = feService.OpenSession(req)
      resp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      val handle = resp.getSessionHandle
      val req2 = new TExecuteStatementReq(handle, "show databases")
      val resp2 = feService.ExecuteStatement(req2)
      resp2.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      val req3 = new TGetOperationStatusReq(resp2.getOperationHandle)
      val resp3 = feService.GetOperationStatus(req3)
      resp3.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      Thread.sleep(5000)
      val req4 = new TFetchResultsReq(resp2.getOperationHandle, TFetchOrientation.FETCH_NEXT, 50)
      val resp4 = feService.FetchResults(req4)
      resp4.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      resp4.getResults.getRows.get(0).getColVals.get(0).getStringVal.getValue should be("default")
      assert(!resp4.getResults.isSetColumns)
      val req5 = new TGetResultSetMetadataReq(resp2.getOperationHandle)
      val resp5 = feService.GetResultSetMetadata(req5)
      resp5.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      resp5.getSchema.getColumns.get(0).getColumnName should be("databaseName")
      val req7 = new TCancelOperationReq(resp2.getOperationHandle)
      val resp7 = feService.CancelOperation(req7)
      resp7.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      val req6 = new TCloseOperationReq(resp2.getOperationHandle)
      val resp6 = feService.CloseOperation(req6)
      resp6.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      val req9 = new TCancelOperationReq(resp2.getOperationHandle)
      val resp9 = feService.CancelOperation(req9)
      resp9.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
      val req8 = new TCloseSessionReq(handle)
      val resp8 = feService.CloseSession(req8)
      resp8.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)

      // after session closed
      val resp10 = feService.CloseSession(req8)
      resp10.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
      resp10.getStatus.getErrorMessage should include("does not exist!")
      val resp11 = feService.ExecuteStatement(req2)
      resp11.getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
      feService.GetOperationStatus(req3).getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
      feService.FetchResults(req4).getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
      feService.GetResultSetMetadata(req5)
        .getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
      feService.CancelOperation(req7).getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)
      feService.CloseOperation(req6).getStatus.getStatusCode should be(TStatusCode.ERROR_STATUS)

      val tOpenSessionReq = new TOpenSessionReq(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
      tOpenSessionReq.setUsername("yaooqinn")
      tOpenSessionReq.setPassword("passwd")
      tOpenSessionReq.setConfiguration(
        Map("hive.server2.proxy.user" -> "kent").asJava)
      val tOpenSessionResp = feService.OpenSession(tOpenSessionReq)
      tOpenSessionResp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
    } finally {
      feService.stop()
    }
  }

  test("alter database") {
    withFEServiceAndHandle { (fe, handle) =>
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
  }

  test("alter schema") {
    withFEServiceAndHandle { (fe, handle) =>
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
  }

  test("alter table name") {
    withFEServiceAndHandle { (fe, handle) =>
      val ct = new TExecuteStatementReq(handle, "create table default.src(key int) using parquet")
      fe.ExecuteStatement(ct)
      Thread.sleep(5000)
      val req = new TExecuteStatementReq(handle, "alter table default.src rename to default.src2")
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

  test("alter table set properties") {
    withFEServiceAndHandle { (fe, handle) =>
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

  test("create function") {
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

  def withFEServiceAndHandle(block: (FrontendService, TSessionHandle) => Unit): Unit = {
    val feService = new FrontendService(beService)
    try {
      feService.init(conf)
      feService.start()
      val req = new TOpenSessionReq(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
      val resp = feService.OpenSession(req)
      resp.getStatus.getStatusCode should be(TStatusCode.SUCCESS_STATUS)
      val handle = resp.getSessionHandle
      block(feService, handle)
    } finally {
      feService.stop()
    }
  }
}
