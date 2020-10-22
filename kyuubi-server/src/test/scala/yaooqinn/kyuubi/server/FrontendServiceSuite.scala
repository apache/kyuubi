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
import org.apache.kyuubi.KyuubiSQLException
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.apache.spark.KyuubiConf._
import org.scalatest.Matchers
import yaooqinn.kyuubi.SecuredFunSuite
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
