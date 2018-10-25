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

import org.apache.hive.service.cli.thrift._
import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.apache.spark.KyuubiConf._
import org.scalatest.Matchers

import yaooqinn.kyuubi.service.{ServiceException, State}
import yaooqinn.kyuubi.session.SessionHandle

class FrontendServiceSuite extends SparkFunSuite with Matchers {

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

  test(" test new fe service") {
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
}
