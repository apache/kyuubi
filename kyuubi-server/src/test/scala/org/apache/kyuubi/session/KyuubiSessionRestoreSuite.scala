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

package org.apache.kyuubi.session

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.{Utils, WithMultipleKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.TClientTestUtils

class KyuubiSessionRestoreSuite extends WithMultipleKyuubiServer {

  override protected val conf: KyuubiConf = KyuubiConf()

  test("restore session") {
    val server1Url = getConnectionUrl(0)
    val server2Url = getConnectionUrl(1)
    assert(server1Url !== server2Url)
    var sessionHandle: TSessionHandle = null
    TClientTestUtils.withThriftClient(server1Url) { client =>
      val conf = Map("kyuubi.session.engine.launch.async" -> "false")
      val req = new TOpenSessionReq()
      req.setUsername(Utils.currentUser)
      req.setPassword("anonymous")
      req.setConfiguration(conf.asJava)
      val resp = client.OpenSession(req)
      sessionHandle = resp.getSessionHandle()
      servers(0).stop()
    }
    assert(sessionHandle != null)
    TClientTestUtils.withThriftClient(server2Url) { client =>
      val req = new TGetInfoReq(sessionHandle, TGetInfoType.CLI_SERVER_NAME)
      val resp = client.GetInfo(req)
      assert(resp.getInfoValue.getStringValue == "Apache Kyuubi (Incubating)")

      val executeStmtReq = new TExecuteStatementReq()
      executeStmtReq.setStatement("select engine_name()")
      executeStmtReq.setSessionHandle(sessionHandle)
      executeStmtReq.setRunAsync(false)
      val executeStmtResp = client.ExecuteStatement(executeStmtReq)
      assert(executeStmtResp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)

      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(executeStmtResp.getOperationHandle)
      tFetchResultsReq.setFetchType(0)
      tFetchResultsReq.setMaxRows(10)
      val tFetchResultsResp = client.FetchResults(tFetchResultsReq)
      assert(tFetchResultsResp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
      val resultSet = tFetchResultsResp.getResults.getColumns.asScala
      assert(resultSet.size == 1)
      assert(resultSet.head.getStringVal.getValues.get(0).startsWith("kyuubi_USER_SPARK_SQL_"))
    }
  }

  test("restore GetSchemas") {
    val server1Url = getConnectionUrl(0)
    val server2Url = getConnectionUrl(1)
    assert(server1Url !== server2Url)
    var sessionHandle: TSessionHandle = null
    var operationHandle: TOperationHandle = null
    TClientTestUtils.withThriftClient(server1Url) { client =>
      val conf = Map("kyuubi.session.engine.launch.async" -> "false")
      val req = new TOpenSessionReq()
      req.setUsername(Utils.currentUser)
      req.setPassword("anonymous")
      req.setConfiguration(conf.asJava)
      val resp = client.OpenSession(req)
      assert(resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
      sessionHandle = resp.getSessionHandle()

      val tGetSchemasReq = new TGetSchemasReq()
      tGetSchemasReq.setSessionHandle(sessionHandle)
      val tGetSchemasResp = client.GetSchemas(tGetSchemasReq)
      assert(tGetSchemasResp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
      operationHandle = tGetSchemasResp.getOperationHandle

      servers(0).stop()
    }
    assert(sessionHandle != null)
    assert(operationHandle != null)
    TClientTestUtils.withThriftClient(server2Url) { client =>
      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(operationHandle)
      tFetchResultsReq.setFetchType(0)
      tFetchResultsReq.setMaxRows(10)
      val tFetchResultsResp = client.FetchResults(tFetchResultsReq)
      assert(tFetchResultsResp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
      val resultSet = tFetchResultsResp.getResults.getColumns.asScala
      assert(!resultSet.isEmpty)
      assert(resultSet.head.getStringVal.getValues.contains("default"))
    }
  }

  test("restore ExecuteStatement sync") {
    val server1Url = getConnectionUrl(0)
    val server2Url = getConnectionUrl(1)
    assert(server1Url !== server2Url)
    var sessionHandle: TSessionHandle = null
    var operationHandle: TOperationHandle = null
    TClientTestUtils.withThriftClient(server1Url) { client =>
      val conf = Map("kyuubi.session.engine.launch.async" -> "false")
      val req = new TOpenSessionReq()
      req.setUsername(Utils.currentUser)
      req.setPassword("anonymous")
      req.setConfiguration(conf.asJava)
      val resp = client.OpenSession(req)
      assert(resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
      sessionHandle = resp.getSessionHandle()

      val executeStmtReq = new TExecuteStatementReq()
      executeStmtReq.setStatement(
        "select engine_name() from (select java_method('java.lang.Thread', 'sleep', 2000L))")
      executeStmtReq.setSessionHandle(sessionHandle)
      executeStmtReq.setRunAsync(false)
      val executeStmtResp = client.ExecuteStatement(executeStmtReq)
      assert(executeStmtResp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
      operationHandle = executeStmtResp.getOperationHandle

      servers(0).stop()
    }
    assert(sessionHandle != null)
    assert(operationHandle != null)
    TClientTestUtils.withThriftClient(server2Url) { client =>
      val tGetOperationStatusReq = new TGetOperationStatusReq()
      tGetOperationStatusReq.setOperationHandle(operationHandle)
      val tGetOperationStatusResq = client.GetOperationStatus(tGetOperationStatusReq)
      assert(tGetOperationStatusResq.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
      assert(tGetOperationStatusResq.getOperationState == TOperationState.FINISHED_STATE)

      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(operationHandle)
      tFetchResultsReq.setFetchType(0)
      tFetchResultsReq.setMaxRows(10)
      val tFetchResultsResp = client.FetchResults(tFetchResultsReq)
      assert(tFetchResultsResp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
      val resultSet = tFetchResultsResp.getResults.getColumns.asScala
      assert(!resultSet.isEmpty)
      assert(resultSet.head.getStringVal.getValues.get(0).startsWith("kyuubi_USER_SPARK_SQL_"))
    }
  }

  test("restore ExecuteStatement async") {
    val server1Url = getConnectionUrl(0)
    val server2Url = getConnectionUrl(1)
    assert(server1Url !== server2Url)
    var sessionHandle: TSessionHandle = null
    var operationHandle: TOperationHandle = null
    TClientTestUtils.withThriftClient(server1Url) { client =>
      val conf = Map("kyuubi.session.engine.launch.async" -> "false")
      val req = new TOpenSessionReq()
      req.setUsername(Utils.currentUser)
      req.setPassword("anonymous")
      req.setConfiguration(conf.asJava)
      val resp = client.OpenSession(req)
      assert(resp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
      sessionHandle = resp.getSessionHandle()

      val executeStmtReq = new TExecuteStatementReq()
      executeStmtReq.setStatement(
        "select engine_name(), java_method('java.lang.Thread', 'sleep', 10000L)")
      executeStmtReq.setSessionHandle(sessionHandle)
      executeStmtReq.setRunAsync(true)
      val executeStmtResp = client.ExecuteStatement(executeStmtReq)
      assert(executeStmtResp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
      operationHandle = executeStmtResp.getOperationHandle

      servers(0).stop()
    }
    assert(sessionHandle != null)
    assert(operationHandle != null)
    TClientTestUtils.withThriftClient(server2Url) { client =>
      eventually(Timeout(20.seconds)) {
        val tGetOperationStatusReq = new TGetOperationStatusReq()
        tGetOperationStatusReq.setOperationHandle(operationHandle)
        val tGetOperationStatusResq = client.GetOperationStatus(tGetOperationStatusReq)
        assert(tGetOperationStatusResq.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
        assert(tGetOperationStatusResq.getOperationState == TOperationState.FINISHED_STATE)
      }

      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(operationHandle)
      tFetchResultsReq.setFetchType(0)
      tFetchResultsReq.setMaxRows(10)
      val tFetchResultsResp = client.FetchResults(tFetchResultsReq)
      assert(tFetchResultsResp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
      val resultSet = tFetchResultsResp.getResults.getColumns.asScala
      assert(!resultSet.isEmpty)
      assert(resultSet.head.getStringVal.getValues.get(0).startsWith("kyuubi_USER_SPARK_SQL_"))
    }
  }
}
