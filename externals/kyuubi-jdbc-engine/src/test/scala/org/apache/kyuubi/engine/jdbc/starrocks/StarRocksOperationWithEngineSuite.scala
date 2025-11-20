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
package org.apache.kyuubi.engine.jdbc.starrocks

import scala.concurrent.duration.DurationInt

import org.scalatest.concurrent.TimeLimits.failAfter

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.jdbc.connection.ConnectionProvider
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

class StarRocksOperationWithEngineSuite extends StarRocksOperationSuite with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = jdbcConnectionUrl

  test("starrocks - test for Jdbc engine getInfo") {
    val metaData = ConnectionProvider.create(kyuubiConf).getMetaData

    withSessionConf(Map(KyuubiConf.SERVER_INFO_PROVIDER.key -> "ENGINE"))()() {
      withSessionHandle { (client, handle) =>
        val req = new TGetInfoReq()
        req.setSessionHandle(handle)
        req.setInfoType(TGetInfoType.CLI_DBMS_NAME)
        assert(client.GetInfo(req).getInfoValue.getStringValue == metaData.getDatabaseProductName)

        val req2 = new TGetInfoReq()
        req2.setSessionHandle(handle)
        req2.setInfoType(TGetInfoType.CLI_DBMS_VER)
        assert(
          client.GetInfo(req2).getInfoValue.getStringValue == metaData.getDatabaseProductVersion)

        val req3 = new TGetInfoReq()
        req3.setSessionHandle(handle)
        req3.setInfoType(TGetInfoType.CLI_MAX_COLUMN_NAME_LEN)
        assert(client.GetInfo(req3).getInfoValue.getLenValue == metaData.getMaxColumnNameLength)

        val req4 = new TGetInfoReq()
        req4.setSessionHandle(handle)
        req4.setInfoType(TGetInfoType.CLI_MAX_SCHEMA_NAME_LEN)
        assert(client.GetInfo(req4).getInfoValue.getLenValue == metaData.getMaxSchemaNameLength)

        val req5 = new TGetInfoReq()
        req5.setSessionHandle(handle)
        req5.setInfoType(TGetInfoType.CLI_MAX_TABLE_NAME_LEN)
        assert(client.GetInfo(req5).getInfoValue.getLenValue == metaData.getMaxTableNameLength)
      }
    }
  }

  test("starrocks - JDBC ExecuteStatement operation should contain operationLog") {
    withSessionHandle { (client, handle) =>
      val tExecuteStatementReq = new TExecuteStatementReq()
      tExecuteStatementReq.setSessionHandle(handle)
      tExecuteStatementReq.setStatement("SELECT 1")
      val tExecuteStatementResp = client.ExecuteStatement(tExecuteStatementReq)

      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(tExecuteStatementResp.getOperationHandle)
      tFetchResultsReq.setFetchType(1)
      tFetchResultsReq.setMaxRows(1)

      val tFetchResultsResp = client.FetchResults(tFetchResultsReq)
      assert(tFetchResultsResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
    }
  }

  test("starrocks - JDBC ExecuteStatement cancel operation should kill SQL statement") {
    failAfter(20.seconds) {
      withSessionHandle { (client, handle) =>
        val tExecuteStatementReq = new TExecuteStatementReq()
        tExecuteStatementReq.setSessionHandle(handle)
        // The SQL will sleep 120s
        tExecuteStatementReq.setStatement("SELECT sleep(120)")
        tExecuteStatementReq.setRunAsync(true)
        val tExecuteStatementResp = client.ExecuteStatement(tExecuteStatementReq)

        Thread.sleep(1000) // wait for statement to start executing

        val tCancelOperationReq = new TCancelOperationReq()
        tCancelOperationReq.setOperationHandle(tExecuteStatementResp.getOperationHandle)

        val tFetchResultsResp = client.CancelOperation(tCancelOperationReq)
        assert(tFetchResultsResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
        // If the statement is not cancelled successfully, will block here until 120s
      }
    }
  }
}
