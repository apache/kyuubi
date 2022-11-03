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

package org.apache.kyuubi.parser

import org.apache.hive.service.rpc.thrift.{TCLIService, TExecuteStatementReq, TGetOperationStatusReq, TOperationState, TSessionHandle, TStatusCode}
import org.scalatest.tags.Slow

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.jdbc.hive.KyuubiSQLException
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.session.{KyuubiSessionImpl, KyuubiSessionManager, SessionHandle}

@Slow
class EngineRestartNodeSuite extends WithKyuubiServer with HiveJDBCTestHelper {
  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(ENGINE_SHARE_LEVEL.key, "CONNECTION")
      .set("spark.driver.cores", "1")
  }

  test("kill engine") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT cast(0.1 as float) AS col")
      assert(resultSet.next())
      assert(resultSet.getString("col") == "0.1")
      assert(!statement.execute("drop engine"))

      val exception =
        intercept[KyuubiSQLException](statement.executeQuery("SELECT cast(0.1 as float) AS col"))
      assert(
        exception.getMessage.contains("Error operating ExecuteStatement: Socket for SessionHandle"))
    }
  }

  test("launch engine") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT cast(0.1 as float) AS col")
      assert(resultSet.next())
      assert(resultSet.getString("col") == "0.1")
      assert(!statement.execute("drop engine"))

      val exception =
        intercept[KyuubiSQLException](statement.executeQuery("SELECT cast(0.1 as float) AS col"))
      assert(
        exception.getMessage.contains("Error operating ExecuteStatement: Socket for SessionHandle"))

      assert(!statement.execute("create engine"))
      val resultSet2 = statement.executeQuery("SELECT cast(0.1 as float) AS col")
      assert(resultSet2.next())
      assert(resultSet2.getString("col") == "0.1")
    }
  }

  test("alter session conf") {

    def executeStatement(client: TCLIService.Iface, handle: TSessionHandle, sql: String): Unit = {
      val req = new TExecuteStatementReq()
      req.setSessionHandle(handle)
      req.setStatement(sql)
      val tExecuteStatementResp = client.ExecuteStatement(req)
      val opHandle = tExecuteStatementResp.getOperationHandle
      val tGetOperationStatusReq = new TGetOperationStatusReq()
      tGetOperationStatusReq.setOperationHandle(opHandle)
      val resp = client.GetOperationStatus(tGetOperationStatusReq)
      val status = resp.getStatus
      assert(status.getStatusCode === TStatusCode.SUCCESS_STATUS)
      assert(resp.getOperationState === TOperationState.FINISHED_STATE)
    }

    withSessionHandle { (client, handle) =>
      val sessionHandle = SessionHandle(handle)
      val session = server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
        .getSession(sessionHandle).asInstanceOf[KyuubiSessionImpl]
      assert(session.kyuubiConf.getOption("spark.driver.cores").isDefined)
      assert(session.kyuubiConf.getOption("spark.driver.cores").get == "1")

      executeStatement(client, handle, "SELECT cast(0.1 as float) AS col")

      executeStatement(client, handle, "ALTER SESSION SET (spark.driver.cores=2)")
      assert(session.kyuubiConf.getOption("spark.driver.cores").isDefined)
      assert(session.kyuubiConf.getOption("spark.driver.cores").get == "2")

      executeStatement(client, handle, "SELECT cast(0.1 as float) AS col")
    }
  }

  override protected def jdbcUrl: String = getJdbcUrl
}
