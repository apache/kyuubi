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

package org.apache.kyuubi.engine.spark.session

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TExecuteStatementReq, TFetchResultsReq, TOpenSessionReq}

class UserIsolatedSessionSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {

  override def withKyuubiConf: Map[String, String] = {
    Map(
      ENGINE_SHARE_LEVEL.key -> "GROUP",
      ENGINE_USER_ISOLATED_SPARK_SESSION.key -> "false",
      ENGINE_USER_ISOLATED_SPARK_SESSION_IDLE_INTERVAL.key -> "100",
      ENGINE_USER_ISOLATED_SPARK_SESSION_IDLE_TIMEOUT.key -> "5000")
  }

  override protected def jdbcUrl: String =
    s"jdbc:hive2://${engine.frontendServices.head.connectionUrl}/;#spark.ui.enabled=false"

  private def executeSetStatement(user: String, statement: String): String = {
    withThriftClient(Some(user)) { client =>
      val req = new TOpenSessionReq()
      req.setUsername(user)
      req.setPassword("anonymous")
      val tOpenSessionResp = client.OpenSession(req)
      val tExecuteStatementReq = new TExecuteStatementReq()
      tExecuteStatementReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      tExecuteStatementReq.setStatement(statement)
      tExecuteStatementReq.setRunAsync(false)
      val tExecuteStatementResp = client.ExecuteStatement(tExecuteStatementReq)

      val operationHandle = tExecuteStatementResp.getOperationHandle
      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(operationHandle)
      tFetchResultsReq.setFetchType(0)
      tFetchResultsReq.setMaxRows(1)
      val tFetchResultsResp = client.FetchResults(tFetchResultsReq)
      tFetchResultsResp.getResults.getColumns.get(1).getStringVal.getValues.get(0)
    }
  }

  test("isolated user spark session") {
    executeSetStatement("user1", "set a=1")
    assert(executeSetStatement("user1", "set a") == "1")
    assert(executeSetStatement("user1", "set a") == "1")
    assert(executeSetStatement("user2", "set a") == "<undefined>")
    executeSetStatement("user2", "set a=2")
    assert(executeSetStatement("user1", "set a") == "1")
    assert(executeSetStatement("user2", "set a") == "2")

    Thread.sleep(6000)
    assert(executeSetStatement("user1", "set a") == "<undefined>")
    assert(executeSetStatement("user2", "set a") == "<undefined>")
  }
}
