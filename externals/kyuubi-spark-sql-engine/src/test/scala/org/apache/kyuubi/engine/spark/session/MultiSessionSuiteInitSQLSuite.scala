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

import scala.jdk.CollectionConverters._

import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SESSION_SPARK_INITIALIZE_SQL, ENGINE_SHARE_LEVEL, ENGINE_SINGLE_SPARK_SESSION, ENGINE_USER_ISOLATED_SPARK_SESSION, ENGINE_USER_ISOLATED_SPARK_SESSION_IDLE_INTERVAL, ENGINE_USER_ISOLATED_SPARK_SESSION_IDLE_TIMEOUT}
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TExecuteStatementReq, TFetchResultsReq, TOpenSessionReq}

class MultiSessionSuiteInitSQLSuite  extends WithSparkSQLEngine with HiveJDBCTestHelper {

  override def withKyuubiConf: Map[String, String] = {
    Map(
      ENGINE_SHARE_LEVEL.key -> "SERVER",
      ENGINE_SINGLE_SPARK_SESSION.key -> "false",
      (
        ENGINE_SESSION_SPARK_INITIALIZE_SQL.key,
        "CREATE DATABASE IF NOT EXISTS INIT_DB_SOLO;" +
          "CREATE TABLE IF NOT EXISTS INIT_DB_SOLO.test(a int) USING CSV;" +
          "INSERT INTO INIT_DB_SOLO.test VALUES (2);"))
  }

  override protected def jdbcUrl: String =
    s"jdbc:hive2://${engine.frontendServices.head.connectionUrl}/;#spark.ui.enabled=false"

  private def executeServerSharedSetStatement(user: String,
                                              initStatement: String,
                                              executeStatement: String): Long = {
    withThriftClient(Some(user)) { client =>
      val req = new TOpenSessionReq()
      req.setUsername(user)
      req.setPassword("anonymous")
      req.setConfiguration(Map(
        ENGINE_SHARE_LEVEL.key -> "SERVER",
        ENGINE_SINGLE_SPARK_SESSION.key -> "false",
        (
          ENGINE_SESSION_SPARK_INITIALIZE_SQL.key,
          initStatement)).asJava)
      val tOpenSessionResp = client.OpenSession(req)
      val tExecuteStatementReq = new TExecuteStatementReq()
      tExecuteStatementReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      tExecuteStatementReq.setStatement(executeStatement)
      tExecuteStatementReq.setRunAsync(false)
      val tExecuteStatementResp = client.ExecuteStatement(tExecuteStatementReq)

      val operationHandle = tExecuteStatementResp.getOperationHandle
      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(operationHandle)
      tFetchResultsReq.setFetchType(0)
      tFetchResultsReq.setMaxRows(1)
      val tFetchResultsResp = client.FetchResults(tFetchResultsReq)
      tFetchResultsResp.getResults.getColumns.get(0).getI64Val.getValues.get(0)
    }
  }

  test("isolated user spark session") {
    assert(executeServerSharedSetStatement("user",
      "SHOW TABLES",
      "SELECT COUNT(*) FROM INIT_DB_SOLO.test WHERE a = 2") ==
      1)


    assert(executeServerSharedSetStatement("user",
      "INSERT INTO INIT_DB_SOLO.test VALUES (3);",
      "SELECT COUNT(*) FROM INIT_DB_SOLO.test WHERE a = 3") ==
      1)
  }
}