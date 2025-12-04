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

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TExecuteStatementReq, TFetchResultsReq, TOpenSessionReq}

class MultiSessionSuiteInitSQLSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {

  override def withKyuubiConf: Map[String, String] = {
    Map(
      ENGINE_SHARE_LEVEL.key -> "SERVER",
      ENGINE_SINGLE_SPARK_SESSION.key -> "false")
  }

  override protected def jdbcUrl: String =
    s"jdbc:hive2://${engine.frontendServices.head.connectionUrl}/;#spark.ui.enabled=false"

  test("isolated user spark session") {
    Seq("abc", "xyz").foreach { value =>
      withThriftClient(Some(user)) { client =>
        val req = new TOpenSessionReq()
        req.setUsername("user")
        req.setPassword("anonymous")
        req.setConfiguration(Map(
          ENGINE_SHARE_LEVEL.key -> "SERVER",
          ENGINE_SINGLE_SPARK_SESSION.key -> "false",
          ENGINE_SESSION_SPARK_INITIALIZE_SQL.key -> s"SET varA=$value").asJava)
        val tOpenSessionResp = client.OpenSession(req)
        val tExecuteStatementReq = new TExecuteStatementReq()
        tExecuteStatementReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
        tExecuteStatementReq.setStatement("SELECT '${varA}'")
        tExecuteStatementReq.setRunAsync(false)
        val tExecuteStatementResp = client.ExecuteStatement(tExecuteStatementReq)

        val operationHandle = tExecuteStatementResp.getOperationHandle
        val tFetchResultsReq = new TFetchResultsReq()
        tFetchResultsReq.setOperationHandle(operationHandle)
        tFetchResultsReq.setFetchType(0)
        tFetchResultsReq.setMaxRows(1)
        val tFetchResultsResp = client.FetchResults(tFetchResultsReq)
        val ret = tFetchResultsResp.getResults.getColumns.get(0).getStringVal.getValues.get(0)
        assert(ret === value)
      }
    }
  }
}
