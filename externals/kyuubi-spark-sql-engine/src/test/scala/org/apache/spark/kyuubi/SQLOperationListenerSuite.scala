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

package org.apache.spark.kyuubi

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.hive.service.rpc.thrift.{TExecuteStatementReq, TFetchOrientation, TFetchResultsReq}
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class SQLOperationListenerSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {

  override def withKyuubiConf: Map[String, String] = Map.empty

  override protected def jdbcUrl: String = getJdbcUrl

  test("operation listener") {
    val sql = "select /*+ REPARTITION(3, a) */ a from values(1) t(a);"
    withSessionHandle { (client, handle) =>
      val req = new TExecuteStatementReq()
      req.setSessionHandle(handle)
      req.setStatement(sql)
      val tExecuteStatementResp = client.ExecuteStatement(req)
      val opHandle = tExecuteStatementResp.getOperationHandle
      val fetchResultsReq = new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_NEXT, 1000)
      fetchResultsReq.setFetchType(1.toShort)
      eventually(timeout(90.seconds), interval(500.milliseconds)) {
        val resultsResp = client.FetchResults(fetchResultsReq)
        val logs = resultsResp.getResults.getColumns.get(0).getStringVal.getValues.asScala
        assert(logs.exists(_.contains("started with 2 stages")))
        assert(logs.exists(_.contains("started with 1 tasks")))
        assert(logs.exists(_.contains("started with 3 tasks")))
        assert(logs.exists(_.contains("Finished stage:")))
        assert(logs.exists(_.contains(s"Job ${0 + initJobId} succeeded")))
      }
    }
  }
}
