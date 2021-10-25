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

package org.apache.kyuubi.operation

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hive.service.rpc.thrift._
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf

class KyuubiIncrementCollectSuite extends WithKyuubiServer with JDBCTestHelper {

  override protected val conf: KyuubiConf = KyuubiConf()
    .set(KyuubiConf.OPERATION_INCREMENTAL_COLLECT, true)

  override protected def jdbcUrl: String = getJdbcUrl

  test("change incremental collect mode using SET commands") {
    val querySQL = "SELECT * FROM VALUES(1),(2),(3) AS t(c1) DISTRIBUTE BY c1"
    withSessionHandle { (client, handle) =>
      def execute(sql: String): TOperationHandle = {
        val req = new TExecuteStatementReq()
        req.setSessionHandle(handle)
        req.setStatement(sql)
        val execStmtResp = client.ExecuteStatement(req)
        execStmtResp.getOperationHandle
      }

      def executeAndWait(sql: String): TOperationHandle = {
        val opHandle = execute(sql)
        waitForOperationToComplete(client, opHandle)
        opHandle
      }

      def queryAndCheckLog(sql: String, checkedText: String): Unit = {
        val opHandle = execute(sql)
        val fetchResultsReq = new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_NEXT, 1000)
        fetchResultsReq.setFetchType(1.toShort)
        eventually(timeout(10.seconds), interval(100.milliseconds)) {
          val resultsResp = client.FetchResults(fetchResultsReq)
          val logs = resultsResp.getResults.getColumns.get(0).getStringVal.getValues.asScala
          assert(logs.exists(_ contains checkedText))
        }
      }

      queryAndCheckLog(querySQL, "Execute in incremental collect mode")
      executeAndWait("SET kyuubi.operation.incremental.collect=false")
      queryAndCheckLog(querySQL, "Execute in full collect mode")
      executeAndWait("SET kyuubi.operation.incremental.collect=true")
      queryAndCheckLog(querySQL, "Execute in incremental collect mode")
    }
  }

  test("incremental collect query result") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT * FROM VALUES(1),(2),(3) AS t(c1) DISTRIBUTE BY c1")
      val result = new ArrayBuffer[Int]
      while (rs.next()) {
        result += rs.getInt(1)
      }
      assert((Set(1, 2, 3) diff result.toSet).isEmpty)
    }
  }
}
