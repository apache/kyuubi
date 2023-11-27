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

package org.apache.kyuubi.engine.spark.operation

import scala.collection.JavaConverters._

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TExecuteStatementReq, TGetOperationStatusReq, TJobExecutionStatus}

class SparkOperationProgressSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {

  override protected def jdbcUrl: String = getJdbcUrl
  override def withKyuubiConf: Map[String, String] = Map(
    "spark.master" -> "local[1]",
    "spark.ui.liveUpdate.period" -> "0",
    "kyuubi.operation.progress.enabled" -> "true",
    "kyuubi.operation.status.polling.timeout" -> "1000")

  test("test operation progress") {
    withSessionHandle { (client, handle) =>
      val preReq = new TExecuteStatementReq()
      preReq.setStatement("show databases")
      preReq.setSessionHandle(handle)
      preReq.setRunAsync(false)
      client.ExecuteStatement(preReq)

      val sql = "SELECT java_method('java.lang.Thread', 'sleep', 10000l) FROM range(1, 3, 1, 2)"
      val req = new TExecuteStatementReq()
      req.setStatement(sql)
      req.setRunAsync(true)
      req.setSessionHandle(handle)
      val resp = client.ExecuteStatement(req)
      var checkFlag1 = false
      var checkFlag2 = false
      var checkFlag3 = false
      val initStageId = initJobId
      eventually(Timeout(25.seconds)) {
        val statusReq = new TGetOperationStatusReq(resp.getOperationHandle)
        val statusResp = client.GetOperationStatus(statusReq)
        val headers = statusResp.getProgressUpdateResponse.getHeaderNames
        val progress = statusResp.getProgressUpdateResponse.getProgressedPercentage
        val rows = statusResp.getProgressUpdateResponse.getRows
        val footerSummary = statusResp.getProgressUpdateResponse.getFooterSummary
        val status = statusResp.getProgressUpdateResponse.getStatus
        assertResult(Seq(
          "STAGES",
          "ATTEMPT",
          "STATUS",
          "TOTAL",
          "COMPLETED",
          "RUNNING",
          "PENDING",
          "FAILED",
          ""))(headers.asScala)
        assert(rows.size() == 1)
        progress match {
          case 0.0 =>
            assert(Seq(s"Stage-$initStageId ", "0", "PENDING", "2", "0", "0", "2", "0", "")
              == rows.get(0).asScala ||
              Seq(s"Stage-$initStageId ", "0", "RUNNING", "2", "0", "1", "1", "0", "")
              == rows.get(0).asScala ||
              Seq(s"Stage-$initStageId ", "0", "RUNNING", "2", "0", "2", "0", "0", "")
              == rows.get(0).asScala)
            assert("STAGES: 00/01" === footerSummary)
            assert(TJobExecutionStatus.IN_PROGRESS === status)
            checkFlag1 = true
          case 0.5 =>
            assertResult(Seq(
              s"Stage-$initStageId ....",
              "0",
              "RUNNING",
              "2",
              "1",
              "1",
              "0",
              "0",
              ""))(
              rows.get(0).asScala)
            assert("STAGES: 00/01" === footerSummary)
            assert(TJobExecutionStatus.IN_PROGRESS === status)
            checkFlag2 = true
          case 1.0 =>
            assertResult(Seq(
              s"Stage-$initStageId ........",
              "0",
              "FINISHED",
              "2",
              "2",
              "0",
              "0",
              "0",
              ""))(
              rows.get(0).asScala)
            assert("STAGES: 01/01" === footerSummary)
            checkFlag3 = true
        }
        assert(checkFlag1 && checkFlag2 && checkFlag3)
      }
    }
  }

}
