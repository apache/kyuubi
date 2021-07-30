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

package org.apache.kyuubi.engine.spark

import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap}

import org.apache.hive.service.rpc.thrift._
import org.apache.hive.service.rpc.thrift.TCLIService.Iface
import org.apache.hive.service.rpc.thrift.TOperationState._
import org.apache.spark.scheduler.JobSucceeded
import org.scalatest.PrivateMethodTester
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.engine.spark.monitor.KyuubiStatementMonitor
import org.apache.kyuubi.engine.spark.monitor.entity.{KyuubiJobInfo, KyuubiStatementInfo}
import org.apache.kyuubi.operation.{HiveJDBCTests, OperationHandle}

class KyuubiStatementMonitorSuite extends WithSparkSQLEngine with HiveJDBCTests
    with PrivateMethodTester {

  override protected def jdbcUrl: String = getJdbcUrl
  override def withKyuubiConf: Map[String, String] = Map.empty

  test("add kyuubiStatementInfo into queue") {
    var baseSql = "select timestamp'2021-06-0"
    val total: Int = 7
    // Clear kyuubiStatementQueue first
    val getQueue = PrivateMethod[
      ArrayBlockingQueue[KyuubiStatementInfo]](Symbol("kyuubiStatementQueue"))()
    val kyuubiStatementQueue = KyuubiStatementMonitor.invokePrivate(getQueue)
    kyuubiStatementQueue.clear()
    withSessionHandle { (client, handle) =>

      for ( a <- 1 to total ) {
        val sql = baseSql + a + "'"
        val req = new TExecuteStatementReq()
        req.setSessionHandle(handle)
        req.setStatement(sql)
        val tExecuteStatementResp = client.ExecuteStatement(req)
        val operationHandle = tExecuteStatementResp.getOperationHandle

        val kyuubiStatementInfo = kyuubiStatementQueue.poll()
        assert(
          kyuubiStatementInfo.statementId === OperationHandle(operationHandle).identifier.toString)
        assert(sql === kyuubiStatementInfo.statement)
      }
    }
  }

  test("add kyuubiJobInfo into queue and remove them when threshold reached") {
    val sql = "select timestamp'2021-06-01'"
    val getJobMap = PrivateMethod[
      ConcurrentHashMap[Int, KyuubiJobInfo]](Symbol("kyuubiJobIdToJobInfoMap"))()

    val jobIdToJobInfoMap = KyuubiStatementMonitor.invokePrivate(getJobMap)
    jobIdToJobInfoMap.clear()
    withSessionHandle { (client, handle) =>
      val req = new TExecuteStatementReq()
      req.setSessionHandle(handle)
      req.setStatement(sql)
      val tExecuteStatementResp = client.ExecuteStatement(req)
      val opHandle = tExecuteStatementResp.getOperationHandle

      eventually(timeout(10.seconds), interval(100.milliseconds)) {
        val elements = jobIdToJobInfoMap.elements()
        while (elements.hasMoreElements) {
          val kyuubiJobInfo = elements.nextElement()
          assert(jobIdToJobInfoMap.size() === 1)
          assert(kyuubiJobInfo.statementId === OperationHandle(opHandle).identifier.toString)
          assert(kyuubiJobInfo.stageIds.length === 1)
          assert(kyuubiJobInfo.jobResult === JobSucceeded)
          assert(kyuubiJobInfo.endTime !== 0)
        }
      }

      // Test for clear kyuubiJobIdToJobInfoMap when threshold reached
      // This function is used for avoiding mem leak
      (1 to 7).foreach { _ =>
        val req = new TExecuteStatementReq()
        req.setSessionHandle(handle)
        req.setStatement(sql)
        val tExecuteStatementResp = client.ExecuteStatement(req)
        val operationHandle = tExecuteStatementResp.getOperationHandle
        waitForOperationToComplete(client, operationHandle)
      }
      assert(jobIdToJobInfoMap.size() === 1)
    }
  }

  test("test for dumpping statementInfo into local file and query it by id") {
    val sql = "select timestamp'2021-06-01'"
    val dumpLocalDir = KyuubiStatementMonitor
      .invokePrivate(PrivateMethod[String](Symbol("dumpLocalDir"))())
    withSessionHandle { (client, handle) =>
      val req = new TExecuteStatementReq()
      req.setSessionHandle(handle)
      req.setStatement(sql)
      val tExecuteStatementResp = client.ExecuteStatement(req)
      val tOperationHandle = tExecuteStatementResp.getOperationHandle
      val statementId = OperationHandle(tOperationHandle).identifier.toString
      val querySql = new StringBuilder("select count(*) from parquet.`")
        .append(dumpLocalDir)
        .append("/statement_info.parquet")
        .append("` where statementId = \"")
        .append(statementId)
        .append("\"")
        .toString()
      eventually(timeout(90.seconds), interval(5.seconds)) {
        assert(spark.sql(querySql).first().get(0).asInstanceOf[Long] >= 1)
      }
    }
  }

  private def waitForOperationToComplete(client: Iface, op: TOperationHandle): Unit = {
    val req = new TGetOperationStatusReq(op)
    var state = client.GetOperationStatus(req).getOperationState
    while (state == INITIALIZED_STATE || state == PENDING_STATE || state == RUNNING_STATE) {
      state = client.GetOperationStatus(req).getOperationState
    }
  }
}
