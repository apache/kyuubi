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

import java.sql.ResultSet

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_JDBC_FETCH_SIZE, OPERATION_RESULT_PREFETCH, OPERATION_RESULT_SAVE_TO_FILE, OPERATION_RESULT_SAVE_TO_FILE_DIR, OPERATION_RESULT_SAVE_TO_FILE_MIN_ROWS, OPERATION_RESULT_SAVE_TO_FILE_MINSIZE}
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.engine.spark.session.SparkSQLSessionManager
import org.apache.kyuubi.jdbc.hive.KyuubiStatement
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TExecuteStatementReq, TFetchOrientation, TFetchResultsReq, TStatusCode}

class SparkSaveFilePrefetchSuite extends WithSparkSQLEngine with HiveJDBCTestHelper {
  val defaultFetchSize: Int = 300

  override def withKyuubiConf: Map[String, String] = {
    Map(
      OPERATION_RESULT_SAVE_TO_FILE.key -> "true",
      OPERATION_RESULT_SAVE_TO_FILE_MINSIZE.key -> "100",
      OPERATION_RESULT_SAVE_TO_FILE_MIN_ROWS.key -> "100",
      OPERATION_RESULT_PREFETCH.key -> "true",
      ENGINE_JDBC_FETCH_SIZE.key -> defaultFetchSize.toString)
  }

  override protected def jdbcUrl: String =
    s"jdbc:hive2://${engine.frontendServices.head.connectionUrl}/;#spark.ui.enabled=false"

  private def sessionHandle: String = {
    engine.backendService.sessionManager.allSessions().head.handle.identifier.toString
  }

  private def getOperationHandle: String = {
    val operationHandle =
      engine.backendService.sessionManager.operationManager.allOperations()
        .head.getHandle.identifier.toString
    s"$sessionHandle/$operationHandle"
  }

  test("test save file effect") {
    var enginePath: Path = new Path(kyuubiConf.get(OPERATION_RESULT_SAVE_TO_FILE_DIR))
    val sparkFileSystem = enginePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    var sessionPath: Path = enginePath
    withJdbcStatement("table1") { statement =>
      enginePath =
        engine.backendService.sessionManager.asInstanceOf[SparkSQLSessionManager]
          .getEngineResultSavePath()
      sessionPath = new Path(s"$enginePath/$sessionHandle")
      statement.asInstanceOf[KyuubiStatement].getConnection.getClientInfo
      statement.setFetchSize(100)
      val expectedStoppedNum = 1000
      // test save result skip command
      statement.executeQuery(s"create table table1 as select id from range($expectedStoppedNum)")
      var resultPath = new Path(s"$enginePath/$getOperationHandle")
      assert(!sparkFileSystem.exists(resultPath))
      statement.executeQuery("show tables")
      resultPath = new Path(s"$enginePath/$getOperationHandle")
      assert(!sparkFileSystem.exists(resultPath))
      // test query save result
      val res = statement.executeQuery("select * from table1")
      resultPath = new Path(s"$enginePath/$getOperationHandle")
      assert(sparkFileSystem.exists(resultPath))
      res.close()
      assert(!sparkFileSystem.exists(resultPath))
      // test rows number less than minRows
      statement.executeQuery("select * from table1 limit 10")
      resultPath = new Path(s"$enginePath/$getOperationHandle")
      assert(!sparkFileSystem.exists(resultPath))
    }
    // delete session path after session close
    assert(!sparkFileSystem.exists(sessionPath))
  }

  test("check query results with prefetch") {
    withJdbcStatement("table1") { statement =>
      statement.setFetchSize(100)
      val expectedStoppedNum = 1000
      statement.executeQuery(s"create table table1 as select id from range($expectedStoppedNum)")
      // check whole query result
      val res = statement.executeQuery("select * from table1")
      var numRecords = 0
      while (res.next()) {
        assert(res.getLong(1) == numRecords)
        numRecords += 1
      }
      assert(numRecords == expectedStoppedNum)
    }
  }

  ignore("[invalid] change rowSetSize while fetching results") {
    withJdbcStatement("table1") { statement =>
      statement.setFetchSize(100)
      val expectedStoppedNum = 1000
      statement.executeQuery(s"create table table1 as select id from range($expectedStoppedNum)")
      val res = statement.executeQuery("select * from table1")
      var numRecords = 0
      while (res.next()) {
        assert(res.getLong(1) == numRecords)
        numRecords += 1
        if (numRecords == 200) {
          statement.setFetchSize(200)
        }
      }
      assert(numRecords == expectedStoppedNum)
    }
  }

  ignore("fetch record with specified location while fetching results") {
    withJdbcStatement("table1") { statement =>
      statement.setFetchSize(100)
      val expectedStoppedNum = 1000
      statement.executeQuery(s"create table table1 as select id from range($expectedStoppedNum)")
      val res = statement.executeQuery("select * from table1")
      var numRecords = 0
      while (res.next()) {
        assert(res.getLong(1) == numRecords)
        numRecords += 1
        if (numRecords == 200) {
          // absolute method is not supported in kyuubi hive driver
          res.absolute(0)
          numRecords = 0
        }
      }
      assert(numRecords == expectedStoppedNum)
    }
  }

  ignore("change orientation while fetching results") {
    withJdbcStatement("table1") { statement =>
      statement.setFetchSize(100)
      // setFetchDirection method is not supported in kyuubi hive driver
      statement.setFetchDirection(ResultSet.FETCH_REVERSE)
      val expectedStoppedNum = 1000
      statement.executeQuery(s"create table table1 as select id from range($expectedStoppedNum)")
      val res = statement.executeQuery("select * from table1")
      var numRecords = 0
      while (res.next()) {
        assert(res.getLong(1) == numRecords)
        numRecords += 1
      }
      assert(numRecords == expectedStoppedNum)
    }
  }

  test("test fetch orientation with prefetch") {
    val sql = "SELECT id FROM range(1000)"

    withSessionConf(Map())()() {
      withSessionHandle { (client, handle) =>
        val req = new TExecuteStatementReq()
        req.setSessionHandle(handle)
        req.setStatement(sql)

        val tExecuteStatementResp = client.ExecuteStatement(req)
        val opHandle = tExecuteStatementResp.getOperationHandle
        waitForOperationToComplete(client, opHandle)
        val fetchSize: Int = 300

        // fetch next from before first row
        val tFetchResultsReq1 =
          new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_NEXT, fetchSize)
        val tFetchResultsResp1 = client.FetchResults(tFetchResultsReq1)
        assert(tFetchResultsResp1.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
        val idSeq1 = tFetchResultsResp1.getResults.getColumns.get(0)
          .getI64Val.getValues.asScala
        assertResult(Seq.range(0, fetchSize))(idSeq1)

        // fetch next from first rowSet
        val tFetchResultsReq2 =
          new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_NEXT, fetchSize)
        val tFetchResultsResp2 = client.FetchResults(tFetchResultsReq2)
        assert(tFetchResultsResp2.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
        val idSeq2 = tFetchResultsResp2.getResults.getColumns.get(0)
          .getI64Val.getValues.asScala
        assertResult(Seq.range(fetchSize, fetchSize * 2))(idSeq2)

        // fetch prior from second rowSet, expected got first rowSet
        val tFetchResultsReq3 =
          new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_PRIOR, fetchSize)
        val tFetchResultsResp3 = client.FetchResults(tFetchResultsReq3)
        assert(tFetchResultsResp3.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)

        // fetch first
        val tFetchResultsReq4 =
          new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_FIRST, fetchSize)
        val tFetchResultsResp4 = client.FetchResults(tFetchResultsReq4)
        assert(tFetchResultsResp4.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      }
    }
  }

  test("change rowSetSize while fetching results") {
    val sql = "SELECT id FROM range(1000)"

    withSessionConf(Map(KyuubiConf.OPERATION_RESULT_PREFETCH.key -> "true"))()() {
      withSessionHandle { (client, handle) =>
        val req = new TExecuteStatementReq()
        req.setSessionHandle(handle)
        req.setStatement(sql)

        val tExecuteStatementResp = client.ExecuteStatement(req)
        val opHandle = tExecuteStatementResp.getOperationHandle
        waitForOperationToComplete(client, opHandle)
        val fetchSize: Int = 300

        // fetch next from before first row
        val tFetchResultsReq1 =
          new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_NEXT, fetchSize)
        val tFetchResultsResp1 = client.FetchResults(tFetchResultsReq1)
        assert(tFetchResultsResp1.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
        val idSeq1 = tFetchResultsResp1.getResults.getColumns.get(0)
          .getI64Val.getValues.asScala
        assertResult(Seq.range(0, fetchSize))(idSeq1)

        // change rowSetSize(fetchSize) while second fetching
        val tFetchResultsReq2 =
          new TFetchResultsReq(opHandle, TFetchOrientation.FETCH_NEXT, fetchSize + 1)
        val tFetchResultsResp2 = client.FetchResults(tFetchResultsReq2)
        assert(tFetchResultsResp2.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)

      }
    }
  }
}
