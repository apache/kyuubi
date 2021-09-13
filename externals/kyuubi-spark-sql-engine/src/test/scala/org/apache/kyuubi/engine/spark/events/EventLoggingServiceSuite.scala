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

package org.apache.kyuubi.engine.spark.events

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.hive.service.rpc.thrift.TExecuteStatementReq
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.{KyuubiSparkUtil, WithSparkSQLEngine}
import org.apache.kyuubi.events.EventLoggerType._
import org.apache.kyuubi.events.JsonProtocol
import org.apache.kyuubi.operation.{JDBCTestUtils, OperationHandle}

class EventLoggingServiceSuite extends WithSparkSQLEngine with JDBCTestUtils {

  private val logRoot = "file:" + Utils.createTempDir().toString
  private val currentDate = Utils.getDateFromTimestamp(System.currentTimeMillis())

  override def withKyuubiConf: Map[String, String] = Map(
    KyuubiConf.ENGINE_EVENT_LOGGERS.key -> s"$JSON,$SPARK",
    KyuubiConf.ENGINE_EVENT_JSON_LOG_PATH.key -> logRoot,
    "spark.eventLog.enabled" -> "true",
    "spark.eventLog.dir" -> logRoot
  )

  override protected def jdbcUrl: String = getJdbcUrl

  test("round-trip for event logging service") {

//    val rootLog = logRoot.replace("file:", "")
//    val engineEventPath = Paths.get(
//      rootLog.toString, "engine", s"day=$currentDate", KyuubiSparkUtil.engineId + ".json")

    val engineEventPath = Paths.get(
      logRoot.toString, "engine", s"day=$currentDate", KyuubiSparkUtil.engineId + ".json")
    val sessionEventPath = Paths.get(
      logRoot.toString, "session", s"day=$currentDate", KyuubiSparkUtil.engineId + ".json")
    val engineEventReader = Files.newBufferedReader(engineEventPath, StandardCharsets.UTF_8)

    val readEvent = JsonProtocol.jsonToEvent(engineEventReader.readLine(),
      classOf[KyuubiSparkEvent])
    assert(readEvent.isInstanceOf[KyuubiSparkEvent])

    withJdbcStatement() { statement =>
      val table = engineEventPath.getParent
      val resultSet = statement.executeQuery(s"SELECT * FROM `json`.`${table}`")
      while (resultSet.next()) {
        assert(resultSet.getString("Event") === classOf[EngineEvent].getCanonicalName)
        assert(resultSet.getString("applicationId") === spark.sparkContext.applicationId)
        assert(resultSet.getString("master") === spark.sparkContext.master)
      }
      val table2 = table.getParent
      val rs2 = statement.executeQuery(s"SELECT * FROM `json`.`${table2}`" +
        s" where Event = '${classOf[EngineEvent].getCanonicalName}'")
      while (rs2.next()) {
        assert(rs2.getString("Event") === classOf[EngineEvent].getCanonicalName)
        assert(rs2.getString("applicationId") === spark.sparkContext.applicationId)
        assert(rs2.getString("master") === spark.sparkContext.master)
      }

      val table3 = sessionEventPath.getParent
      val rs3 = statement.executeQuery(s"SELECT * FROM `json`.`${table3}`")
      while (rs3.next()) {
        assert(rs3.getString("Event") === classOf[SessionEvent].getCanonicalName)
        assert(rs3.getString("username") === Utils.currentUser)
        assert(rs3.getString("engineId") === spark.sparkContext.applicationId)
        assert(rs3.getInt("totalOperations") === 0,
          "update num of operations after session close as statement event will track these")
      }
    }

    withJdbcStatement() { statement =>
      val rs = statement.executeQuery(s"SELECT * FROM `json`.`${sessionEventPath.getParent}`" +
        " where totalOperations > 0")
      assert(rs.next())
      // there 3 statements executed above
      assert(rs.getInt("totalOperations") === 3)
    }
  }

  test("statementEvent: generate, dump and query") {
    val statementEventPath = Paths.get(
      logRoot, "spark_statement", s"day=$currentDate", engine.engineId + ".json")
    val sql = "select timestamp'2021-09-01';"
    withSessionHandle { (client, handle) =>

      val table = statementEventPath.getParent
      val req = new TExecuteStatementReq()
      req.setSessionHandle(handle)
      req.setStatement(sql)
      val tExecuteStatementResp = client.ExecuteStatement(req)
      val opHandle = tExecuteStatementResp.getOperationHandle
      val statementId = OperationHandle(opHandle).identifier.toString

      eventually(timeout(60.seconds), interval(5.seconds)) {
        val result = spark.sql(s"select * from `json`.`${table}`")
          .where(s"statementId = '${statementId}'")

        assert(result.select("statementId").first().get(0) === statementId)
        assert(result.count() >= 1)
        assert(result.select("statement").first().get(0) === sql)
      }
    }
  }
}
