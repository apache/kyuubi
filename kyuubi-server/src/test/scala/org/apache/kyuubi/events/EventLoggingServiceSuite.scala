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

package org.apache.kyuubi.events

import java.net.InetAddress
import java.nio.file.Paths
import java.util.UUID

import org.apache.kyuubi.{Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.JDBCTestUtils
import org.apache.kyuubi.operation.OperationState._

class EventLoggingServiceSuite extends WithKyuubiServer with JDBCTestUtils {

  private val logRoot = "file:" + Utils.createTempDir().toString
  private val currentDate = Utils.getDateFromTimestamp(System.currentTimeMillis())

  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(KyuubiConf.SERVER_EVENT_LOGGERS, Seq("JSON"))
      .set(KyuubiConf.SERVER_EVENT_JSON_LOG_PATH, logRoot)
      .set(KyuubiConf.ENGINE_EVENT_LOGGERS, Seq("JSON"))
      .set(KyuubiConf.ENGINE_EVENT_JSON_LOG_PATH, logRoot)
  }

  override protected def jdbcUrl: String = getJdbcUrl

  test("statementEvent: generate, dump and query") {
    val hostName = InetAddress.getLocalHost.getCanonicalHostName
    val serverStatementEventPath =
      Paths.get(logRoot, "kyuubi_statement", s"day=$currentDate", s"server-$hostName.json")
    val engineStatementEventPath =
      Paths.get(logRoot, "spark_statement", s"day=$currentDate", "*.json")
    val sql = "select timestamp'2021-06-01'"

    withJdbcStatement() { statement =>
      statement.execute(sql)

      // check server statement events
      val serverTable = serverStatementEventPath.getParent
      val resultSet = statement.executeQuery(s"SELECT * FROM `json`.`${serverTable}`" +
        "where statement = \"" + sql + "\"")
      val states = Array(INITIALIZED, PENDING, RUNNING, FINISHED, CLOSED)
      var stateIndex = 0
      while (resultSet.next()) {
        assert(resultSet.getString("user") == Utils.currentUser)
        assert(resultSet.getString("statement") == sql)
        assert(resultSet.getString("state") == states(stateIndex).toString)
        stateIndex += 1
      }

      // check engine statement events
      val engineTable = engineStatementEventPath.getParent
      val resultSet2 = statement.executeQuery(s"SELECT * FROM `json`.`${engineTable}`" +
        "where statement = \"" + sql + "\"")
      val engineStates = Array(INITIALIZED, PENDING, RUNNING, COMPILED, FINISHED)
      stateIndex = 0
      while (resultSet2.next()) {
        assert(resultSet2.getString("Event") ==
          "org.apache.kyuubi.engine.spark.events.SparkStatementEvent")
        assert(resultSet2.getString("statement") == sql)
        assert(resultSet2.getString("state") == engineStates(stateIndex).toString)
        stateIndex += 1
      }
    }
  }

  test("test Kyuubi session event") {
    withSessionConf()(Map.empty)(Map(KyuubiConf.SESSION_NAME.key -> "test1")) {
      withJdbcStatement() { statement =>
        statement.execute("SELECT 1")
      }
    }

    val eventPath =
      Paths.get(logRoot, "kyuubi_session", s"day=$currentDate")
    withSessionConf()(Map.empty)(Map("spark.sql.shuffle.partitions" -> "2")) {
      withJdbcStatement() { statement =>
        val res = statement.executeQuery(
          s"SELECT * FROM `json`.`$eventPath` where sessionName = 'test1' order by totalOperations")
        assert(res.next())
        assert(res.getString("user") == Utils.currentUser)
        assert(res.getString("sessionName") == "test1")
        assert(res.getString("sessionId") == "")
        assert(res.getLong("startTime") > 0)
        assert(res.getInt("totalOperations") == 0)
        assert(res.next())
        assert(res.getInt("totalOperations") == 0)
        assert(res.getString("sessionId") != "")
        assert(res.getLong("openedTime") > 0)
        assert(res.next())
        assert(res.getInt("totalOperations") == 1)
        assert(res.getLong("endTime") > 0)
        assert(!res.next())
      }
    }
  }

  test("engine session id should be same with server session id") {
    val name = UUID.randomUUID().toString
    withSessionConf()(Map.empty)(Map(KyuubiConf.SESSION_NAME.key -> name)) {
      withJdbcStatement() { statement =>
        statement.execute("SELECT 1")
      }
    }

    val serverSessionEventPath =
      Paths.get(logRoot, "kyuubi_session", s"day=$currentDate")
    val engineSessionEventPath =
      Paths.get(logRoot, "session", s"day=$currentDate")
    withSessionConf()(Map.empty)(Map.empty) {
      withJdbcStatement() { statement =>
        val res = statement.executeQuery(
          s"SELECT * FROM `json`.`$serverSessionEventPath` " +
            s"where sessionName = '$name' and sessionId != '' limit 1")
        assert(res.next())
        val serverSessionId = res.getString("sessionId")
        assert(!res.next())

        val res2 = statement.executeQuery(
          s"SELECT * FROM `json`.`$engineSessionEventPath` " +
            s"where sessionId = '$serverSessionId' limit 1")
        assert(res2.next())
      }
    }
  }
}
