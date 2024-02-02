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

package org.apache.kyuubi.events.handler

import java.net.InetAddress
import java.nio.file.Paths
import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.matching.Regex

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi._
import org.apache.kyuubi.client.util.BatchUtils._
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.events.ServerEventHandlerRegister
import org.apache.kyuubi.metrics.MetricsConf
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.OperationState._
import org.apache.kyuubi.server.KyuubiServer
import org.apache.kyuubi.service.ServiceState
import org.apache.kyuubi.session.{KyuubiSessionManager, SessionType}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TOpenSessionReq, TStatusCode}

class ServerJsonLoggingEventHandlerSuite extends WithKyuubiServer with HiveJDBCTestHelper
  with BatchTestHelper {

  private val engineLogRoot = "file://" + Utils.createTempDir().toString
  private val serverLogRoot = "file://" + Utils.createTempDir().toString
  private val currentDate = Utils.getDateFromTimestamp(System.currentTimeMillis())

  private val fileSystem: FileSystem = FileSystem.get(new Configuration())
  fileSystem.delete(new Path(engineLogRoot), true)

  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(KyuubiConf.SERVER_EVENT_LOGGERS, Seq("JSON"))
      .set(KyuubiConf.SERVER_EVENT_JSON_LOG_PATH, serverLogRoot)
      .set(KyuubiConf.ENGINE_SPARK_EVENT_LOGGERS, Seq("JSON"))
      .set(KyuubiConf.ENGINE_EVENT_JSON_LOG_PATH, engineLogRoot)
      .set(MetricsConf.METRICS_REPORTERS, Set.empty[String])
  }

  override protected def jdbcUrl: String = getJdbcUrl

  test("round-trip for logging and querying statement events for both kyuubi server and engine") {
    val hostName = InetAddress.getLocalHost.getCanonicalHostName
    val serverStatementEventPath =
      Paths.get(serverLogRoot, "kyuubi_operation", s"day=$currentDate", s"server-$hostName.json")
    val engineStatementEventPath =
      Paths.get(engineLogRoot, "spark_operation", s"day=$currentDate", "*.json")
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
        assert(resultSet.getString("statement") === sql)
        assert(resultSet.getString("shouldRunAsync") === "true")
        assert(resultSet.getString("state") === states(stateIndex).name())
        assert(resultSet.getString("exception") === null)
        assert(resultSet.getString("sessionUser") === Utils.currentUser)
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
          "org.apache.kyuubi.engine.spark.events.SparkOperationEvent")
        assert(resultSet2.getString("statement") == sql)
        assert(resultSet2.getString("state") == engineStates(stateIndex).name())
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

    val serverSessionEventPath =
      Paths.get(serverLogRoot, "kyuubi_session", s"day=$currentDate")
    withSessionConf()(Map.empty)(Map("spark.sql.shuffle.partitions" -> "2")) {
      withJdbcStatement() { statement =>
        val res = statement.executeQuery(
          s"SELECT * FROM `json`.`$serverSessionEventPath` " +
            s"where sessionName = 'test1' order by totalOperations")
        assert(res.next())
        assert(res.getString("user") == Utils.currentUser)
        assert(res.getString("sessionName") == "test1")
        val sid = res.getString("sessionId")
        assert(res.getString("remoteSessionId") == "")
        assert(res.getLong("startTime") > 0)
        assert(res.getInt("totalOperations") == 0)
        assert(res.getString("sessionType") === SessionType.INTERACTIVE.toString)
        assert(res.next())
        assert(res.getInt("totalOperations") == 0)
        assert(res.getString("sessionId") == sid)
        assert(res.getString("remoteSessionId") != "")
        assert(res.getLong("openedTime") > 0)
        assert(res.next())
        assert(res.getInt("totalOperations") == 1)
        assert(res.getString("sessionType") === SessionType.INTERACTIVE.toString)
        assert(res.getLong("endTime") > 0)
        assert(!res.next())
      }
    }

    val batchRequest = newSparkBatchRequest(Map(KYUUBI_BATCH_ID_KEY -> UUID.randomUUID().toString))
    val sessionMgr = server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
    val batchSessionHandle = sessionMgr.openBatchSession(
      Utils.currentUser,
      "kyuubi",
      "127.0.0.1",
      batchRequest)
    withSessionConf()(Map.empty)(Map("spark.sql.shuffle.partitions" -> "2")) {
      withJdbcStatement() { statement =>
        val res = statement.executeQuery(
          s"SELECT * FROM `json`.`$serverSessionEventPath` " +
            s"where sessionName = '${sparkBatchTestAppName}' order by totalOperations")
        assert(res.next())
        assert(res.getString("user") == Utils.currentUser)
        assert(res.getString("sessionName") == sparkBatchTestAppName)
        assert(res.getString("sessionId") === batchSessionHandle.identifier.toString)
        assert(res.getString("remoteSessionId") == "")
        assert(res.getLong("startTime") > 0)
        assert(res.getInt("totalOperations") == 0)
        assert(res.getString("sessionType") === SessionType.BATCH.toString)
      }
    }
  }

  test("engine session id is same with server session id") {
    val name = UUID.randomUUID().toString
    withSessionConf()(Map.empty)(Map(KyuubiConf.SESSION_NAME.key -> name)) {
      withJdbcStatement() { statement =>
        statement.execute("SELECT 1")
      }
    }

    val serverSessionEventPath =
      Paths.get(serverLogRoot, "kyuubi_session", s"day=$currentDate")
    val engineSessionEventPath =
      Paths.get(engineLogRoot, "session", s"day=$currentDate")
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

  test("test Kyuubi server info event") {
    val confKv = List(("awesome.kyuubi", "true"), ("awesome.kyuubi.server", "yeah"))
    confKv.foreach(kv => conf.set(kv._1, kv._2))

    val name = "KyuubiServerInfoTest"
    val server = new KyuubiServer(name)
    server.initialize(conf)
    server.start()
    server.stop()
    ServerEventHandlerRegister.registerEventLoggers(conf) // register event loggers again

    val hostName = InetAddress.getLocalHost.getCanonicalHostName
    val kyuubiServerInfoPath =
      Paths.get(serverLogRoot, "kyuubi_server_info", s"day=$currentDate", s"server-$hostName.json")

    withJdbcStatement() { statement =>
      statement.executeQuery("set spark.sql.caseSensitive=true")
      val res = statement.executeQuery(
        s"SELECT * FROM `json`.`$kyuubiServerInfoPath` where serverName = '$name'")

      res.next()
      assert(res.getString("serverName") == name)
      assert(res.getLong("startTime") > 0)

      val startEventTime = res.getLong("eventTime")
      assert(res.getLong("startTime") <= startEventTime)
      assert(res.getString("state") == ServiceState.STARTED.toString)

      val serverIP = res.getString("serverIP")
      assert(serverIP != null && !"".equals(serverIP))

      var confIdx = 0
      List(""""awesome.kyuubi":"(.*?)"""".r, """"awesome.kyuubi.server":"(.*?)"""".r).foreach { p =>
        val matchConf = p.findFirstMatchIn(res.getString("serverConf"))
        assert(matchConf.isDefined)
        matchConf.foreach {
          case p(value) =>
            assert(value == confKv(confIdx)._2)
            confIdx += 1
        }
      }

      assert(res.getString("BUILD_USER") == BUILD_USER)
      assert(res.getString("BUILD_DATE") == BUILD_DATE)
      assert(res.getString("REPO_URL") == REPO_URL)

      val objMapper = new ObjectMapper()
      val versionInfoMap = objMapper.readTree(res.getString("VERSION_INFO"))
      assert(versionInfoMap.get("KYUUBI_VERSION").asText() == KYUUBI_VERSION)
      assert(versionInfoMap.get("JAVA_COMPILE_VERSION").asText() == JAVA_COMPILE_VERSION)
      assert(versionInfoMap.get("SCALA_COMPILE_VERSION").asText() == SCALA_COMPILE_VERSION)
      assert(versionInfoMap.get("HIVE_COMPILE_VERSION").asText() == HIVE_COMPILE_VERSION)
      assert(versionInfoMap.get("HADOOP_COMPILE_VERSION").asText() == HADOOP_COMPILE_VERSION)
      assert(res.getString("eventType") == "kyuubi_server_info")

      res.next()
      assert(res.getString("serverName") == name)
      assert(startEventTime <= res.getLong("eventTime"))
      assert(res.getString("state") == ServiceState.STOPPED.toString)
      assert(res.getString("BUILD_USER") == BUILD_USER)
      assert(res.getString("eventType") == "kyuubi_server_info")

      val USER = "USER"
      if (sys.env.contains(USER)) {
        val pattern: Regex = s""""$USER":"(.*?)"""".r
        val matchEnv = pattern.findFirstMatchIn(res.getString("serverEnv"))
        assert(matchEnv.isDefined)
        matchEnv.foreach {
          case pattern(usr) => assert(usr == sys.env(USER))
        }
      }
    }
  }

  test("open session exception") {
    val name = UUID.randomUUID().toString
    withSessionConf(Map(
      "spark.driver.memory" -> "abc",
      KyuubiConf.ENGINE_SHARE_LEVEL.key -> "CONNECTION",
      KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "false",
      KyuubiConf.SESSION_NAME.key -> name))()() {
      withThriftClient { client =>
        val req = new TOpenSessionReq()
        req.setUsername(Utils.currentUser)
        req.setPassword("anonymous")
        req.setConfiguration(sessionConfigs.asJava)
        val resp = client.OpenSession(req)
        assert(resp.getSessionHandle === null)
        assert(resp.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      }
    }

    eventually(timeout(2.minutes), interval(10.seconds)) {
      val serverSessionEventPath =
        Paths.get(serverLogRoot, "kyuubi_session", s"day=$currentDate")
      withJdbcStatement() { statement =>
        val res = statement.executeQuery(
          s"SELECT * FROM `json`.`$serverSessionEventPath` " +
            s"where sessionName = '$name' and exception is not null limit 1")
        assert(res.next())
        val exception = res.getObject("exception")
        assert(exception.toString.contains("Invalid maximum heap size: -Xmxabc"))
      }
    }
  }
}
