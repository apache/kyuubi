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

import java.sql.SQLException
import java.util
import java.util.{Properties, UUID}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{KYUUBI_VERSION, Utils, WithKyuubiServer}
import org.apache.kyuubi.config.{KyuubiConf, KyuubiReservedKeys}
import org.apache.kyuubi.config.KyuubiConf.SESSION_CONF_ADVISOR
import org.apache.kyuubi.engine.{ApplicationInfo, ApplicationManagerInfo, ApplicationState, KyuubiApplicationManager}
import org.apache.kyuubi.jdbc.KyuubiHiveDriver
import org.apache.kyuubi.jdbc.hive.{KyuubiConnection, KyuubiSQLException, KyuubiStatement}
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.plugin.SessionConfAdvisor
import org.apache.kyuubi.session.{KyuubiSessionImpl, KyuubiSessionManager, SessionHandle, SessionType}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.shaded.thrift.transport.TTransportException

/**
 * UT with Connection level engine shared cost much time, only run basic jdbc tests.
 */
class KyuubiOperationPerConnectionSuite extends WithKyuubiServer with HiveJDBCTestHelper {

  override protected def jdbcUrl: String =
    s"jdbc:kyuubi://${server.frontendServices.head.connectionUrl}/;"
  override protected val URL_PREFIX: String = "jdbc:kyuubi://"

  override protected val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.ENGINE_SHARE_LEVEL, "connection")
      .set(SESSION_CONF_ADVISOR.key, classOf[TestSessionConfAdvisor].getName)
      .set(KyuubiConf.ENGINE_SPARK_MAX_INITIAL_WAIT.key, "0")
  }

  test("KYUUBI #647 - async query causes engine crash") {
    withSessionHandle { (client, handle) =>
      val executeStmtReq = new TExecuteStatementReq()
      executeStmtReq.setStatement("select java_method('java.lang.System', 'exit', 1)")
      executeStmtReq.setSessionHandle(handle)
      executeStmtReq.setRunAsync(true)
      val executeStmtResp = client.ExecuteStatement(executeStmtReq)

      // TODO KYUUBI #745
      eventually(timeout(60.seconds), interval(500.milliseconds)) {
        val getOpStatusReq = new TGetOperationStatusReq(executeStmtResp.getOperationHandle)
        val getOpStatusResp = client.GetOperationStatus(getOpStatusReq)
        assert(getOpStatusResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
        assert(getOpStatusResp.getOperationState === TOperationState.ERROR_STATE)
      }
    }
  }

  test("sync query causes engine crash") {
    withSessionHandle { (client, handle) =>
      val executeStmtReq = new TExecuteStatementReq()
      executeStmtReq.setStatement("select java_method('java.lang.System', 'exit', 1)")
      executeStmtReq.setSessionHandle(handle)
      executeStmtReq.setRunAsync(false)
      val executeStmtResp = client.ExecuteStatement(executeStmtReq)
      assert(executeStmtResp.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
      assert(executeStmtResp.getOperationHandle === null)
      val errMsg = executeStmtResp.getStatus.getErrorMessage
      assert(errMsg.contains("Caused by: java.net.SocketException: Connection reset") ||
        errMsg.contains(s"Socket for ${SessionHandle(handle)} is closed") ||
        errMsg.contains("Socket is closed by peer") ||
        errMsg.contains("SparkContext was shut down") ||
        errMsg.contains("DAGScheduler.cleanupQueryJobs"))
    }
  }

  test("test asynchronous open kyuubi session") {
    withSessionConf(Map(
      KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "true"))(Map.empty)(Map.empty) {
      withSessionAndLaunchEngineHandle { (client, handle, launchOpHandleOpt) =>
        assert(launchOpHandleOpt.isDefined)
        val launchOpHandle = launchOpHandleOpt.get
        val executeStmtReq = new TExecuteStatementReq
        executeStmtReq.setStatement("select engine_name()")
        executeStmtReq.setSessionHandle(handle)
        executeStmtReq.setRunAsync(false)
        val executeStmtResp = client.ExecuteStatement(executeStmtReq)
        val getOpStatusReq = new TGetOperationStatusReq(executeStmtResp.getOperationHandle)
        val getOpStatusResp = client.GetOperationStatus(getOpStatusReq)
        assert(getOpStatusResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
        assert(getOpStatusResp.getOperationState === TOperationState.FINISHED_STATE)

        val launchEngineResp = client.GetOperationStatus(new TGetOperationStatusReq(launchOpHandle))
        assert(launchEngineResp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
        assert(getOpStatusResp.getOperationState === TOperationState.FINISHED_STATE)
      }
    }
  }

  test("test asynchronous open kyuubi session failure") {
    withSessionConf(Map(
      KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "true",
      "spark.master" -> "invalid"))(Map.empty)(Map.empty) {
      withSessionAndLaunchEngineHandle { (client, handle, launchOpHandleOpt) =>
        assert(launchOpHandleOpt.isDefined)
        val launchOpHandle = launchOpHandleOpt.get
        val executeStmtReq = new TExecuteStatementReq
        executeStmtReq.setStatement("select engine_name()")
        executeStmtReq.setSessionHandle(handle)
        executeStmtReq.setRunAsync(false)
        val executeStmtResp = client.ExecuteStatement(executeStmtReq)
        assert(executeStmtResp.getStatus.getStatusCode == TStatusCode.ERROR_STATUS)
        assert(executeStmtResp.getStatus.getErrorMessage.contains("kyuubi-spark-sql-engine.log"))

        val launchEngineResp = client.GetOperationStatus(new TGetOperationStatusReq(launchOpHandle))
        assert(launchEngineResp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)
        assert(launchEngineResp.getOperationState == TOperationState.ERROR_STATE)
      }
    }
  }

  test("open session with KyuubiConnection") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "true",
      "spark.ui.enabled" -> "true")) {
      val driver = new KyuubiHiveDriver()
      val connection = driver.connect(jdbcUrlWithConf, new Properties())
        .asInstanceOf[KyuubiConnection]
      assert(connection.getEngineId.startsWith("local-"))
      assert(connection.getEngineName.startsWith("kyuubi"))
      assert(connection.getEngineUrl.nonEmpty)
      assert(connection.getEngineRefId.nonEmpty)
      val stmt = connection.createStatement()
      try {
        stmt.execute("select engine_name()")
        val resultSet = stmt.getResultSet
        assert(resultSet.next())
        assert(resultSet.getString(1).nonEmpty)
      } finally {
        stmt.close()
        connection.close()
      }
    }

    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "false")) {
      val driver = new KyuubiHiveDriver()
      val connection = driver.connect(jdbcUrlWithConf, new Properties())

      val stmt = connection.createStatement()
      try {
        stmt.execute("select engine_name()")
        val resultSet = stmt.getResultSet
        assert(resultSet.next())
        assert(resultSet.getString(1).nonEmpty)
      } finally {
        stmt.close()
        connection.close()
      }
    }
  }

  test("support to specify OPERATION_LANGUAGE with confOverlay") {
    withSessionHandle { (client, handle) =>
      val executeStmtReq = new TExecuteStatementReq()
      executeStmtReq.setStatement("""spark.sql("SET kyuubi.operation.language").show(false)""")
      executeStmtReq.setSessionHandle(handle)
      executeStmtReq.setRunAsync(false)
      executeStmtReq.setConfOverlay(Map(KyuubiConf.OPERATION_LANGUAGE.key -> "SCALA").asJava)
      val executeStmtResp = client.ExecuteStatement(executeStmtReq)
      assert(executeStmtResp.getStatus.getStatusCode == TStatusCode.SUCCESS_STATUS)

      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(executeStmtResp.getOperationHandle)
      tFetchResultsReq.setFetchType(0)
      tFetchResultsReq.setMaxRows(10)
      val tFetchResultsResp = client.FetchResults(tFetchResultsReq)
      val resultSet = tFetchResultsResp.getResults.getColumns.asScala
      assert(resultSet.size == 1)
      assert(resultSet.head.getStringVal.getValues.get(0).contains("kyuubi.operation.language"))
    }
  }

  test("test session conf plugin") {
    withSessionConf()(Map())(Map("spark.k1" -> "v0", "spark.k3" -> "v4")) {
      withJdbcStatement() { statement =>
        val r1 = statement.executeQuery("set spark.k1")
        assert(r1.next())
        assert(r1.getString(2) == "v0")

        val r2 = statement.executeQuery("set spark.k3")
        assert(r2.next())
        assert(r2.getString(2) == "v3")

        val r3 = statement.executeQuery("set spark.k4")
        assert(r3.next())
        assert(r3.getString(2) == "v4")
      }
    }
  }

  test("close kyuubi connection on launch engine operation failure") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "true",
      "spark.master" -> "invalid")) {
      val prop = new Properties()
      prop.setProperty(KyuubiConnection.BEELINE_MODE_PROPERTY, "true")
      val kyuubiConnection = new KyuubiConnection(jdbcUrlWithConf, prop)
      intercept[SQLException](kyuubiConnection.waitLaunchEngineToComplete())
      assert(kyuubiConnection.isClosed)
    }
  }

  test("transfer the TGetInfoReq to kyuubi engine side to verify the connection valid") {
    withSessionConf(Map.empty)(Map(
      KyuubiConf.SERVER_INFO_PROVIDER.key -> "ENGINE",
      KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "false"))() {
      withJdbcStatement() { statement =>
        val conn = statement.getConnection.asInstanceOf[KyuubiConnection]
        assert(conn.isValid(3000))
        val sessionManager = server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
        eventually(timeout(10.seconds)) {
          assert(sessionManager.allSessions().size === 1)
        }
        val engineId = sessionManager.allSessions().head.handle.identifier.toString
        // kill the engine application and wait the engine terminate
        sessionManager.applicationManager.killApplication(ApplicationManagerInfo(None), engineId)
        eventually(timeout(30.seconds), interval(100.milliseconds)) {
          assert(sessionManager.applicationManager.getApplicationInfo(
            ApplicationManagerInfo(None),
            engineId)
            .exists(_.state == ApplicationState.NOT_FOUND))
        }
        assert(!conn.isValid(3000))
      }
    }
  }

  test("trace the connection metrics with session type") {
    val connOpenMetric = s"${MetricsConstants.CONN_OPEN}.${SessionType.INTERACTIVE}"
    val connTotalMetric = s"${MetricsConstants.CONN_TOTAL}.${SessionType.INTERACTIVE}"
    val connFailedMetric = s"${MetricsConstants.CONN_FAIL}.${SessionType.INTERACTIVE}"
    val connTotalCount = MetricsSystem.counterValue(connTotalMetric).getOrElse(0L)
    val connFailedCount = MetricsSystem.counterValue(connFailedMetric).getOrElse(0L)

    withJdbcStatement() { statement =>
      statement.executeQuery("select engine_name()")
    }
    eventually(timeout(5.seconds), interval(100.milliseconds)) {
      assert(MetricsSystem.counterValue(connTotalMetric).getOrElse(0L) > connTotalCount)
      assert(MetricsSystem.counterValue(connOpenMetric).getOrElse(0L) === 0)
    }

    withSessionConf(Map.empty)(Map.empty)(Map(
      KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "false",
      "spark.master" -> "invalid")) {
      intercept[Exception] {
        withJdbcStatement() { statement =>
          statement.executeQuery("select engine_name()")
        }
      }
    }

    eventually(timeout(5.seconds), interval(100.milliseconds)) {
      assert(MetricsSystem.counterValue(connTotalMetric).getOrElse(0L) - connTotalCount > 1)
      assert(MetricsSystem.counterValue(connOpenMetric).getOrElse(0L) === 0)
      assert(MetricsSystem.counterValue(connFailedMetric).getOrElse(0L) > connFailedCount)
    }
  }

  test("support to transfer client version when opening jdbc connection") {
    withJdbcStatement() { stmt =>
      val rs = stmt.executeQuery(s"set spark.${KyuubiReservedKeys.KYUUBI_CLIENT_VERSION_KEY}")
      assert(rs.next())
      assert(rs.getString(2) === KYUUBI_VERSION)
    }
  }

  test("JDBC client should catch task failed exception in the incremental mode") {
    withJdbcStatement() { statement =>
      statement.executeQuery(s"set ${KyuubiConf.OPERATION_INCREMENTAL_COLLECT.key}=true;")
      val resultSet = statement.executeQuery(
        "SELECT raise_error('client should catch this exception');")
      val e = intercept[KyuubiSQLException](resultSet.next())
      assert(e.getMessage.contains("client should catch this exception"))
    }
  }

  test("close session only for a confirmed terminated engine application") {
    withSessionConf(Map(
      KyuubiConf.ENGINE_ALIVE_PROBE_ENABLED.key -> "false"))(Map.empty)(
      Map.empty) {
      withSessionHandle { (client, handle) =>
        val preReq = new TExecuteStatementReq()
        preReq.setStatement("select engine_name()")
        preReq.setSessionHandle(handle)
        preReq.setRunAsync(false)
        client.ExecuteStatement(preReq)

        val sessionHandle = SessionHandle(handle)
        val sessionManager =
          server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
        val session =
          sessionManager.getSession(sessionHandle).asInstanceOf[KyuubiSessionImpl]
        val sessionEvent = session.getSessionEvent.get
        val originalApplicationManager = sessionManager.applicationManager
        var applicationInfo: () => Option[ApplicationInfo] =
          () => Some(ApplicationInfo("engine-id", "engine-name", ApplicationState.RUNNING))

        sessionManager.applicationManager = new KyuubiApplicationManager(None) {
          override def getApplicationInfo(
              appMgrInfo: ApplicationManagerInfo,
              tag: String,
              proxyUser: Option[String],
              submitTime: Option[Long]): Option[ApplicationInfo] = applicationInfo()
        }

        class TestExecuteStatement
          extends ExecuteStatement(session, "SELECT 1", Map.empty, false, 0L) {
          def fail(t: Throwable): Unit = {
            setState(OperationState.PENDING)
            onError()(t)
          }
        }

        def failWithTransportError(): org.apache.kyuubi.KyuubiSQLException = {
          val operation = new TestExecuteStatement
          try {
            intercept[org.apache.kyuubi.KyuubiSQLException] {
              operation.fail(new TTransportException("Socket is closed by peer"))
            }
          } finally {
            operation.close()
          }
        }

        try {
          val runningError = failWithTransportError()
          assert(!runningError.getMessage.contains("engine application has been terminated"))
          assert(sessionManager.getSessionOption(sessionHandle).nonEmpty)

          applicationInfo = () => Some(ApplicationInfo.NOT_FOUND)
          val notFoundError = failWithTransportError()
          assert(!notFoundError.getMessage.contains("engine application has been terminated"))
          assert(sessionManager.getSessionOption(sessionHandle).nonEmpty)

          applicationInfo = () => throw new RuntimeException("application lookup failed")
          val lookupError = failWithTransportError()
          assert(!lookupError.getMessage.contains("engine application has been terminated"))
          assert(sessionManager.getSessionOption(sessionHandle).nonEmpty)

          applicationInfo = () =>
            Some(ApplicationInfo(
              "engine-id",
              "engine-name",
              ApplicationState.FAILED,
              error = Some("driver terminated")))
          val engineError = failWithTransportError()
          assert(engineError.getMessage.contains("The engine application has been terminated"))
          assert(engineError.getMessage.contains("ApplicationInfo"))
          assert(engineError.getMessage.contains("driver terminated"))
          assert(sessionEvent.exception.contains(engineError))
          eventually(timeout(5.seconds), interval(100.milliseconds)) {
            assert(session.client.remoteEngineBroken)
            assert(session.client.engineConnectionClosed)
            assert(session.client.asyncRequestInterrupted)
            assert(sessionManager.getSessionOption(sessionHandle).isEmpty)
          }
        } finally {
          sessionManager.applicationManager = originalApplicationManager
        }
      }
    }
  }

  test("support to interrupt the thrift request if remote engine is broken") {
    withSessionConf(Map(
      KyuubiConf.ENGINE_ALIVE_PROBE_ENABLED.key -> "true",
      KyuubiConf.ENGINE_ALIVE_PROBE_INTERVAL.key -> "1000",
      KyuubiConf.ENGINE_ALIVE_TIMEOUT.key -> "1000"))(Map.empty)(
      Map.empty) {
      withSessionHandle { (client, handle) =>
        val preReq = new TExecuteStatementReq()
        preReq.setStatement("select engine_name()")
        preReq.setSessionHandle(handle)
        preReq.setRunAsync(false)
        client.ExecuteStatement(preReq)

        val sessionHandle = SessionHandle(handle)
        val sessionManager =
          server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]
        val session =
          sessionManager.getSession(sessionHandle).asInstanceOf[KyuubiSessionImpl]

        val exitReq = new TExecuteStatementReq()
        exitReq.setStatement("SELECT java_method('java.lang.Thread', 'sleep', 1000L)," +
          "java_method('java.lang.System', 'exit', 1)")
        exitReq.setSessionHandle(handle)
        exitReq.setRunAsync(true)
        client.ExecuteStatement(exitReq)

        session.sessionManager.getConf
          .set(KyuubiConf.OPERATION_STATUS_UPDATE_INTERVAL, 3000L)

        val executeStmtReq = new TExecuteStatementReq()
        executeStmtReq.setStatement("SELECT java_method('java.lang.Thread', 'sleep', 30000l)")
        executeStmtReq.setSessionHandle(handle)
        executeStmtReq.setRunAsync(false)
        val startTime = System.currentTimeMillis()
        val executeStmtResp = client.ExecuteStatement(executeStmtReq)
        assert(executeStmtResp.getStatus.getStatusCode === TStatusCode.ERROR_STATUS)
        val errorMsg = executeStmtResp.getStatus.getErrorMessage
        assert(errorMsg.contains("java.net.SocketException") ||
          errorMsg.contains("org.apache.kyuubi.shaded.thrift.transport.TTransportException") ||
          errorMsg.contains("connection does not exist") ||
          errorMsg.contains(s"Socket for ${SessionHandle(handle)} is closed") ||
          errorMsg.contains("Error submitting query in background, query rejected"))
        val elapsedTime = System.currentTimeMillis() - startTime
        assert(elapsedTime < 20 * 1000)
        eventually(timeout(3.seconds)) {
          assert(session.client.asyncRequestInterrupted)
        }
        eventually(timeout(15.seconds), interval(100.milliseconds)) {
          assert(sessionManager.getSessionOption(sessionHandle).isEmpty)
        }
      }
    }
  }

  test("Scala REPL should see jars added by spark.jars") {
    val jarDir = Utils.createTempDir().toFile
    val udfCode =
      """
        |package test.utils
        |
        |object Math {
        |  def add(x: Int, y: Int): Int = x + y
        |}
        |
        |""".stripMargin
    val jarFile = UserJarTestUtils.createJarFile(
      udfCode,
      "test",
      s"test-function-${UUID.randomUUID}.jar",
      jarDir.toString)
    val localPath = new Path(jarFile.getAbsolutePath)
    withSessionConf()(Map("spark.jars" -> localPath.toString))() {
      withJdbcStatement() { statement =>
        val kyuubiStatement = statement.asInstanceOf[KyuubiStatement]
        kyuubiStatement.executeScala("import test.utils.{Math => TMath}")
        val rs = kyuubiStatement.executeScala("println(TMath.add(1,2))")
        rs.next()
        assert(rs.getString(1) === "3")
      }
    }
  }
}

class TestSessionConfAdvisor extends SessionConfAdvisor {
  override def getConfOverlay(
      user: String,
      sessionConf: util.Map[String, String]): util.Map[String, String] = {
    Map("spark.k3" -> "v3", "spark.k4" -> "v4").asJava
  }
}
