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

import java.util.{Properties, UUID}

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.scalatest.time.SpanSugar._

import org.apache.kyuubi.{KYUUBI_VERSION, Utils, WithKyuubiServer, WithSimpleDFSService}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.KYUUBI_ENGINE_ENV_PREFIX
import org.apache.kyuubi.jdbc.KyuubiHiveDriver
import org.apache.kyuubi.jdbc.hive.{KyuubiConnection, KyuubiStatement}
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.session.{KyuubiSessionImpl, SessionHandle}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TExecuteStatementReq, TGetInfoReq, TGetInfoType, TStatusCode}
import org.apache.kyuubi.util.SemanticVersion
import org.apache.kyuubi.zookeeper.ZookeeperConf

class KyuubiOperationPerUserSuite
  extends WithKyuubiServer with SparkQueryTests with WithSimpleDFSService {

  override protected def jdbcUrl: String = getJdbcUrl

  override protected val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.ENGINE_SHARE_LEVEL, "user")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    conf.set(s"$KYUUBI_ENGINE_ENV_PREFIX.HADOOP_CONF_DIR", getHadoopConfDir)
  }

  test("audit Kyuubi server MetaData") {
    withSessionConf()(Map(KyuubiConf.SERVER_INFO_PROVIDER.key -> "SERVER"))(Map.empty) {
      withJdbcStatement() { statement =>
        val metaData = statement.getConnection.getMetaData
        assert(metaData.getDatabaseProductName === "Apache Kyuubi")
        assert(metaData.getDatabaseProductVersion === KYUUBI_VERSION)
        val ver = SemanticVersion(KYUUBI_VERSION)
        assert(metaData.getDatabaseMajorVersion === ver.majorVersion)
        assert(metaData.getDatabaseMinorVersion === ver.minorVersion)
      }
    }
  }

  test("kyuubi defined function - system_user/session_user") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT system_user(), session_user()")
      assert(rs.next())
      assert(rs.getString(1) === Utils.currentUser)
      assert(rs.getString(2) === Utils.currentUser)
    }
  }

  test("kyuubi defined function - engine_url") {
    withSessionConf(Map.empty)(Map.empty)(Map(
      "spark.ui.enabled" -> "true")) {
      val driver = new KyuubiHiveDriver()
      val connection = driver.connect(jdbcUrlWithConf, new Properties())
        .asInstanceOf[KyuubiConnection]
      val stmt = connection.createStatement()
      val rs = stmt.executeQuery("SELECT engine_url()")
      assert(rs.next())
      assert(rs.getString(1).nonEmpty)
    }
  }

  test("ensure two connections in user mode share the same engine") {
    var r1: String = null
    var r2: String = null
    new Thread {
      override def run(): Unit = withJdbcStatement() { statement =>
        val res = statement.executeQuery("set spark.app.name")
        assert(res.next())
        r1 = res.getString("value")
      }
    }.start()

    new Thread {
      override def run(): Unit = withJdbcStatement() { statement =>
        val res = statement.executeQuery("set spark.app.name")
        assert(res.next())
        r2 = res.getString("value")
      }
    }.start()

    eventually(timeout(120.seconds), interval(100.milliseconds)) {
      assert(r1 != null && r2 != null)
    }

    assert(r1 === r2)
  }

  test("ensure open session asynchronously for USER mode still share the same engine") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT engine_id()")
      assert(resultSet.next())
      val engineId = resultSet.getString(1)

      withSessionConf(Map(
        KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "true"))(Map.empty)(Map.empty) {
        withJdbcStatement() { stmt =>
          val rs = stmt.executeQuery("SELECT engine_id()")
          assert(rs.next())
          assert(rs.getString(1) == engineId)
        }
      }
    }
  }

  test("ensure two connections share the same engine when specifying subdomain.") {
    withSessionConf()(
      Map(
        KyuubiConf.ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "abc"))(Map.empty) {

      var r1: String = null
      var r2: String = null
      new Thread {
        override def run(): Unit = withJdbcStatement() { statement =>
          val res = statement.executeQuery("set spark.app.name")
          assert(res.next())
          r1 = res.getString("value")
        }
      }.start()

      new Thread {
        override def run(): Unit = withJdbcStatement() { statement =>
          val res = statement.executeQuery("set spark.app.name")
          assert(res.next())
          r2 = res.getString("value")
        }
      }.start()

      eventually(timeout(120.seconds), interval(100.milliseconds)) {
        assert(r1 != null && r2 != null)
      }

      assert(r1 === r2)
    }
  }

  test("ensure engine discovery works when mixed use subdomain") {
    var r1: String = null
    var r2: String = null
    withSessionConf()(Map.empty)(Map.empty) {
      withJdbcStatement() { statement =>
        val res = statement.executeQuery("set spark.app.name")
        assert(res.next())
        r1 = res.getString("value")
      }
    }
    assert(r1 contains "default")

    withSessionConf()(Map(KyuubiConf.ENGINE_SHARE_LEVEL_SUBDOMAIN.key -> "abc"))(Map.empty) {
      withJdbcStatement() { statement =>
        val res = statement.executeQuery("set spark.app.name")
        assert(res.next())
        r2 = res.getString("value")
      }
    }
    assert(r2 contains "abc")

    assert(r1 !== r2)
  }

  test("max result rows") {
    Seq("true", "false").foreach { incremental =>
      Seq("thrift", "arrow").foreach { resultFormat =>
        Seq("0", "1").foreach { maxResultRows =>
          withSessionConf()(Map.empty)(Map(
            KyuubiConf.OPERATION_RESULT_FORMAT.key -> resultFormat,
            KyuubiConf.OPERATION_RESULT_MAX_ROWS.key -> maxResultRows,
            KyuubiConf.OPERATION_INCREMENTAL_COLLECT.key -> incremental)) {
            withJdbcStatement("va") { statement =>
              statement.executeQuery("create temporary view va as select * from values(1),(2)")
              val resultLimit = statement.executeQuery("select * from va")
              assert(resultLimit.next())
              // always ignore max result rows on incremental collect mode
              if (incremental == "true" || maxResultRows == "0") assert(resultLimit.next())
              assert(!resultLimit.next())
            }
          }
        }
      }
    }
  }

  test("scala NPE issue with hdfs jar") {
    val jarDir = Utils.createTempDir().toFile
    val udfCode =
      """
        |package test.utils
        |
        |object Math {
        |def add(x: Int, y: Int): Int = x + y
        |}
        |
        |""".stripMargin
    val jarFile = UserJarTestUtils.createJarFile(
      udfCode,
      "test",
      s"test-function-${UUID.randomUUID}.jar",
      jarDir.toString)
    val hadoopConf = getHadoopConf
    val dfs = FileSystem.get(hadoopConf)
    val dfsJarDir = dfs.makeQualified(new Path(s"jars-${UUID.randomUUID()}"))
    val localFs = FileSystem.getLocal(hadoopConf)
    val localPath = new Path(jarFile.getAbsolutePath)
    val dfsJarPath = new Path(dfsJarDir, "test-function.jar")
    FileUtil.copy(localFs, localPath, dfs, dfsJarPath, false, false, hadoopConf)
    withJdbcStatement() { statement =>
      val kyuubiStatement = statement.asInstanceOf[KyuubiStatement]
      statement.executeQuery(s"add jar $dfsJarPath")
      val rs = kyuubiStatement.executeScala("println(test.utils.Math.add(1,2))")
      rs.next()
      assert(rs.getString(1) === "3")
    }
  }

  test("server info provider - server") {
    assume(!httpMode)
    withSessionConf(Map(KyuubiConf.SERVER_INFO_PROVIDER.key -> "SERVER"))()() {
      withSessionHandle { (client, handle) =>
        val req = new TGetInfoReq()
        req.setSessionHandle(handle)
        req.setInfoType(TGetInfoType.CLI_DBMS_NAME)
        assert(client.GetInfo(req).getInfoValue.getStringValue === "Apache Kyuubi")
      }
    }
  }

  test("server info provider - engine") {
    assume(!httpMode)
    withSessionConf(Map(KyuubiConf.SERVER_INFO_PROVIDER.key -> "ENGINE"))()() {
      withSessionHandle { (client, handle) =>
        val req = new TGetInfoReq()
        req.setSessionHandle(handle)
        req.setInfoType(TGetInfoType.CLI_DBMS_NAME)
        assert(client.GetInfo(req).getInfoValue.getStringValue === "Spark SQL")
      }
    }
  }

  test("the new client should work properly when the engine exits unexpectedly") {
    assume(!httpMode)
    withSessionConf(Map(
      ZookeeperConf.ZK_MAX_SESSION_TIMEOUT.key -> "10000"))(Map.empty)(
      Map.empty) {
      withSessionHandle { (client, handle) =>
        val preReq = new TExecuteStatementReq()
        preReq.setStatement("SET kyuubi.operation.language=scala")
        preReq.setSessionHandle(handle)
        preReq.setRunAsync(false)
        client.ExecuteStatement(preReq)

        val exitReq = new TExecuteStatementReq()
        // force kill engine without shutdown hook
        exitReq.setStatement("java.lang.Runtime.getRuntime().halt(-1)")
        exitReq.setSessionHandle(handle)
        exitReq.setRunAsync(false)
        client.ExecuteStatement(exitReq)
      }
      withSessionHandle { (client, handle) =>
        val preReq = new TExecuteStatementReq()
        preReq.setStatement("select engine_name()")
        preReq.setSessionHandle(handle)
        preReq.setRunAsync(false)
        val tExecuteStatementResp = client.ExecuteStatement(preReq)
        val opHandle = tExecuteStatementResp.getOperationHandle
        waitForOperationToComplete(client, opHandle)
        assert(tExecuteStatementResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      }
    }
  }

  test("transfer connection url when opening connection") {
    withJdbcStatement() { _ =>
      val session =
        server.backendService.sessionManager.allSessions().head.asInstanceOf[KyuubiSessionImpl]
      assert(session.connectionUrl == server.frontendServices.head.connectionUrl)
    }
  }

  test("remove spark.kyuubi.engine.credentials") {
    withJdbcStatement() { statement =>
      val result = statement.executeQuery("set spark.kyuubi.engine.credentials")
      assert(result.next())
      assert(result.getString(1) === "spark.kyuubi.engine.credentials")
      assert(result.getString(2).isEmpty)
      assert(!result.next())
    }
  }

  test("accumulate the operation terminal state") {
    val opType = classOf[ExecuteStatement].getSimpleName
    val finishedMetric = s"${MetricsConstants.OPERATION_STATE}.$opType" +
      s".${OperationState.FINISHED.toString.toLowerCase}"
    val closedMetric = s"${MetricsConstants.OPERATION_STATE}.$opType" +
      s".${OperationState.CLOSED.toString.toLowerCase}"
    val finishedCount = MetricsSystem.meterValue(finishedMetric).getOrElse(0L)
    val closedCount = MetricsSystem.meterValue(finishedMetric).getOrElse(0L)
    withJdbcStatement() { statement =>
      statement.executeQuery("select engine_name()")
    }
    eventually(timeout(5.seconds), interval(100.milliseconds)) {
      assert(MetricsSystem.meterValue(finishedMetric).getOrElse(0L) > finishedCount)
      assert(MetricsSystem.meterValue(closedMetric).getOrElse(0L) > closedCount)
    }
  }

  test("trace ExecuteStatement exec time histogram") {
    withJdbcStatement() { statement =>
      statement.executeQuery("select engine_name()")
    }
    val metric =
      s"${MetricsConstants.OPERATION_EXEC_TIME}.${classOf[ExecuteStatement].getSimpleName}"
    val snapshot = MetricsSystem.histogramSnapshot(metric).get
    assert(snapshot.getMax > 0 && snapshot.getMedian > 0)
  }

  test("align the server/engine session/executeStatement handle for Spark engine") {
    withSessionConf(Map(
      KyuubiConf.SESSION_ENGINE_LAUNCH_ASYNC.key -> "false"))(Map.empty)(Map.empty) {
      withJdbcStatement() { _ =>
        val session =
          server.backendService.sessionManager.allSessions().head.asInstanceOf[KyuubiSessionImpl]
        eventually(timeout(10.seconds)) {
          assert(session.handle === SessionHandle.apply(session.client.remoteSessionHandle))
        }

        def checkOpHandleAlign(statement: String, confOverlay: Map[String, String]): Unit = {
          val opHandle = session.executeStatement(statement, confOverlay, true, 0L)
          eventually(timeout(10.seconds)) {
            val operation = session.sessionManager.operationManager.getOperation(
              opHandle).asInstanceOf[KyuubiOperation]
            assert(opHandle == OperationHandle.apply(operation.remoteOpHandle()))
          }
        }

        val statement = "SELECT engine_id()"

        val confOverlay = Map(KyuubiConf.OPERATION_PLAN_ONLY_MODE.key -> "PARSE")
        checkOpHandleAlign(statement, confOverlay)

        Map(
          statement -> "SQL",
          s"""spark.sql("$statement")""" -> "SCALA",
          s"spark.sql('$statement')" -> "PYTHON").foreach { case (statement, lang) =>
          val confOverlay = Map(KyuubiConf.OPERATION_LANGUAGE.key -> lang)
          checkOpHandleAlign(statement, confOverlay)
        }
      }
    }
  }

  test("support to expose kyuubi operation metrics") {
    withSessionConf()(Map.empty)(Map.empty) {
      withJdbcStatement() { statement =>
        val uuid = UUID.randomUUID().toString
        val query = s"select '$uuid'"
        val res = statement.executeQuery(query)
        assert(res.next())
        assert(!res.next())

        val operationMetrics =
          server.backendService.sessionManager.operationManager.allOperations()
            .map(_.asInstanceOf[KyuubiOperation])
            .filter(_.statement == query)
            .head.metrics
        assert(operationMetrics.get("fetchResultsCount") == Some("1"))
        assert(operationMetrics.get("fetchLogCount") == Some("0"))
      }
    }
  }
}
