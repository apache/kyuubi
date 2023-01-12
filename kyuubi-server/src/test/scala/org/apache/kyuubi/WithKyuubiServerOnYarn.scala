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

package org.apache.kyuubi

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiConf.FrontendProtocols.FrontendProtocol
import org.apache.kyuubi.engine.ApplicationState._
import org.apache.kyuubi.engine.YarnApplicationOperation
import org.apache.kyuubi.operation.{FetchOrientation, HiveJDBCTestHelper, OperationState}
import org.apache.kyuubi.operation.OperationState.ERROR
import org.apache.kyuubi.server.MiniYarnService
import org.apache.kyuubi.session.{KyuubiBatchSessionImpl, KyuubiSessionManager}

/**
 * To developers:
 *   You should specify JAVA_HOME before running test with mini yarn server. Otherwise the error
 * may be thrown `/bin/bash: /bin/java: No such file or directory`.
 */
sealed trait WithKyuubiServerOnYarn extends WithKyuubiServer {

  protected lazy val yarnOperation: YarnApplicationOperation = {
    val operation = new YarnApplicationOperation()
    operation.initialize(miniYarnService.getConf)
    operation
  }

  protected var miniYarnService: MiniYarnService = _

  override def beforeAll(): Unit = {
    conf.set("spark.master", "yarn")
      .set("spark.executor.instances", "1")
    miniYarnService = new MiniYarnService()
    miniYarnService.initialize(conf)
    miniYarnService.start()
    conf.set(s"$KYUUBI_ENGINE_ENV_PREFIX.HADOOP_CONF_DIR", miniYarnService.getHadoopConfDir)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    // stop kyuubi server
    // stop yarn operation client
    // stop yarn cluster
    super.afterAll()
    yarnOperation.stop()
    if (miniYarnService != null) {
      miniYarnService.stop()
      miniYarnService = null
    }
  }
}

class KyuubiOperationYarnClusterSuite extends WithKyuubiServerOnYarn with HiveJDBCTestHelper
  with BatchTestHelper {

  override protected val frontendProtocols: Seq[FrontendProtocol] =
    FrontendProtocols.THRIFT_BINARY :: FrontendProtocols.REST :: Nil

  override protected val conf: KyuubiConf = {
    new KyuubiConf()
      .set(s"$KYUUBI_BATCH_CONF_PREFIX.spark.spark.master", "yarn")
      .set(BATCH_CONF_IGNORE_LIST, Seq("spark.master"))
      .set(BATCH_APPLICATION_CHECK_INTERVAL, 3000L)
  }

  override protected def jdbcUrl: String = getJdbcUrl

  test("KYUUBI #527- Support test with mini yarn cluster") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("""SELECT "${spark.app.id}" as id""")
      assert(resultSet.next())
      assert(resultSet.getString("id").startsWith("application_"))
    }
  }

  test("session_user shall work on yarn") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT SESSION_USER() as su")
      assert(resultSet.next())
      assert(resultSet.getString("su") === user)
    }
  }

  private def sessionManager: KyuubiSessionManager =
    server.backendService.sessionManager.asInstanceOf[KyuubiSessionManager]

  test("open batch session") {
    val batchRequest =
      newSparkBatchRequest(Map("spark.master" -> "local", "spark.executor.instances" -> "1"))

    val sessionHandle = sessionManager.openBatchSession(
      "kyuubi",
      "passwd",
      "localhost",
      batchRequest.getConf.asScala.toMap,
      batchRequest)

    val session = sessionManager.getSession(sessionHandle).asInstanceOf[KyuubiBatchSessionImpl]
    val batchJobSubmissionOp = session.batchJobSubmissionOp

    eventually(timeout(3.minutes), interval(50.milliseconds)) {
      val appInfo = batchJobSubmissionOp.currentApplicationInfo
      assert(appInfo.nonEmpty)
      assert(appInfo.exists(_.id.startsWith("application_")))
    }

    eventually(timeout(10.seconds)) {
      val metadata = session.sessionManager.getBatchMetadata(session.handle.identifier.toString)
      assert(metadata.state === "RUNNING")
      assert(metadata.engineId.startsWith("application_"))
    }

    val killResponse = yarnOperation.killApplicationByTag(sessionHandle.identifier.toString)
    assert(killResponse._1)
    assert(killResponse._2 startsWith "Succeeded to terminate:")

    val appInfo = yarnOperation.getApplicationInfoByTag(sessionHandle.identifier.toString)

    assert(appInfo.state === KILLED)

    eventually(timeout(10.minutes), interval(50.milliseconds)) {
      assert(batchJobSubmissionOp.getStatus.state === ERROR)
    }

    val resultColumns = batchJobSubmissionOp.getNextRowSet(FetchOrientation.FETCH_NEXT, 10)
      .getColumns.asScala

    val keys = resultColumns.head.getStringVal.getValues.asScala
    val values = resultColumns.apply(1).getStringVal.getValues.asScala
    val rows = keys.zip(values).toMap
    val appId = rows("id")
    val appName = rows("name")
    val appState = rows("state")
    val appUrl = rows("url")
    val appError = rows("error")

    val appInfo2 = batchJobSubmissionOp.currentApplicationInfo.get
    assert(appId === appInfo2.id)
    assert(appName === appInfo2.name)
    assert(appState === appInfo2.state.toString)
    assert(appUrl === appInfo2.url.orNull)
    assert(appError === appInfo2.error.orNull)
    sessionManager.closeSession(sessionHandle)
  }

  test("prevent dead loop if the batch job submission process it not alive") {
    val batchRequest = newSparkBatchRequest(Map("spark.submit.deployMode" -> "invalid"))

    val sessionHandle = sessionManager.openBatchSession(
      "kyuubi",
      "passwd",
      "localhost",
      batchRequest.getConf.asScala.toMap,
      batchRequest)

    val session = sessionManager.getSession(sessionHandle).asInstanceOf[KyuubiBatchSessionImpl]
    val batchJobSubmissionOp = session.batchJobSubmissionOp

    eventually(timeout(3.minutes), interval(50.milliseconds)) {
      assert(batchJobSubmissionOp.currentApplicationInfo.isEmpty)
      assert(batchJobSubmissionOp.getStatus.state === OperationState.ERROR)
    }
  }

  test("fast fail the kyuubi connection on engine terminated") {
    withSessionConf(Map.empty)(Map(
      "spark.master" -> "yarn",
      "spark.submit.deployMode" -> "cluster",
      "spark.sql.defaultCatalog=spark_catalog" -> "spark_catalog",
      "spark.sql.catalog.spark_catalog.type" -> "invalid_type",
      "kyuubi.session.engine.initialize.timeout" -> "PT10m"))(Map.empty) {
      val startTime = System.currentTimeMillis()
      val exception = intercept[Exception] {
        withJdbcStatement() { _ => }
      }
      val elapsedTime = System.currentTimeMillis() - startTime
      assert(elapsedTime < 60 * 1000)
      assert(exception.getMessage contains "The engine application has been terminated.")
    }
  }
}
