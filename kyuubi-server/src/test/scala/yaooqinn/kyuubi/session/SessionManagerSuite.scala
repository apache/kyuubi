/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.session

import java.io.File
import java.util.UUID

import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf, SparkFunSuite}

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.metrics.MetricsSystem
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.service.{ServiceException, State}
import yaooqinn.kyuubi.utils.ReflectUtils

class SessionManagerSuite extends SparkFunSuite {

  import KyuubiConf._

  private val conf = new SparkConf()
    .setMaster("local")
    .set(KyuubiConf.FRONTEND_BIND_PORT.key, "0")
  KyuubiSparkUtil.setupCommonConfig(conf)
  private val server = new KyuubiServer()
  server.init(conf)
  server.start()

  override def beforeAll(): Unit = {
    MetricsSystem.close()
    super.beforeAll()
  }

  test("init operation log") {
    val logRoot = UUID.randomUUID().toString
    val logRoot2 = logRoot + "/sub"
    val confClone = conf.clone()
      .set(KyuubiConf.LOGGING_OPERATION_ENABLED.key, "false")
        .set(KyuubiConf.LOGGING_OPERATION_LOG_DIR.key, logRoot)

    KyuubiSparkUtil.setupCommonConfig(confClone)

    val sessionManager = new SessionManager(server)
    assert(sessionManager.getName === classOf[SessionManager].getSimpleName)
    sessionManager.init(confClone)
    assert(!new File(logRoot).exists(), "Operation Log should be off")
    sessionManager.stop()

    val sessionManager2 = new SessionManager(server)
    confClone.set(KyuubiConf.LOGGING_OPERATION_ENABLED.key, "true")
    sessionManager2.init(confClone)
    assert(new File(logRoot).exists(), "Operation Log should be on")
    new File(logRoot).delete()
    sessionManager2.stop()

    new File(logRoot).createNewFile()
    val sessionManager3 = new SessionManager(server)
    sessionManager3.init(confClone)
    assert(ReflectUtils.getFieldValue(sessionManager3, "isOperationLogEnabled") === false)
    sessionManager3.stop()

    confClone.set(KyuubiConf.LOGGING_OPERATION_LOG_DIR.key, logRoot2)
    new File(logRoot).setWritable(false)
    val sessionManager4 = new SessionManager(server)
    sessionManager4.init(confClone)
    assert(ReflectUtils.getFieldValue(sessionManager4, "isOperationLogEnabled") === false)
    new File(logRoot).setWritable(true)
    assert(!new File(logRoot2).exists(), "Operation Log fails for not writable")
    sessionManager4.stop()
  }

  test("init resources root dir") {
    val confClone = conf.clone().set(KyuubiConf.LOGGING_OPERATION_ENABLED.key, "false")
    val sessionManager = new SessionManager(server)

    sessionManager.init(confClone)
    val resourcesRoot = new File(confClone.get(OPERATION_DOWNLOADED_RESOURCES_DIR))
    assert(resourcesRoot.exists())
    assert(resourcesRoot.isDirectory)
    resourcesRoot.delete()
    resourcesRoot.createNewFile()
    val e1 = intercept[ServiceException](sessionManager.init(confClone))
    assert(e1.getMessage.startsWith(
      "The operation downloaded resources directory exists but is not a directory"))
    assert(resourcesRoot.delete())
    resourcesRoot.getParentFile.setWritable(false)
    try {
      intercept[Exception](sessionManager.init(confClone))
    } finally {
      resourcesRoot.getParentFile.setWritable(true)
    }
  }

  test("start timeout checker") {
    val conf = new SparkConf().set(KyuubiConf.FRONTEND_SESSION_CHECK_INTERVAL.key, "-1")
    val sessionManager = new SessionManager(server)
    KyuubiSparkUtil.setupCommonConfig(conf)
    sessionManager.init(conf)
    sessionManager.start()
    assert(ReflectUtils.getFieldValue(sessionManager, "checkInterval") === -1)
    sessionManager.stop()
  }

  test("init session manager") {
    val conf = new SparkConf()
    val sessionManager = new SessionManager(server)
    intercept[NoSuchElementException](sessionManager.init(conf))
    KyuubiSparkUtil.setupCommonConfig(conf)
    assert(sessionManager.getServiceState === State.NOT_INITED)
    assert(sessionManager.getOperationMgr.getServiceState === State.NOT_INITED)
    sessionManager.init(conf)
    assert(sessionManager.getServiceState === State.INITED)
    assert(sessionManager.getOperationMgr.getServiceState === State.INITED)
    assert(sessionManager.getCacheMgr !== null)
    intercept[IllegalStateException](sessionManager.init(conf))
    sessionManager.stop()

    val sessionManager2 = new SessionManager(server)
    MetricsSystem.init(conf)
    sessionManager2.init(conf)
    MetricsSystem.close()
    sessionManager2.stop()
  }

  test("start session manager") {
    val conf = new SparkConf()
    val sessionManager = new SessionManager(server)
    KyuubiSparkUtil.setupCommonConfig(conf)
    sessionManager.init(conf)
    sessionManager.start()
    assert(sessionManager.getServiceState === State.STARTED)
    assert(sessionManager.getOperationMgr.getServiceState === State.STARTED)
    sessionManager.stop()
  }

  test("stop session manager") {
    val conf = new SparkConf()
    val sessionManager = new SessionManager(server)
    KyuubiSparkUtil.setupCommonConfig(conf)
    sessionManager.init(conf)
    sessionManager.start()
    sessionManager.stop()
    assert(sessionManager.getServiceState === State.STOPPED)
    assert(sessionManager.getOperationMgr.getServiceState === State.STOPPED)
    val r = new Runnable {
      override def run(): Unit = {}
    }
    intercept[NullPointerException](sessionManager.submitBackgroundOperation(r))
    val sessionManager2 = new SessionManager(server)
    sessionManager2.stop()
  }

  test("open get and close session") {
    val logRoot = UUID.randomUUID().toString

    val conf = new SparkConf()
      .setMaster("local")
      .set(KyuubiConf.LOGGING_OPERATION_ENABLED.key, "false")
      .set(KyuubiConf.LOGGING_OPERATION_LOG_DIR.key, logRoot)

    KyuubiSparkUtil.setupCommonConfig(conf)
    val sessionManager = new SessionManager(server)

    sessionManager.init(conf)
    assert(!new File(logRoot).exists(), "Operation Log should be off")
    sessionManager.start()
    val sessionHandle = sessionManager.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8,
      KyuubiSparkUtil.getCurrentUserName,
      "",
      "",
      Map.empty[String, String],
      withImpersonation = true)
    assert(!sessionManager.getSession(sessionHandle).isOperationLogEnabled)
    assert(sessionManager.getSession(sessionHandle).getSessionMgr === sessionManager)
    assert(sessionManager.getOpenSessionCount === 1)
    sessionManager.closeSession(sessionHandle)
    assert(sessionManager.getOpenSessionCount === 0)

    val e1 = intercept[KyuubiSQLException](sessionManager.closeSession(sessionHandle))
    assert(e1.getMessage.contains(sessionHandle.toString))
    val e2 = intercept[KyuubiSQLException](sessionManager.getSession(sessionHandle))
    assert(e2.getMessage.contains(sessionHandle.toString))
  }

  test("close session and stop server if it is deregistered") {
    val server = new KyuubiServer()
    server.init(conf)
    server.start()
    server.stop()
    val sessionHandle = server.beService.getSessionManager.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8,
      KyuubiSparkUtil.getCurrentUserName,
      "",
      "",
      Map.empty[String, String],
      withImpersonation = true)

    server.deregisterWithZK()
    server.beService.getSessionManager.closeSession(sessionHandle)
    assert(server.beService.getSessionManager.getOpenSessionCount === 0)
    assert(server.feService.getServiceState === State.STOPPED)
  }
}
