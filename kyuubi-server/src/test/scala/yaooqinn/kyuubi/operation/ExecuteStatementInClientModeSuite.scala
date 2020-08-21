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

package yaooqinn.kyuubi.operation

import java.io.File

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.cli.thrift.{TFetchOrientation, TProtocolVersion}
import org.apache.kyuubi.KyuubiSQLException
import org.apache.spark._
import org.apache.spark.KyuubiConf.{FRONTEND_BIND_PORT, LOGGING_OPERATION_LOG_DIR}
import org.apache.spark.sql.catalyst.catalog.FunctionResource
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.command.CreateFunctionCommand
import org.apache.spark.sql.internal.SQLConf
import org.mockito.Mockito.when
import org.scalatest.mock.MockitoSugar
import yaooqinn.kyuubi.cli.FetchOrientation
import yaooqinn.kyuubi.cli.FetchOrientation.{FETCH_FIRST, FETCH_NEXT}
import yaooqinn.kyuubi.operation.statement.ExecuteStatementInClientMode
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.session.{KyuubiSession, SessionHandle, SessionManager}
import yaooqinn.kyuubi.utils.ReflectUtils

class ExecuteStatementInClientModeSuite extends SparkFunSuite with MockitoSugar {

  private var server: KyuubiServer = _
  private val conf = new SparkConf()
  KyuubiSparkUtil.setupCommonConfig(conf)
  conf.remove(KyuubiSparkUtil.CATALOG_IMPL)
  conf.setMaster("local").set(FRONTEND_BIND_PORT.key, "0")

  private val proto = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8
  private val user = UserGroupInformation.getCurrentUser
  private val userName = user.getShortUserName
  private val passwd = ""

  protected val statement = "show tables"
  protected var sessionMgr: SessionManager = _
  protected var session: KyuubiSession = _
  protected var sessHandle: SessionHandle = _

  override protected def beforeAll(): Unit = {
    server = new KyuubiServer()
    server.init(conf)
    server.start()
    sessionMgr = server.beService.getSessionManager
    sessHandle = sessionMgr.openSession(proto, userName, passwd, "", Map.empty, false)
    session = sessionMgr.getSession(sessHandle)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    sessionMgr.closeSession(sessHandle)
    Option(server).foreach(_.stop())
    super.afterAll()
  }

  test("get next row set") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, "show databases")
    op.run()
    while(!op.getStatus.getState.isTerminal()) {
      Thread.sleep(50)
    }
    val rowSet1 = op.getNextRowSet(FETCH_FIRST, 10)
    assert(rowSet1.toTRowSet.getColumns.get(0).getStringVal.getValues.get(0) === "default")
    val rowSet2 = op.getNextRowSet(FETCH_NEXT, 10)
    assert(rowSet2.toTRowSet.getColumns.get(0).getStringVal.getValues.get(0) === "default")
  }

  test("is resource downloadable") {
    intercept[IllegalArgumentException](ExecuteStatementInClientMode.isResourceDownloadable(null))
    intercept[IllegalArgumentException](ExecuteStatementInClientMode.isResourceDownloadable(""))
    assert(ExecuteStatementInClientMode.isResourceDownloadable("hdfs://a/b/c.jar"))
    assert(!ExecuteStatementInClientMode.isResourceDownloadable("file://a/b/c.jar"))
    assert(!ExecuteStatementInClientMode.isResourceDownloadable("dfs://a/b/c.jar"))
  }

  test("transform plan") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
      .asInstanceOf[ExecuteStatementInClientMode]

    val parser = new SparkSqlParser(new SQLConf)
    val plan0 = parser.parsePlan("create temporary function a as 'a.b.c'")
    assert(op.transform(plan0) === plan0)

    val plan1 = parser.parsePlan(
      "create temporary function a as 'a.b.c' using file 'hdfs://a/b/c.jar'")
    val e1 = intercept[KyuubiSQLException](op.transform(plan1))
    assert(e1.getMessage.startsWith("Failed to read external resource"))

    val plan2 = parser.parsePlan(
      "create temporary function a as 'a.b.c' using jar 'hdfs://a/b/c.jar'")
    val e2 = intercept[KyuubiSQLException](op.transform(plan2))
    assert(e2.getMessage.startsWith("Failed to read external resource"))

    val resources = mock[Seq[FunctionResource]]
    when(resources.isEmpty).thenReturn(false)

    val command = plan2.asInstanceOf[CreateFunctionCommand].copy(resources = resources)
    val plan4 = op.transform(command)
    assert(plan4 === command)
    assert(plan4.asInstanceOf[CreateFunctionCommand].resources !== resources)

    val plan5 = parser.parsePlan(
      "create temporary function a as 'a.b.c' using archive 'hdfs://a/b/c.jar'")

    val e3 = intercept[KyuubiSQLException](op.transform(plan5))
    assert(e3.getMessage.startsWith("Resource Type"))
  }

  test("test get operation log") {
    val operationLogRootDir = new File(conf.get(LOGGING_OPERATION_LOG_DIR.key))
    operationLogRootDir.mkdirs()
    session.setOperationLogSessionDir(operationLogRootDir)
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(op.getOperationLog === null)
    op.run()
    val operationLog = op.getOperationLog
    assert(operationLog !== null)
    val rowSet = sessionMgr.getOperationMgr.getOperationLogRowSet(op.getHandle, FETCH_NEXT, 50)
    assert(rowSet !== null)
    op.close()
    operationLogRootDir.delete()
  }

  test("test execute statement operation is timeout or not") {
    val confClone = conf.clone()
    confClone.set(KyuubiConf.OPERATION_IDLE_TIMEOUT.key, "-1")
    val ks = mock[KyuubiSession]
    when(ks.sparkSession).thenReturn(this.session.sparkSession)
    when(ks.getConf).thenReturn(confClone)
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(ks, statement)
    assert(!op.isTimedOut, "configure no timeout")
    confClone.set(KyuubiConf.OPERATION_IDLE_TIMEOUT.key, "1ms")
    when(ks.getConf).thenReturn(confClone)
    val op2 = sessionMgr.getOperationMgr.newExecuteStatementOperation(ks, statement)
    assert(!op2.isTimedOut, s"operation state isTerminal is ${op.getStatus.getState.isTerminal()}")

    op2.close()
    Thread.sleep(5)
    assert(op2.isTimedOut,
      s"operation state isTerminal is ${op.getStatus.getState.isTerminal()} and timeout exceeded")
  }

  test("cancel operation") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(op.getStatus.getState === INITIALIZED)
    op.cancel()
    assert(op.getStatus.getState === CANCELED)
  }

  test("get operation handle") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(!op.getHandle.isHasResultSet)
    assert(!op.getHandle.toTOperationHandle.isHasResultSet)
    op.getHandle.setHasResultSet(true)
    assert(op.getHandle.isHasResultSet)
    assert(op.getHandle.toTOperationHandle.isHasResultSet)
    assert(op.getHandle.getProtocolVersion === TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)
    assert(op.getHandle.getOperationType === EXECUTE_STATEMENT)
  }

  test("get status") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(op.getStatus.getState === INITIALIZED)
    assert(op.getStatus.getOperationException === null)
  }

  test("testGetProtocolVersion") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(op.getProtocolVersion === proto)
  }

  test("testClose") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    assert(op.getStatus.getState === INITIALIZED)
    op.close()
    assert(op.getStatus.getState === CLOSED)
  }

  test("testGetSession") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    val s = op.getSession
    assert(s == session)
    assert(s.getUserName === userName)
  }

  test("is closed or canceled") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    op.cancel()
    assert(op.getStatus.getState === CANCELED)
    op.close()
    assert(op.getStatus.getState === CLOSED)
    val op2 = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    op2.close()
    assert(op2.getStatus.getState === CLOSED)
    val op3 = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, null)
    op3.cancel()
    op3.close()
    assert(op3.getStatus.getState === CLOSED)
  }

  test("test set, check and assert state") {
    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    ReflectUtils.invokeMethod(op, "setState", List(classOf[OperationState]), List(RUNNING))
    assert(ReflectUtils.invokeMethod(
      op, "checkState", List(classOf[OperationState]), List(RUNNING)) === true)
    assert(ReflectUtils.invokeMethod(
      op, "checkState", List(classOf[OperationState]), List(FINISHED)) === false)
    ReflectUtils.invokeMethod(op, "assertState", List(classOf[OperationState]), List(RUNNING))
  }

  test("test validateDefaultFetchOrientation") {
    case object FETCH_RELATIVE extends FetchOrientation {
      override val toTFetchOrientation: TFetchOrientation = TFetchOrientation.FETCH_RELATIVE
    }

    val op = sessionMgr.getOperationMgr.newExecuteStatementOperation(session, statement)
    val e = intercept[KyuubiSQLException](ReflectUtils.invokeMethod(op,
      "validateDefaultFetchOrientation", List(classOf[FetchOrientation]), List(FETCH_RELATIVE)))
    assert(e.getMessage === "The fetch type " + FETCH_RELATIVE.toString +
      " is not supported for this resultset")
  }
}
