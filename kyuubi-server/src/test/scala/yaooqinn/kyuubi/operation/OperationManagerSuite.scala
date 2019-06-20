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

package yaooqinn.kyuubi.operation

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkConf, SparkFunSuite}
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito._
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.cli.FetchOrientation
import yaooqinn.kyuubi.service.State
import yaooqinn.kyuubi.session.{KyuubiClientSession, SessionManager}

class OperationManagerSuite extends SparkFunSuite with Matchers with MockitoSugar {

  private val conf = new SparkConf(loadDefaults = true).setMaster("local").setAppName("test")
  KyuubiSparkUtil.setupCommonConfig(conf)

  test("operation manager as a abstract service") {
    val operationMgr = new OperationManager()
    operationMgr.getServiceState should be(State.NOT_INITED)
    operationMgr.getName should be(classOf[OperationManager].getSimpleName)
    operationMgr.getConf should be(null)
    operationMgr.getStartTime should be(0)
    intercept[IllegalStateException](operationMgr.start())

    val msg = "Initializing OperationManager"
    operationMgr.info(msg)
    operationMgr.debug(msg)
    operationMgr.warn(msg)
    operationMgr.error(msg)

    operationMgr.init(conf)

    operationMgr.getServiceState should be(State.INITED)
    operationMgr.getConf should be(conf)
    operationMgr.getStartTime should be(0)

    operationMgr.start()
    operationMgr.getServiceState should be(State.STARTED)
    operationMgr.getStartTime should not be 0

    operationMgr.stop()
    operationMgr.getServiceState should be(State.STOPPED)
    operationMgr.getStartTime should not be 0
  }

  test("set, get, and unregister operation log") {
    val operationMgr = new OperationManager()
    operationMgr.init(conf)

    operationMgr.getOperationLog should be(null)
    val log = mock[OperationLog]
    operationMgr.setOperationLog(KyuubiSparkUtil.getCurrentUserName, log)

    operationMgr.getOperationLog should be(log)

    OperationLog.removeCurrentOperationLog()
    operationMgr.getOperationLog should be(log)

    operationMgr.unregisterOperationLog(KyuubiSparkUtil.getCurrentUserName)
    operationMgr.getOperationLog should be(null)

  }

  test("handle operation") {
    val operationMgr = new OperationManager()
    conf.remove(KyuubiSparkUtil.CATALOG_IMPL)
    operationMgr.init(conf)

    val session = mock[KyuubiClientSession]
    val ss =
      SparkSession.builder()
        .config(KyuubiSparkUtil.SPARK_UI_PORT, KyuubiSparkUtil.SPARK_UI_PORT_DEFAULT)
        .config(conf)
        .getOrCreate()
    when(session.sparkSession).thenReturn(ss)
    val statement = "show tables"
    val op = operationMgr.newExecuteStatementOperation(session, statement)

    val op2 = operationMgr.getOperation(op.getHandle)
    assert(op === op2)
    val operationHandle = mock[OperationHandle]
    val e = intercept[KyuubiSQLException](operationMgr.getOperation(operationHandle))
    e.getMessage should startWith("Invalid OperationHandle")

    val e2 = intercept[KyuubiSQLException](operationMgr.closeOperation(operationHandle))
    e2.getMessage should be("Operation does not exist!")
    operationMgr.closeOperation(op.getHandle)
    assert(op.getStatus.getState === CLOSED)

    val op3 = operationMgr.newExecuteStatementOperation(session, statement)
    operationMgr.cancelOperation(op3.getHandle)
    val op4 = operationMgr.newExecuteStatementOperation(session, statement)
    op4.close()
    operationMgr.cancelOperation(op4.getHandle)

    val op5 = operationMgr.newExecuteStatementOperation(session, statement)
    op5.cancel()
    operationMgr.cancelOperation(op5.getHandle)
    ss.stop()
  }

  test("rm expired operations") {
    val operationMgr = new OperationManager()
    conf.remove(KyuubiSparkUtil.CATALOG_IMPL)
    conf.set(KyuubiConf.OPERATION_IDLE_TIMEOUT.key, "1s")
    operationMgr.init(conf)

    val session = mock[KyuubiClientSession]

    val ss =
      SparkSession.builder()
        .config(KyuubiSparkUtil.SPARK_UI_PORT, KyuubiSparkUtil.SPARK_UI_PORT_DEFAULT)
        .config(conf)
        .getOrCreate()
    when(session.sparkSession).thenReturn(ss)
    val statement = "show tables"

    val op1 = operationMgr.newExecuteStatementOperation(session, statement)
    val handles = new ArrayBuffer[OperationHandle]
    operationMgr.removeExpiredOperations(handles) should be(Nil) // empty handles
    handles += op1.getHandle
    operationMgr.removeExpiredOperations(handles) should be(Nil) // no expired op
    val op2 = operationMgr.newExecuteStatementOperation(session, statement)
    handles += op2.getHandle
    operationMgr.cancelOperation(op2.getHandle) // isTerminal=true
    Thread.sleep(1500) // timeout
    operationMgr.removeExpiredOperations(handles) should be(Seq(op2)) // op2 is timeout and terminal
    ss.stop()
  }

  test("get operation next row set") {
    val operationMgr = new OperationManager()
    conf.remove(KyuubiSparkUtil.CATALOG_IMPL)
    operationMgr.init(conf)

    val session = mock[KyuubiClientSession]
    val ss =
      SparkSession.builder()
        .config(KyuubiSparkUtil.SPARK_UI_PORT, KyuubiSparkUtil.SPARK_UI_PORT_DEFAULT)
        .config(conf)
        .getOrCreate()
    when(session.sparkSession).thenReturn(ss)
    val statement = "show tables"
    val op1 = operationMgr.newExecuteStatementOperation(session, statement)

    val sessionMgr = mock[SessionManager]
    when(session.getSessionMgr).thenReturn(sessionMgr)
    intercept[KyuubiSQLException](
      operationMgr.getOperationNextRowSet(op1.getHandle, FetchOrientation.FETCH_NEXT, 5))
    op1.close()
    intercept[KyuubiSQLException](
      operationMgr.getOperationNextRowSet(op1.getHandle, FetchOrientation.FETCH_NEXT, 5))
    ss.stop()
  }

  test("get operation log row set") {
    val operationMgr = new OperationManager()
    conf.remove(KyuubiSparkUtil.CATALOG_IMPL)
    operationMgr.init(conf)

    val session = mock[KyuubiClientSession]
    val ss =
      SparkSession.builder()
        .config(KyuubiSparkUtil.SPARK_UI_PORT, KyuubiSparkUtil.SPARK_UI_PORT_DEFAULT)
        .config(conf)
        .getOrCreate()
    when(session.sparkSession).thenReturn(ss)
    val statement = "show tables"
    val op1 = operationMgr.newExecuteStatementOperation(session, statement)

    val e = intercept[KyuubiSQLException](
      operationMgr.getOperationLogRowSet(op1.getHandle, FetchOrientation.FETCH_NEXT, 5))
    e.getMessage should startWith("Couldn't find log associated with operation handle:")
    ss.stop()
  }

}
