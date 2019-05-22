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

package yaooqinn.kyuubi.server

import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkFunSuite}

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.cli.GetInfoType
import yaooqinn.kyuubi.operation.{CANCELED, CLOSED, FINISHED, RUNNING}
import yaooqinn.kyuubi.session.{KyuubiSession, SessionHandle}

class BackendServiceSuite extends SparkFunSuite {

  private var backendService: BackendService = _
  private val user = KyuubiSparkUtil.getCurrentUserName
  private val conf = new SparkConf(loadDefaults = true).setAppName("be test")
  KyuubiSparkUtil.setupCommonConfig(conf)
  conf.remove(KyuubiSparkUtil.CATALOG_IMPL)
  conf.setMaster("local")
  private var sessionHandle: SessionHandle = _
  private val showTables = "show tables"
  private val ip = "localhost"
  private val proto = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8

  override protected def beforeAll(): Unit = {
    backendService = new BackendService()
    backendService.init(conf)
    backendService.start()
    sessionHandle = backendService.openSession(
      proto,
      user,
      "",
      ip,
      Map.empty)
  }

  protected override def afterAll(): Unit = {
    backendService.stop()
  }


  test("open session") {
    val sessionManager = backendService.getSessionManager
    assert(sessionManager.getOpenSessionCount === 1)
    val kyuubiSession = sessionManager.getSession(sessionHandle)
    assert(kyuubiSession.getSessionHandle === sessionHandle)
    assert(kyuubiSession.getUserName === user)
    assert(!kyuubiSession.sparkSession.sparkContext.isStopped)
    assert(kyuubiSession.ugi.getShortUserName === user)
    assert(kyuubiSession.getResourcesSessionDir.exists())
    assert(kyuubiSession.getIpAddress === ip)
    assert(kyuubiSession.getPassword.isEmpty)
    assert(kyuubiSession.isOperationLogEnabled)
    kyuubiSession.closeExpiredOperations
    assert(kyuubiSession.getProtocolVersion === proto)
  }

  test("get info") {
    assert(
      backendService.getInfo(
        sessionHandle, GetInfoType.SERVER_NAME).toTGetInfoValue.getStringValue === "Kyuubi Server")

    assert(
      backendService.getInfo(
        sessionHandle, GetInfoType.DBMS_NAME).toTGetInfoValue.getStringValue === "Spark SQL")

    assert(
      backendService.getInfo(
        sessionHandle,
        GetInfoType.DBMS_VERSION).toTGetInfoValue.getStringValue === KyuubiSparkUtil.SPARK_VERSION)
  }

  test("get type info") {
    val e1 = intercept[KyuubiSQLException](backendService.getTypeInfo(sessionHandle))
    assert(e1.toTStatus.getErrorMessage === "Method Not Implemented!")
  }

  test("get catalogs") {
    val e2 = intercept[KyuubiSQLException](backendService.getCatalogs(sessionHandle))
    assert(e2.toTStatus.getErrorMessage === "Method Not Implemented!")
    assert(KyuubiSQLException.toTStatus(e2).getErrorMessage === "Method Not Implemented!")
  }

  test("get schemas") {
    intercept[KyuubiSQLException](backendService.getSchemas(sessionHandle, "", ""))
  }

  test("get tables") {
    intercept[KyuubiSQLException](backendService.getTables(sessionHandle, "", "", "", null))
  }

  test("get table types") {
    intercept[KyuubiSQLException](backendService.getTableTypes(sessionHandle))
  }

  test("get functions") {
    intercept[KyuubiSQLException](backendService.getFunctions(sessionHandle, "", "", ""))
  }

  test("execute statement") {
    val operationHandle = backendService.executeStatement(sessionHandle, showTables)
    val operationMgr = backendService.getSessionManager.getOperationMgr
    val kyuubiOperation = operationMgr.getOperation(operationHandle)
    assert(kyuubiOperation.getHandle === operationHandle)
    assert(kyuubiOperation.getProtocolVersion === proto)
    assert(!kyuubiOperation.isTimedOut)
    assert(!kyuubiOperation.isClosedOrCanceled)
    var count = 0
    while (count < 100 && kyuubiOperation.getStatus.getState != FINISHED) {
      Thread.sleep(50 )
      count = count + 1
    }
    assert(kyuubiOperation.getStatus.getState === FINISHED)
    assert(backendService.getOperationStatus(operationHandle).getState === FINISHED)
    assert(backendService.getResultSetMetadata(operationHandle).head.name === "database")

  }

  test("execute statement async") {
    val operationHandle = backendService.executeStatementAsync(sessionHandle, showTables)
    val operationMgr = backendService.getSessionManager.getOperationMgr
    val kyuubiOperation = operationMgr.getOperation(operationHandle)
    assert(kyuubiOperation.getHandle === operationHandle)
    assert(kyuubiOperation.getProtocolVersion === proto)
    assert(!kyuubiOperation.isTimedOut)
    assert(!kyuubiOperation.isClosedOrCanceled)
    var count = 0
    while (count < 100 && kyuubiOperation.getStatus.getState != FINISHED) {
      Thread.sleep(50 )
      count = count + 1
    }
    assert(kyuubiOperation.getStatus.getState === FINISHED)
    assert(backendService.getOperationStatus(operationHandle).getState === FINISHED)
    assert(backendService.getResultSetMetadata(operationHandle).head.name === "database")
  }

  test("cancel operation") {
    val operationHandle = backendService.executeStatementAsync(sessionHandle, showTables)
    val operationMgr = backendService.getSessionManager.getOperationMgr
    backendService.cancelOperation(operationHandle)
    val operation = operationMgr.getOperation(operationHandle)
    assert(operation.isClosedOrCanceled || operation.getStatus.getState === FINISHED)
  }

  test("close operation") {
    val operationHandle = backendService.executeStatementAsync(sessionHandle, showTables)
    val operationMgr = backendService.getSessionManager.getOperationMgr
    val operation = operationMgr.getOperation(operationHandle)
    backendService.closeOperation(operationHandle)
    assert(operation.isClosedOrCanceled || operation.getStatus.getState === FINISHED)
  }
}
