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
import yaooqinn.kyuubi.cli.{FetchOrientation, FetchType, GetInfoType}
import yaooqinn.kyuubi.cli.FetchOrientation.FETCH_FIRST
import yaooqinn.kyuubi.operation.{CANCELED, CLOSED, RUNNING}

class BackendServiceSuite extends SparkFunSuite {

  test("verify default cli service protocol") {
    assert(BackendService.SERVER_VERSION === TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)
  }

  test("a") {
    val backendService = new BackendService()
    val conf = new SparkConf(loadDefaults = true).setAppName("spark session test")
    conf.remove(KyuubiSparkUtil.CATALOG_IMPL)
    conf.setMaster("local")
    KyuubiServer.setupCommonConfig(conf)
    assert(backendService.getName === classOf[BackendService].getSimpleName)
    // before init
    assert(backendService.getSessionManager === null)
    assert(backendService.getConf === null)

    // after init
    backendService.init(conf)
    assert(backendService.getSessionManager !== null)
    assert(backendService.getConf !== null)
    backendService.start()

    val user = KyuubiSparkUtil.getCurrentUserName()
    val session = backendService.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8,
      user,
      "",
      "localhost",
      Map.empty)
    assert(
      backendService.getInfo(
        session, GetInfoType.SERVER_NAME).toTGetInfoValue.getStringValue === "Kyuubi Server")

    assert(
      backendService.getInfo(
        session, GetInfoType.DBMS_NAME).toTGetInfoValue.getStringValue === "Spark SQL")

    assert(
      backendService.getInfo(
        session,
        GetInfoType.DBMS_VERSION).toTGetInfoValue.getStringValue === KyuubiSparkUtil.SPARK_VERSION)

    val op1 = backendService.executeStatement(session, "show tables")
    val op2 = backendService.executeStatementAsync(session, "show databases")
    val e1 = intercept[KyuubiSQLException](backendService.getTypeInfo(session))
    assert(e1.toTStatus.getErrorMessage === "Method Not Implemented!")
    val e2 = intercept[KyuubiSQLException](backendService.getCatalogs(session))
    assert(e2.toTStatus.getErrorMessage === "Method Not Implemented!")
    intercept[KyuubiSQLException](backendService.getSchemas(session, "", ""))
    intercept[KyuubiSQLException](backendService.getTables(session, "", "", "", null))
    intercept[KyuubiSQLException](backendService.getTableTypes(session))
    intercept[KyuubiSQLException](backendService.getFunctions(session, "", "", ""))

    assert(backendService.getOperationStatus(op1).getState === RUNNING)
    assert(backendService.getOperationStatus(op2).getState === RUNNING)
    assert(backendService.getResultSetMetadata(op1).head.name === "Result")
    backendService.cancelOperation(op1)
    assert(backendService.getOperationStatus(op1).getState === CANCELED)
    backendService.closeOperation(op2)
  }
}
