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

import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.hive.service.cli.thrift.{TGetInfoType, TProtocolVersion}
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkFunSuite}
import org.apache.spark.sql.SparkSession
import org.scalatest.mock.MockitoSugar

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.auth.KyuubiAuthFactory
import yaooqinn.kyuubi.cli.{FetchOrientation, FetchType, GetInfoType}
import yaooqinn.kyuubi.operation.CANCELED
import yaooqinn.kyuubi.schema.ColumnBasedSet
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.ui.KyuubiServerMonitor
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiSessionSuite extends SparkFunSuite with MockitoSugar {

  import KyuubiConf._

  var server: KyuubiServer = _
  var session: KyuubiSession = _
  var spark: SparkSession = _
  val statement = "show tables"

  override def beforeAll(): Unit = {
    System.setProperty(KyuubiConf.FRONTEND_BIND_PORT.key, "0")
    System.setProperty("spark.master", "local")
    System.setProperty("spark.sql.catalogImplementation", "hive")

    server = KyuubiServer.startKyuubiServer()
    val be = server.beService
    val sessionMgr = be.getSessionManager
    val operationMgr = sessionMgr.getOperationMgr
    val user = KyuubiSparkUtil.getCurrentUserName
    val passwd = ""
    val ip = ""
    val imper = true
    val proto = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8
    session =
      new KyuubiSession(proto, user, passwd, server.getConf, ip, imper, sessionMgr, operationMgr)
    session.open(Map.empty)
    KyuubiServerMonitor.getListener(user)
      .foreach(_.onSessionCreated(
        session.getIpAddress, session.getSessionHandle.getSessionId.toString, user))
    spark = session.sparkSession
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    System.clearProperty(KyuubiConf.FRONTEND_BIND_PORT.key)
    System.clearProperty("spark.master")
    System.clearProperty("spark.sql.catalogImplementation")
    if (session != null) {
      if (session.sparkSession != null) session.sparkSession.stop()
      session.close()
    }
    if (server != null) server.stop()
    super.afterAll()
  }

  test("test session ugi") {
    assert(session.ugi.getAuthenticationMethod === AuthenticationMethod.SIMPLE)
  }

  test("spark session") {
    assert(!session.sparkSession.sparkContext.isStopped)
  }

  test("session handle") {
    val handle = session.getSessionHandle
    assert(handle.getProtocolVersion === TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)
  }

  test("get info") {
    assert(
      session.getInfo(GetInfoType.SERVER_NAME).toTGetInfoValue.getStringValue === "Kyuubi Server")
    assert(session.getInfo(GetInfoType.DBMS_NAME).toTGetInfoValue.getStringValue === "Spark SQL")
    assert(
      session.getInfo(GetInfoType.DBMS_VERSION).toTGetInfoValue.getStringValue === spark.version)
    val e = intercept[KyuubiSQLException](session.getInfo(new GetInfoType {}))
    assert(e.getMessage.startsWith("Unrecognized GetInfoType value"))
  }

  test("get last access time") {
    session.getInfo(GetInfoType.SERVER_NAME)
    assert(session.getLastAccessTime !== 0L)
  }

  test("get password") {
    assert(session.getPassword === "")
  }

  test("set operation log session dir") {
    val operationLogRootDir = new File(server.getConf.get(LOGGING_OPERATION_LOG_DIR))
    operationLogRootDir.mkdirs()
    session.setOperationLogSessionDir(operationLogRootDir)
    assert(session.isOperationLogEnabled)
    assert(operationLogRootDir.exists())
    assert(operationLogRootDir.listFiles().exists(_.getName == KyuubiSparkUtil.getCurrentUserName))
    assert(operationLogRootDir.listFiles().filter(_.getName == KyuubiSparkUtil.getCurrentUserName)
      .head.listFiles().exists(_.getName === session.getSessionHandle.getHandleIdentifier.toString))
    session.setOperationLogSessionDir(operationLogRootDir)
    assert(session.isOperationLogEnabled)
    operationLogRootDir.delete()
    operationLogRootDir.setExecutable(false)
    operationLogRootDir.setReadable(false)
    operationLogRootDir.setWritable(false)
    session.setOperationLogSessionDir(operationLogRootDir)
    assert(!session.isOperationLogEnabled)
    operationLogRootDir.setReadable(true)
    operationLogRootDir.setWritable(true)
    operationLogRootDir.setExecutable(true)
  }

  test("set resources session dir") {
    val resourceRoot = new File(server.getConf.get(OPERATION_DOWNLOADED_RESOURCES_DIR))
    resourceRoot.mkdirs()
    resourceRoot.deleteOnExit()
    assert(resourceRoot.isDirectory)
    session.setResourcesSessionDir(resourceRoot)
    val subDir = resourceRoot.listFiles().head
    assert(subDir.getName === KyuubiSparkUtil.getCurrentUserName)
    val resourceDir = subDir.listFiles().head
    assert(resourceDir.getName === session.getSessionHandle.getSessionId + "_resources")
    session.setResourcesSessionDir(resourceRoot)
    assert(subDir.listFiles().length === 1, "directory should already exists")
    assert(resourceDir.delete())
    resourceDir.createNewFile()
    assert(resourceDir.isFile)
    val e1 = intercept[RuntimeException](session.setResourcesSessionDir(resourceRoot))
    assert(e1.getMessage.startsWith("The resources directory exists but is not a directory"))
    resourceDir.delete()
    subDir.setWritable(false)
    val e2 = intercept[RuntimeException](session.setResourcesSessionDir(resourceRoot))
    assert(e2.getMessage.startsWith("Couldn't create session resources directory"))
    subDir.setWritable(true)
  }

  test("get no operation time") {
    assert(session.getNoOperationTime !== 0L)
  }

  test("get delegation token for non secured") {
    val authFactory =
      ReflectUtils.getFieldValue(server.feService, "authFactory").asInstanceOf[KyuubiAuthFactory]
    val e = intercept[KyuubiSQLException](
      session.getDelegationToken(authFactory, session.getUserName, session.getUserName))
    assert(e.getMessage === "Delegation token only supported over kerberos authentication")
    assert(e.toTStatus.getSqlState === "08S01")
  }

  test("cancel delegation token for non secured") {
    val authFactory =
      ReflectUtils.getFieldValue(server.feService, "authFactory").asInstanceOf[KyuubiAuthFactory]
    val e = intercept[KyuubiSQLException](
      session.cancelDelegationToken(authFactory, ""))
    assert(e.getMessage === "Delegation token only supported over kerberos authentication")
    assert(e.toTStatus.getSqlState === "08S01")
  }

  test("renew delegation token for non secured") {
    val authFactory =
      ReflectUtils.getFieldValue(server.feService, "authFactory").asInstanceOf[KyuubiAuthFactory]
    val e = intercept[KyuubiSQLException](
      session.renewDelegationToken(authFactory, ""))
    assert(e.getMessage === "Delegation token only supported over kerberos authentication")
    assert(e.toTStatus.getSqlState === "08S01")
  }

  test("test getProtocolVersion") {
    assert(session.getProtocolVersion === TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)
  }

  test("test close operation") {
    val opMgr = session.getSessionMgr.getOperationMgr
    val op = opMgr.newExecuteStatementOperation(session, statement)
    val opHandle = op.getHandle
    assert(opMgr.getOperation(opHandle) !== null)
    session.closeOperation(opHandle)
    val e = intercept[KyuubiSQLException](opMgr.getOperation(opHandle))
    assert(e.getMessage === "Invalid OperationHandle " + opHandle)
  }

  test("test cancel operation") {
    val opMgr = session.getSessionMgr.getOperationMgr
    val op = opMgr.newExecuteStatementOperation(session, statement)
    val opHandle = op.getHandle
    session.cancelOperation(opHandle)
    assert(op.getStatus.getState === CANCELED)
  }

  test("test get info") {
    assert(session.getInfo(GetInfoType.SERVER_NAME).toTGetInfoValue
      .getStringValue === "Kyuubi Server")
    assert(session.getInfo(GetInfoType.DBMS_NAME).toTGetInfoValue
      .getStringValue === "Spark SQL")
    assert(session.getInfo(GetInfoType.DBMS_VERSION).toTGetInfoValue
      .getStringValue === spark.version)

    case object UNSUPPORT_INFO extends GetInfoType {
      override val tInfoType: TGetInfoType = TGetInfoType.CLI_USER_NAME
    }
    intercept[KyuubiSQLException](session.getInfo(UNSUPPORT_INFO))
  }

  test("test getNoOperationTime") {
    val mockSession = mock[KyuubiSession]
    assert(mockSession.getNoOperationTime === 0L)
  }

  test("test executeStatement") {
    val sessionHadnle = server.beService.getSessionManager.openSession(
      session.getProtocolVersion,
      session.getUserName,
      "",
      session.getIpAddress,
      Map.empty,
      true)
    val kyuubiSession = server.beService.getSessionManager.getSession(sessionHadnle)
    kyuubiSession.getSessionMgr.getCacheMgr.set(session.getUserName, spark)

    var opHandle = kyuubiSession.executeStatement("wrong statement")
    Thread.sleep(5000)
    var opException = kyuubiSession.getSessionMgr.getOperationMgr.getOperation(opHandle)
      .getStatus.getOperationException
    assert(opException.getSQLState === "ParseException")

    opHandle = kyuubiSession.executeStatement("select * from tablea")
    Thread.sleep(5000)
    opException = kyuubiSession.getSessionMgr.getOperationMgr.getOperation(opHandle)
      .getStatus.getOperationException
    assert(opException.getSQLState === "AnalysisException")

    opHandle = kyuubiSession.executeStatement("show tables")
    Thread.sleep(5000)
    val results = kyuubiSession.fetchResults(opHandle, FetchOrientation.FETCH_FIRST,
      10, FetchType.QUERY_OUTPUT)
    val logs = kyuubiSession.fetchResults(opHandle, FetchOrientation.FETCH_FIRST,
      10, FetchType.LOG)
    assert(results.isInstanceOf[ColumnBasedSet] && logs.isInstanceOf[ColumnBasedSet])
  }
}
