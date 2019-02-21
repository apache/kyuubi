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
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.{KyuubiConf, KyuubiSparkUtil, SparkFunSuite}
import org.apache.spark.sql.SparkSession

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.auth.KyuubiAuthFactory
import yaooqinn.kyuubi.cli.GetInfoType
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.ui.KyuubiServerMonitor
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiSessionSuite extends SparkFunSuite {

  import KyuubiConf._

  var server: KyuubiServer = _
  var session: KyuubiSession = _
  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    System.setProperty(KyuubiConf.FRONTEND_BIND_PORT.key, "0")
    System.setProperty("spark.master", "local")

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
}
