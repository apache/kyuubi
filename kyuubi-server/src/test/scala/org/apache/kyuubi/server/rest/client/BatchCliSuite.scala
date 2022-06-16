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

package org.apache.kyuubi.server.rest.client

import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.{RestClientTestHelper, Utils}
import org.apache.kyuubi.client.api.v1.dto.BatchRequest
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_CHECK_INTERVAL, ENGINE_SPARK_MAX_LIFETIME}
import org.apache.kyuubi.ctl.TestPrematureExit
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.session.KyuubiSessionManager

class BatchCliSuite extends RestClientTestHelper with TestPrematureExit {

  val basePath: String = Utils.getCodeSourceLocation(getClass)
  val batchFile: String = s"${basePath}/batch.yaml"
  val appName: String = "test_batch"

  override def beforeAll(): Unit = {
    super.beforeAll()

    System.setProperty("kyuubi.ctl.rest.base.url", baseUri.toString)
    System.setProperty("kyuubi.ctl.rest.spnego.host", "localhost")

    val sparkProcessBuilder = new SparkProcessBuilder("kyuubi", conf)
    val batch_basic = s"""apiVersion: v1
                         |batchType: Spark
                         |username: ${ldapUser}
                         |request:
                         |  name: ${appName}
                         |  resource: ${sparkProcessBuilder.mainResource.get}
                         |  className: ${sparkProcessBuilder.mainClass}
                         |  args:
                         |   - x1
                         |   - x2
                         |  configs:
                         |    spark.master: local
                         |    spark.${ENGINE_SPARK_MAX_LIFETIME.key}: "5000"
                         |    spark.${ENGINE_CHECK_INTERVAL.key}: "1000"
                         |    k1: v1
                         |options:
                         |  verbose: true""".stripMargin
    Files.write(Paths.get(batchFile), batch_basic.getBytes(StandardCharsets.UTF_8))
  }

  override def afterEach(): Unit = {
    val sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]
    sessionManager.allSessions().foreach { session =>
      sessionManager.closeSession(session.handle)
    }
    sessionManager.getBatchesFromStateStore(null, null, null, 0, 0, 0, Int.MaxValue).foreach {
      batch =>
        sessionManager.applicationManager.killApplication(None, batch.getId)
        sessionManager.cleanupMetadata(batch.getId)
    }
  }

  test("basic batch rest client") {
    val createArgs = Array(
      "create",
      "batch",
      "-f",
      batchFile,
      "--password",
      ldapUserPasswd)
    var result = testPrematureExitForControlCli(createArgs, "")
    assert(result.contains("Type: SPARK"))
    assert(result.contains(s"Kyuubi Instance: ${fe.connectionUrl}"))
    val startIndex = result.indexOf("Id: ") + 4
    val endIndex = result.indexOf("\n", startIndex)
    val batchId = result.substring(startIndex, endIndex)

    val getArgs = Array(
      "get",
      "batch",
      batchId,
      "--username",
      ldapUser,
      "--password",
      ldapUserPasswd)
    result = testPrematureExitForControlCli(getArgs, "Type: SPARK")
    assert(result.contains("Type: SPARK"))
    assert(result.contains(s"Kyuubi Instance: ${fe.connectionUrl}"))

    val logArgs = Array(
      "log",
      "batch",
      batchId,
      "--size",
      "2",
      "--username",
      ldapUser,
      "--password",
      ldapUserPasswd)
    result = testPrematureExitForControlCli(logArgs, "")
    val rows = result.split("\n")
    assert(rows.length > 0)

    val deleteArgs = Array(
      "delete",
      "batch",
      batchId,
      "--username",
      ldapUser,
      "--password",
      ldapUserPasswd)
    result = testPrematureExitForControlCli(deleteArgs, "\"success\":true")
  }

  test("spnego batch rest client") {
    UserGroupInformation.loginUserFromKeytab(testPrincipal, testKeytab)

    val createArgs = Array(
      "create",
      "batch",
      "-f",
      batchFile,
      "--authSchema",
      "SPNEGO")
    var result = testPrematureExitForControlCli(createArgs, "")
    assert(result.contains("Type: SPARK"))
    assert(result.contains(s"Kyuubi Instance: ${fe.connectionUrl}"))
    val startIndex = result.indexOf("Id: ") + 4
    val endIndex = result.indexOf("\n", startIndex)
    val batchId = result.substring(startIndex, endIndex)

    val getArgs = Array(
      "get",
      "batch",
      batchId,
      "--authSchema",
      "spnego")
    result = testPrematureExitForControlCli(getArgs, "Type: SPARK")
    assert(result.contains("Type: SPARK"))
    assert(result.contains(s"Kyuubi Instance: ${fe.connectionUrl}"))

    val logArgs = Array(
      "log",
      "batch",
      batchId,
      "--size",
      "2",
      "--authSchema",
      "spnego")
    result = testPrematureExitForControlCli(logArgs, "")
    val rows = result.split("\n")
    assert(rows.length > 0)

    val deleteArgs = Array(
      "delete",
      "batch",
      batchId,
      "--authSchema",
      "spnego")
    result = testPrematureExitForControlCli(deleteArgs, "\"success\":true")
  }

  test("log batch test") {
    val createArgs = Array(
      "create",
      "batch",
      "-f",
      batchFile,
      "--password",
      ldapUserPasswd)
    val result = testPrematureExitForControlCli(createArgs, "")
    assert(result.contains("Type: SPARK"))
    assert(result.contains(s"Kyuubi Instance: ${fe.connectionUrl}"))
    val startIndex = result.indexOf("Id: ") + 4
    val endIndex = result.indexOf("\n", startIndex)
    val batchId = result.substring(startIndex, endIndex)

    val logArgs = Array(
      "log",
      "batch",
      batchId,
      "--size",
      "100",
      "--username",
      ldapUser,
      "--password",
      ldapUserPasswd,
      "--forward")
    testPrematureExitForControlCli(logArgs, s"Submitted application: ${appName}")
  }

  test("submit batch test") {
    val submitArgs = Array(
      "submit",
      "batch",
      "-f",
      batchFile,
      "--password",
      ldapUserPasswd)
    testPrematureExitForControlCli(submitArgs, s"Submitted application: ${appName}")
  }

  test("list batch test") {
    val sessionManager = server.frontendServices.head
      .be.sessionManager.asInstanceOf[KyuubiSessionManager]
    sessionManager.allSessions().foreach(_.close())

    sessionManager.openBatchSession(
      "kyuubi",
      "kyuubi",
      InetAddress.getLocalHost.getCanonicalHostName,
      Map.empty,
      new BatchRequest(
        "spark",
        "",
        "",
        ""))
    sessionManager.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11,
      "",
      "",
      "",
      Map.empty)
    sessionManager.openSession(
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11,
      "",
      "",
      "",
      Map.empty)
    sessionManager.openBatchSession(
      "kyuubi",
      "kyuubi",
      InetAddress.getLocalHost.getCanonicalHostName,
      Map.empty,
      new BatchRequest(
        "spark",
        "",
        "",
        ""))
    sessionManager.openBatchSession(
      "kyuubi",
      "kyuubi",
      InetAddress.getLocalHost.getCanonicalHostName,
      Map.empty,
      new BatchRequest(
        "spark",
        "",
        "",
        ""))

    val listArgs = Array(
      "list",
      "batch",
      "--username",
      ldapUser,
      "--password",
      ldapUserPasswd,
      "--batchType",
      "spark",
      "--batchUser",
      "kyuubi",
      "--createTime",
      "20220101000000")
    testPrematureExitForControlCli(listArgs, "Total number of batches: 3")

    val listArgs1 = Array(
      "list",
      "batch",
      "--username",
      ldapUser,
      "--password",
      ldapUserPasswd,
      "--endTime",
      "20220101000000")
    testPrematureExitForControlCli(listArgs1, "Total number of batches: 0")
  }

}
