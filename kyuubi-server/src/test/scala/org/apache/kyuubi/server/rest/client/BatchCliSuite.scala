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

import java.io.File
import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.shaded.com.nimbusds.jose.util.StandardCharset
import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{BatchTestHelper, RestClientTestHelper, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ctl.{CtlConf, TestPrematureExit}
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.session.KyuubiSessionManager

class BatchCliSuite extends RestClientTestHelper with TestPrematureExit with BatchTestHelper {

  val basePath: String = Utils.getCodeSourceLocation(getClass)
  val batchFile: String = s"${basePath}/batch.yaml"

  override protected val otherConfigs: Map[String, String] = {
    Map(KyuubiConf.BATCH_APPLICATION_CHECK_INTERVAL.key -> "100")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    System.setProperty("kyuubi.ctl.rest.base.url", baseUri.toString)
    System.setProperty("kyuubi.ctl.rest.spnego.host", "localhost")

    val batch_basic = s"""apiVersion: v1
                         |username: ${ldapUser}
                         |request:
                         |  batchType: Spark
                         |  name: ${sparkBatchTestAppName}
                         |  resource: ${sparkBatchTestResource.get}
                         |  className: $sparkBatchTestMainClass
                         |  args:
                         |   - 1
                         |   - x1
                         |   - x2
                         |   - true
                         |  configs:
                         |    spark.master: local
                         |    wait.completion: true
                         |    k1: v1
                         |    1: test_integer_key
                         |    key:
                         |options:
                         |  verbose: true""".stripMargin
    Files.write(Paths.get(batchFile), batch_basic.getBytes(StandardCharsets.UTF_8))
  }

  override def afterEach(): Unit = {
    val sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]
    sessionManager.allSessions().foreach { session =>
      sessionManager.closeSession(session.handle)
    }
    sessionManager.getBatchesFromMetadataStore(null, null, null, 0, 0, 0, Int.MaxValue).foreach {
      batch =>
        sessionManager.applicationManager.killApplication(None, batch.getId)
        sessionManager.cleanupMetadata(batch.getId)
    }
  }

  test("basic batch rest client") {
    val totalConnections =
      MetricsSystem.counterValue(MetricsConstants.REST_CONN_TOTAL).getOrElse(0L)

    val createArgs = Array(
      "create",
      "batch",
      "-f",
      batchFile,
      "--password",
      ldapUserPasswd)
    var result = testPrematureExitForControlCli(createArgs, "")
    assert(result.contains("SPARK"))
    assert(result.contains(s"${fe.connectionUrl}"))
    val batchId = getBatchIdFromBatchReport(result)

    val getArgs = Array(
      "get",
      "batch",
      batchId,
      "--username",
      ldapUser,
      "--password",
      ldapUserPasswd)
    result = testPrematureExitForControlCli(getArgs, "SPARK")
    assert(result.contains("SPARK"))
    assert(result.contains(s"${fe.connectionUrl}"))

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
    assert(rows.length == 2)

    val deleteArgs = Array(
      "delete",
      "batch",
      batchId,
      "--username",
      ldapUser,
      "--password",
      ldapUserPasswd)
    result = testPrematureExitForControlCli(deleteArgs, "\"success\":true")

    eventually(timeout(3.seconds), interval(200.milliseconds)) {
      assert(MetricsSystem.counterValue(
        MetricsConstants.REST_CONN_TOTAL).getOrElse(0L) - totalConnections === 5)
      assert(MetricsSystem.counterValue(MetricsConstants.REST_CONN_OPEN).getOrElse(0L) === 0)
    }
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
    assert(result.contains("SPARK"))
    assert(result.contains(s"${fe.connectionUrl}"))
    val batchId = getBatchIdFromBatchReport(result)

    val getArgs = Array(
      "get",
      "batch",
      batchId,
      "--authSchema",
      "spnego")
    result = testPrematureExitForControlCli(getArgs, "SPARK")
    assert(result.contains("SPARK"))
    assert(result.contains(s"${fe.connectionUrl}"))

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
    assert(rows.length == 2)

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
    var result = testPrematureExitForControlCli(createArgs, "")
    assert(result.contains("SPARK"))
    assert(result.contains(s"${fe.connectionUrl}"))
    val batchId = getBatchIdFromBatchReport(result)

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
    result = testPrematureExitForControlCli(logArgs, "")
    assert(result.contains(s"Submitted application: ${sparkBatchTestAppName}"))
    assert(result.contains("ShutdownHookManager: Shutdown hook called"))
  }

  test("submit batch test") {
    val submitArgs = Array(
      "submit",
      "batch",
      "-f",
      batchFile,
      "--password",
      ldapUserPasswd)
    val result = testPrematureExitForControlCli(submitArgs, "")
    assert(result.contains(s"Submitted application: ${sparkBatchTestAppName}"))
    assert(result.contains("ShutdownHookManager: Shutdown hook called"))
  }

  test("submit batch test with waitCompletion=false") {
    val submitArgs = Array(
      "submit",
      "batch",
      "-f",
      batchFile,
      "--password",
      ldapUserPasswd,
      "--waitCompletion",
      "false",
      "--conf",
      s"${CtlConf.CTL_BATCH_LOG_QUERY_INTERVAL.key}=100")
    val result = testPrematureExitForControlCli(submitArgs, "")
    assert(result.contains(s"/bin/spark-submit"))
    assert(!result.contains("ShutdownHookManager: Shutdown hook called"))
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
      newBatchRequest(
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
      newBatchRequest(
        "spark",
        "",
        "",
        ""))
    sessionManager.openBatchSession(
      "kyuubi",
      "kyuubi",
      InetAddress.getLocalHost.getCanonicalHostName,
      Map.empty,
      newBatchRequest(
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
    testPrematureExitForControlCli(listArgs, "Batch List (from 0 total 3)")

    val listArgs1 = Array(
      "list",
      "batch",
      "--username",
      ldapUser,
      "--password",
      ldapUserPasswd,
      "--endTime",
      "20220101000000")
    testPrematureExitForControlCli(listArgs1, "Batch List (from 0 total 0)")
  }

  test("test batch yaml without request field") {
    val tempDir = Utils.createTempDir()
    val yamlFile1 = Files.write(
      new File(tempDir.toFile, "f1.yaml").toPath,
      s"""
         |apiVersion: v1
         |user: test_user
         |""".stripMargin
        .getBytes(StandardCharset.UTF_8))
    val args = Array(
      "create",
      "batch",
      "-f",
      yamlFile1.toFile.getAbsolutePath)
    testPrematureExitForControlCli(args, "No batch request field specified in yaml")
  }

  private def getBatchIdFromBatchReport(batchReport: String): String = {
    val batchIdRegex = s"""Batch Report \\((.*)\\)""".r
    batchIdRegex.findFirstMatchIn(batchReport) match {
      case Some(m) => m.group(1)
      case _ => throw new IllegalArgumentException("Invalid batch report")
    }
  }
}
