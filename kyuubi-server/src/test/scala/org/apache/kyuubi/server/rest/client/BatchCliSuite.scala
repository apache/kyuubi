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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.{RestClientTestHelper, Utils}
import org.apache.kyuubi.client.api.v1.dto.Batch
import org.apache.kyuubi.client.util.JsonUtil
import org.apache.kyuubi.ctl.TestPrematureExit
import org.apache.kyuubi.engine.spark.SparkProcessBuilder

class BatchCliSuite extends RestClientTestHelper with TestPrematureExit {

  val basePath: String = Utils.getCodeSourceLocation(getClass)
  val batchBasicFile: String = s"${basePath}/batch_basic.yaml"
  val batchErrorFile: String = s"${basePath}/batch_error.yaml"
  val appName: String = "test_batch"

  override def beforeAll(): Unit = {
    super.beforeAll()
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
                         |    hive.server2.proxy.user: b_user
                         |    k1: v1
                         |options:
                         |  verbose: true""".stripMargin
    Files.write(Paths.get(batchBasicFile), batch_basic.getBytes(StandardCharsets.UTF_8))

    val batch_error = s"""apiVersion: v1
                         |batchType: Spark
                         |username: ${ldapUser}
                         |request:
                         |  name: ${appName}
                         |  resource: /Fake/kyuubi-spark-sql-engine_2.12-1.6.0-SNAPSHOT.jar
                         |  className: org.apache.kyuubi.engine.spark.SparkSQLEngine
                         |  args:
                         |   - x1
                         |   - x2
                         |  configs:
                         |    spark.master: local
                         |    hive.server2.proxy.user: b_user
                         |    k1: v1
                         |options:
                         |  verbose: true""".stripMargin
    Files.write(Paths.get(batchErrorFile), batch_error.getBytes(StandardCharsets.UTF_8))

    System.setProperty("kyuubi.ctl.rest.base.url", baseUri.toString)
    System.setProperty("kyuubi.ctl.rest.spnego.host", "localhost")
  }

  test("basic batch rest client") {
    val createArgs = Array(
      "create",
      "batch",
      "-f",
      batchBasicFile,
      "--password",
      ldapUserPasswd)
    var result = testPrematureExit(createArgs, "\"batchType\":\"SPARK\"")
    var batch = JsonUtil.toObject(result, classOf[Batch])
    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "SPARK")

    val getArgs = Array(
      "get",
      "batch",
      batch.getId,
      "--username",
      ldapUser,
      "--password",
      ldapUserPasswd)
    result = testPrematureExit(getArgs, "\"batchType\":\"SPARK\"")
    batch = JsonUtil.toObject(result, classOf[Batch])
    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "SPARK")

    val logArgs = Array(
      "log",
      "batch",
      batch.getId,
      "--size",
      "2",
      "--username",
      ldapUser,
      "--password",
      ldapUserPasswd)
    result = testPrematureExit(logArgs, "")
    val rows = result.split("\n")
    assert(rows.length === 2)

    val deleteArgs = Array(
      "delete",
      "batch",
      batch.getId,
      "--username",
      ldapUser,
      "--password",
      ldapUserPasswd)
    result = testPrematureExit(deleteArgs, "\"success\":true")
  }

  test("spnego batch rest client") {
    UserGroupInformation.loginUserFromKeytab(testPrincipal, testKeytab)

    val createArgs = Array(
      "create",
      "batch",
      "-f",
      batchBasicFile,
      "--authSchema",
      "spnego")
    var result = testPrematureExit(createArgs, "\"batchType\":\"SPARK\"")
    var batch = JsonUtil.toObject(result, classOf[Batch])
    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "SPARK")

    val getArgs = Array(
      "get",
      "batch",
      batch.getId,
      "--authSchema",
      "spnego")
    result = testPrematureExit(getArgs, "\"batchType\":\"SPARK\"")
    batch = JsonUtil.toObject(result, classOf[Batch])
    assert(batch.getKyuubiInstance === fe.connectionUrl)
    assert(batch.getBatchType === "SPARK")

    val logArgs = Array(
      "log",
      "batch",
      batch.getId,
      "--size",
      "2",
      "--authSchema",
      "spnego")
    result = testPrematureExit(logArgs, "")
    val rows = result.split("\n")
    assert(rows.length === 2)

    val deleteArgs = Array(
      "delete",
      "batch",
      batch.getId,
      "--authSchema",
      "spnego")
    result = testPrematureExit(deleteArgs, "\"success\":true")
  }

  test("log batch test") {
    val createArgs = Array(
      "create",
      "batch",
      "-f",
      batchErrorFile,
      "--password",
      ldapUserPasswd)
    val result = testPrematureExit(createArgs, "\"batchType\":\"SPARK\"")
    val batch = JsonUtil.toObject(result, classOf[Batch])

    val logArgs = Array(
      "log",
      "batch",
      batch.getId,
      "--size",
      "5",
      "--username",
      ldapUser,
      "--password",
      ldapUserPasswd,
      "--forward")
    testPrematureExit(logArgs, s"--conf spark.app.name=${appName}")
  }

  test("submit batch test") {
    val submitArgs = Array(
      "submit",
      "batch",
      "-f",
      batchErrorFile,
      "--password",
      ldapUserPasswd)
    testPrematureExit(submitArgs, s"--conf spark.app.name=${appName}")
  }

}
