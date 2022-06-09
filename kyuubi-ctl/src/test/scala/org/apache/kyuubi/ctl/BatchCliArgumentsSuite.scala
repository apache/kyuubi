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
package org.apache.kyuubi.ctl

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.ctl.ControlCliArgumentsTestUtil._
import org.apache.kyuubi.ctl.DateTimeUtil._

class BatchCliArgumentsSuite extends KyuubiFunSuite {

  test("create/submit batch") {
    Seq("create", "submit").foreach { op =>
      val args = Seq(
        op,
        "batch",
        "-f",
        "src/test/resources/cli/batch.yaml")
      val opArgs = new ControlCliArguments(args)
      assert(opArgs.cliArgs.createOpts.filename == "src/test/resources/cli/batch.yaml")
    }
  }

  test("create/submit batch and overwrite rest config") {
    Seq("create", "submit").foreach { op =>
      val args = Array(
        op,
        "batch",
        "-f",
        "src/test/resources/cli/batch.yaml",
        "--hostUrl",
        "https://localhost:8440",
        "--username",
        "test_user_1",
        "--authSchema",
        "spnego")
      val opArgs = new ControlCliArguments(args)
      assert(opArgs.cliArgs.commonOpts.hostUrl == "https://localhost:8440")
      assert(opArgs.cliArgs.commonOpts.authSchema == "spnego")
      assert(opArgs.cliArgs.commonOpts.username == "test_user_1")
    }
  }

  test("create/submit batch without filename specified") {
    Seq("create", "submit").foreach { op =>
      val args = Array(
        op,
        "batch")
      testPrematureExit(args, "Config file is not specified.")
    }
  }

  test("create/submit batch with non-existed file") {
    Seq("create", "submit").foreach { op =>
      val args = Array(
        op,
        "batch",
        "-f",
        "fake.yaml")
      testPrematureExit(args, "Config file does not exist")
    }
  }

  test("get batch without batch id specified") {
    val args = Array(
      "get",
      "batch")
    testPrematureExit(args, "Must specify batchId for get batch command")
  }

  test("get batch") {
    val args = Array(
      "get",
      "batch",
      "f7fd702c-e54e-11ec-8fea-0242ac120002")
    val opArgs = new ControlCliArguments(args)
    assert(opArgs.cliArgs.batchOpts.batchId == "f7fd702c-e54e-11ec-8fea-0242ac120002")
  }

  test("test list batch option") {
    val args = Array(
      "list",
      "batch",
      "--batchType",
      "spark",
      "--batchUser",
      "tom",
      "--batchState",
      "RUNNING",
      "--createTime",
      "20220607000000",
      "--from",
      "2",
      "--size",
      "5")
    val opArgs = new ControlCliArguments(args)
    assert(opArgs.cliArgs.batchOpts.batchType == "spark")
    assert(opArgs.cliArgs.batchOpts.batchUser == "tom")
    assert(opArgs.cliArgs.batchOpts.batchState == "RUNNING")
    assert(opArgs.cliArgs.batchOpts.createTime ==
      dateStringToMillis("20220607000000", "yyyyMMddHHmmss"))
    assert(opArgs.cliArgs.batchOpts.endTime == 0)
    assert(opArgs.cliArgs.batchOpts.from == 2)
    assert(opArgs.cliArgs.batchOpts.size == 5)
  }

  test("test list batch default option") {
    val args = Array(
      "list",
      "batch")
    val opArgs = new ControlCliArguments(args)
    assert(opArgs.cliArgs.batchOpts.batchType == null)
    assert(opArgs.cliArgs.batchOpts.from == -1)
    assert(opArgs.cliArgs.batchOpts.size == 10)
  }

  test("test bad list batch option - size") {
    val args = Array(
      "list",
      "batch",
      "--batchType",
      "spark",
      "--size",
      "-4")
    testPrematureExit(args, "Option --size must be >=0")
  }

  test("test bad list batch option - create date format") {
    val args = Array(
      "list",
      "batch",
      "--batchType",
      "spark",
      "--size",
      "4",
      "--createTime",
      "20220101")
    testPrematureExit(args, "Option --createTime must be in yyyyMMddHHmmss format.")
  }

  test("test bad list batch option - end date format") {
    val args = Array(
      "list",
      "batch",
      "--batchType",
      "spark",
      "--size",
      "4",
      "--endTime",
      "20220101")
    testPrematureExit(args, "Option --endTime must be in yyyyMMddHHmmss format.")
  }

  test("test bad list batch option - negative create date") {
    val args = Array(
      "list",
      "batch",
      "--batchType",
      "spark",
      "--size",
      "4",
      "--createTime",
      "19690101000000")
    testPrematureExit(args, "Invalid createTime, negative milliseconds are not supported.")
  }

  test("test bad list batch option - negative end date") {
    val args = Array(
      "list",
      "batch",
      "--batchType",
      "spark",
      "--size",
      "4",
      "--endTime",
      "19690101000000")
    testPrematureExit(args, "Invalid endTime, negative milliseconds are not supported.")
  }

  test("test bad list batch option - createTime > endTime") {
    val args = Array(
      "list",
      "batch",
      "--batchType",
      "spark",
      "--size",
      "4",
      "--createTime",
      "20220602000000",
      "--endTime",
      "20220601000000")
    testPrematureExit(
      args,
      "Invalid createTime/endTime, " +
        "createTime should be less or equal to endTime.")
  }

  test("test log batch") {
    val args = Array(
      "log",
      "batch",
      "f7fd702c-e54e-11ec-8fea-0242ac120002",
      "--from",
      "2",
      "--size",
      "5")
    val opArgs = new ControlCliArguments(args)
    assert(opArgs.cliArgs.batchOpts.batchId == "f7fd702c-e54e-11ec-8fea-0242ac120002")
    assert(opArgs.cliArgs.batchOpts.from == 2)
    assert(opArgs.cliArgs.batchOpts.size == 5)
  }

  test("test log batch without batchId") {
    val args = Array(
      "log",
      "batch",
      "--from",
      "2",
      "--size",
      "5")
    testPrematureExit(args, "Must specify batchId for log batch command")
  }

  test("test log batch default option") {
    val args = Array(
      "log",
      "batch",
      "f7fd702c-e54e-11ec-8fea-0242ac120002")
    val opArgs = new ControlCliArguments(args)
    assert(opArgs.cliArgs.batchOpts.batchId == "f7fd702c-e54e-11ec-8fea-0242ac120002")
    assert(opArgs.cliArgs.batchOpts.from == -1)
    assert(opArgs.cliArgs.batchOpts.size == 10)
  }

}
