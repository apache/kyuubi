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

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiFunSuite}

class KyuubiCtlArgumentsSuite extends KyuubiFunSuite {
  val zkQuorum = "localhost:2181"
  val namespace = "kyuubi"
  val user = "kyuubi"
  val host = "localhost"
  val port = "10000"

  /** Check whether the script exits and the given search string is printed. */
  private def testPrematureExit(args: Array[String], searchString: String): Unit = {
    val logAppender = new LogAppender("test premature exit")
    withLogAppender(logAppender) {
      val thread = new Thread {
        override def run(): Unit = try {
          new KyuubiCtlArguments(args)
        } catch {
          case e: Exception =>
            error(e)
        }
      }
      thread.start()
      thread.join()
      assert(logAppender.loggingEvents.exists(_.getRenderedMessage.contains(searchString)))
    }
  }

  test("test basic kyuubi service arguments parser") {
    Seq("create", "get", "list", "delete").foreach { command =>
      Seq("server", "engine").foreach { service =>
        val args = Seq(
          command, service,
          "--zk-quorum", zkQuorum,
          "--namespace", namespace,
          "--user", user,
          "--host", host,
          "--port", port,
          "--version", KYUUBI_VERSION
        )
        if (!(command == "create" && service == "engine")) {
          val opArgs = new KyuubiCtlArguments(args)
          assert(opArgs.action.toString.equalsIgnoreCase(command))
          assert(opArgs.service.toString.equalsIgnoreCase(service))
          assert(opArgs.zkQuorum == zkQuorum)
          assert(opArgs.nameSpace == namespace)
          assert(opArgs.user == user)
          assert(opArgs.host == host)
          assert(opArgs.port == port)
          assert(opArgs.version == KYUUBI_VERSION)
        }
      }
    }
  }

  test("treat --help as action") {
    val args = Seq("--help")
    val opArgs = new KyuubiCtlArguments(args)
    assert(opArgs.action == KyuubiCtlAction.HELP)
    assert(opArgs.version == KYUUBI_VERSION)

    val args2 = Seq(
      "create", "server",
      s"--user=$user",
      "--host", host,
      "--verbose",
      "--help",
      "--port", port
    )
    val opArgs2 = new KyuubiCtlArguments(args2)
    assert(opArgs2.action == KyuubiCtlAction.HELP)
    assert(opArgs2.user == user)
    assert(opArgs2.host == host)
    assert(opArgs2.verbose)
  }

  test("prints usage on empty input") {
    testPrematureExit(Array.empty[String], "Usage: kyuubi-ctl")
    testPrematureExit(Array("--verbose"), "Usage: kyuubi-ctl")
  }

  test("prints error with unrecognized options") {
    testPrematureExit(Array("create", "--unkonwn"), "Unknown/unsupported param --unkonwn")
    testPrematureExit(Array("--unkonwn"), "Unknown/unsupported param --unkonwn")
  }

  test("test invalid arguments") {
    testPrematureExit(Array("create", "--user"), "Missing argument for option '--user'")
  }

  test("test extra unused arguments") {
    val args = Array(
      "list",
      "extraArg1", "extraArg2"
    )
    testPrematureExit(args, "Unknown/unsupported param extraArg1")
  }

  test("test list action arguments") {
    val args = Array(
      "list"
    )
    testPrematureExit(args, "Zookeeper address is not specified")

    val args2 = Array(
      "list",
      "--zk-quorum", zkQuorum
    )
    testPrematureExit(args2, "Zookeeper namespace is not specified")

    val args3 = Array(
      "list",
      "--zk-quorum", zkQuorum,
      "--namespace", namespace
    )
    val opArgs = new KyuubiCtlArguments(args3)
    assert(opArgs.action == KyuubiCtlAction.LIST)
  }

  test("test get/delete action arguments") {
    Seq("get", "delete").foreach { op =>
      val args = Array(
        op
      )
      testPrematureExit(args, "Zookeeper address is not specified")

      val args2 = Array(
        op,
        "--zk-quorum", zkQuorum
      )
      testPrematureExit(args2, "Zookeeper namespace is not specified")

      val args3 = Array(
        op,
        "--zk-quorum", zkQuorum,
        "--namespace", namespace
      )
      testPrematureExit(args3, "Must specify host")

      val args4 = Array(
        op,
        "--zk-quorum", zkQuorum,
        "--namespace", namespace,
        "--host", host
      )
      testPrematureExit(args4, "Must specify port")

      val args5 = Array(
        op, "engine",
        "--zk-quorum", zkQuorum,
        "--namespace", namespace,
        "--host", host,
        "--port", port
      )
      testPrematureExit(args5, "Must specify user name for engine")

      val args6 = Array(
        op, "server",
        "--zk-quorum", zkQuorum,
        "--namespace", namespace,
        "--host", host,
        "--port", port
      )
      val opArgs6 = new KyuubiCtlArguments(args6)
      assert(opArgs6.action.toString.equalsIgnoreCase(op))
    }
  }

  test("test with switches at head") {
    val args = Seq("--verbose", "list", "engine", "--zk-quorum", zkQuorum, "--namespace",
      namespace)
    val opArgs = new KyuubiCtlArguments(args)
    assert(opArgs.verbose)
    assert(opArgs.action == KyuubiCtlAction.LIST)
    assert(opArgs.service == KyuubiCtlActionService.ENGINE)

    val args2 = Seq("list", "--verbose", "engine", "--zk-quorum", zkQuorum, "--namespace",
      namespace)
    val opArgs2 = new KyuubiCtlArguments(args2)
    assert(opArgs2.verbose)
    assert(opArgs2.action == KyuubiCtlAction.LIST)
    assert(opArgs2.service == KyuubiCtlActionService.ENGINE)

    val args3 = Seq("list", "--verbose", "--help", "engine", "--zk-quorum", zkQuorum,
      "--namespace", namespace)
    val opArgs3 = new KyuubiCtlArguments(args3)
    assert(opArgs3.verbose)
    assert(opArgs3.action == KyuubiCtlAction.HELP)
  }

  test("test with unknown host") {
    val args = Array(
      "get", "server",
      "--zk-quorum", zkQuorum,
      "--namespace", namespace,
      "--host", "unknown-host",
      "--port", port
    )
    testPrematureExit(args, "Unknown host")
  }

  test("test with invalid port specification") {
    val args = Array(
      "get", "server",
      "--zk-quorum", zkQuorum,
      "--namespace", namespace,
      "--host", host,
      "--port", "invalid-format"
    )
    testPrematureExit(args, "Specified port is not a valid integer number")

    val args2 = Array(
      "get", "server",
      "--zk-quorum", zkQuorum,
      "--namespace", namespace,
      "--host", host,
      "--port", "0"
    )
    testPrematureExit(args2, "Specified port should be a positive number")
  }

  test("test create action arguments") {
    val op = "create"
    val args = Array(
      op
    )
    testPrematureExit(args, "Zookeeper address is not specified")

    val args2 = Array(
      op,
      "--zk-quorum", zkQuorum
    )
    testPrematureExit(args2, "Zookeeper namespace is not specified")


    val args3 = Array(
      op, "server",
      "--zk-quorum", zkQuorum,
      "--namespace", namespace
    )
    val opArgs3 = new KyuubiCtlArguments(args3)
    assert(opArgs3.action.toString.equalsIgnoreCase(op))

    val args4 = Array(
      op, "engine",
      "--zk-quorum", zkQuorum,
      "--namespace", namespace
    )
    testPrematureExit(args4, "Only support expose Kyuubi server instance to another domain")
  }
}
