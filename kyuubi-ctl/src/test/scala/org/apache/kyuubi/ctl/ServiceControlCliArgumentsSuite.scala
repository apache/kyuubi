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
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_NAMESPACE

class ServiceControlCliArgumentsSuite extends KyuubiFunSuite {
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
          new ServiceControlCliArguments(args)
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
    Seq("get", "list", "delete").foreach { op =>
      Seq("server", "engine").foreach { service =>
        val args = Seq(
          op, service,
          "--zk-quorum", zkQuorum,
          "--namespace", namespace,
          "--user", user,
          "--host", host,
          "--port", port,
          "--version", KYUUBI_VERSION
        )
        val opArgs = new ServiceControlCliArguments(args)
        assert(opArgs.cliArgs.action.toString.equalsIgnoreCase(op))
        assert(opArgs.cliArgs.service.toString.equalsIgnoreCase(service))
        assert(opArgs.cliArgs.zkQuorum == zkQuorum)
        assert(opArgs.cliArgs.namespace == namespace)
        assert(opArgs.cliArgs.user == user)
        assert(opArgs.cliArgs.host == host)
        assert(opArgs.cliArgs.port == port)
        assert(opArgs.cliArgs.version == KYUUBI_VERSION)
      }
    }

    withSystemProperty(HA_ZK_NAMESPACE.key, namespace) {
      val op = "create"
      val service = "server"
      val newNamespace = s"${namespace}_new"
      val args = Seq(
        op, service,
        "--zk-quorum", zkQuorum,
        "--namespace", s"${namespace}_new",
        "--user", user,
        "--host", host,
        "--port", port,
        "--version", KYUUBI_VERSION
      )
      val opArgs = new ServiceControlCliArguments(args)
      assert(opArgs.cliArgs.action.toString.equalsIgnoreCase(op))
      assert(opArgs.cliArgs.service.toString.equalsIgnoreCase(service))
      assert(opArgs.cliArgs.zkQuorum == zkQuorum)
      assert(opArgs.cliArgs.namespace == newNamespace)
      assert(opArgs.cliArgs.user == user)
      assert(opArgs.cliArgs.host == host)
      assert(opArgs.cliArgs.port == port)
      assert(opArgs.cliArgs.version == KYUUBI_VERSION)
    }
  }

  test("treat --help as action") {
    val args = Seq("--help")
    val opArgs = new ServiceControlCliArguments(args)
    assert(opArgs.cliArgs.action == ServiceControlAction.HELP)
    assert(opArgs.cliArgs.version == KYUUBI_VERSION)

    val args2 = Seq(
      "create", "server",
      s"--user=$user",
      "--host", host,
      "--verbose",
      "--help",
      "--port", port
    )
    val opArgs2 = new ServiceControlCliArguments(args2)
    assert(opArgs2.cliArgs.action == ServiceControlAction.HELP)
    assert(opArgs2.cliArgs.user == user)
    assert(opArgs2.cliArgs.host == host)
    assert(opArgs2.cliArgs.verbose)
  }

  test("prints usage on empty input") {
    testPrematureExit(Array.empty[String], "Usage: kyuubi-ctl")
    testPrematureExit(Array("--verbose"), "Usage: kyuubi-ctl")
  }

  test("prints error with unrecognized options") {
    testPrematureExit(Array("create", "--unknown"), "Unknown option --unknown")
    testPrematureExit(Array("--unknown"), "Unknown option --unknown")
  }

  test("test invalid arguments") {
    testPrematureExit(Array("create", "--user"), "Missing value after --user")
  }

  test("test extra unused arguments") {
    val args = Array(
      "list",
      "extraArg1", "extraArg2"
    )
    testPrematureExit(args, "Unknown argument 'extraArg1'")
  }

  test("test list action arguments") {
    val args = Array(
      "list"
    )
    testPrematureExit(args, "Zookeeper quorum is not specified")

    val args2 = Array(
      "list",
      "--zk-quorum", zkQuorum,
      "--namespace", namespace
    )
    val opArgs = new ServiceControlCliArguments(args2)
    assert(opArgs.cliArgs.action == ServiceControlAction.LIST)
  }

  test("test get/delete action arguments") {
    Seq("get", "delete").foreach { op =>
      val args = Array(
        op
      )
      testPrematureExit(args, "Zookeeper quorum is not specified")

      val args2 = Array(
        op,
        "--zk-quorum", zkQuorum,
        "--namespace", namespace
      )
      testPrematureExit(args2, "Must specify host")

      val args3 = Array(
        op,
        "--zk-quorum", zkQuorum,
        "--namespace", namespace,
        "--host", host
      )
      testPrematureExit(args3, "Must specify port")

      val args4 = Array(
        op, "engine",
        "--zk-quorum", zkQuorum,
        "--namespace", namespace,
        "--host", host,
        "--port", port
      )
      testPrematureExit(args4, "Must specify user name for engine")

      val args5 = Array(
        op, "server",
        "--zk-quorum", zkQuorum,
        "--namespace", namespace,
        "--host", host,
        "--port", port
      )
      val opArgs6 = new ServiceControlCliArguments(args5)
      assert(opArgs6.cliArgs.action.toString.equalsIgnoreCase(op))
    }
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
    withSystemProperty(HA_ZK_NAMESPACE.key, namespace) {
      val newNamespace = s"${namespace}_new"
      val op = "create"
      val args = Array(
        op
      )
      testPrematureExit(args, "Zookeeper quorum is not specified")

      val args2 = Array(
        op, "server",
        "--zk-quorum", zkQuorum,
        "--namespace", newNamespace
      )
      val opArgs2 = new ServiceControlCliArguments(args2)
      assert(opArgs2.cliArgs.action.toString.equalsIgnoreCase(op))

      val args4 = Array(
        op, "engine",
        "--zk-quorum", zkQuorum,
        "--namespace", newNamespace
      )
      testPrematureExit(args4, "Only support expose Kyuubi server instance to another domain")
    }
  }

  test("test use default property value if missing") {
    val args = Array(
      "list",
      "--zk-quorum", zkQuorum
    )
    val opArgs = new ServiceControlCliArguments(args)
    assert(opArgs.cliArgs.namespace == namespace)
    assert(opArgs.cliArgs.version == KYUUBI_VERSION)
  }

  test("test use short options") {
    Seq("get", "list", "delete").foreach { op =>
      Seq("server", "engine").foreach { service =>
        val args = Seq(
          op, service,
          "-zk", zkQuorum,
          "-n", namespace,
          "-u", user,
          "-s", host,
          "-p", port,
          "-v", KYUUBI_VERSION
        )
        val opArgs = new ServiceControlCliArguments(args)
        assert(opArgs.cliArgs.action.toString.equalsIgnoreCase(op))
        assert(opArgs.cliArgs.service.toString.equalsIgnoreCase(service))
        assert(opArgs.cliArgs.zkQuorum == zkQuorum)
        assert(opArgs.cliArgs.namespace == namespace)
        assert(opArgs.cliArgs.user == user)
        assert(opArgs.cliArgs.host == host)
        assert(opArgs.cliArgs.port == port)
        assert(opArgs.cliArgs.version == KYUUBI_VERSION)
      }
    }

    // test help
    val args2 = Array("-h")
    val opArgs2 = new ServiceControlCliArguments(args2)
    assert(opArgs2.cliArgs.action == ServiceControlAction.HELP)

    // test verbose
    val args3 = Array(
      "list",
      "-zk", zkQuorum,
      "-b"
    )
    val opArgs3 = new ServiceControlCliArguments(args3)
    assert(opArgs3.cliArgs.verbose)
  }
}
