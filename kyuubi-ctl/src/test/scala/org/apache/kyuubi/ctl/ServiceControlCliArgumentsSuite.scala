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

  /** Check whether the script exits and the given search string is printed. */
  private def testHelpExit(args: Array[String], searchString: String): Unit = {
    val logAppender = new LogAppender("test premature exit")
    withLogAppender(logAppender) {
      val thread = new Thread {
        override def run(): Unit = try {
          new ServiceControlCliArguments(args) {
            override private[kyuubi] lazy val effectSetup = new KyuubiOEffectSetup {
              // nothing to do, to handle out stream.
              override def terminate(exitState: Either[String, Unit]): Unit = ()
            }
          }
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
        "--host", host,
        "--port", port,
        "--version", KYUUBI_VERSION
      )
      val opArgs = new ServiceControlCliArguments(args)
      assert(opArgs.cliArgs.action.toString.equalsIgnoreCase(op))
      assert(opArgs.cliArgs.service.toString.equalsIgnoreCase(service))
      assert(opArgs.cliArgs.zkQuorum == zkQuorum)
      assert(opArgs.cliArgs.namespace == newNamespace)
      assert(opArgs.cliArgs.host == host)
      assert(opArgs.cliArgs.port == port)
      assert(opArgs.cliArgs.version == KYUUBI_VERSION)
    }
  }

  test("prints usage on empty input") {
    testPrematureExit(Array.empty[String], "Must specify action command: [create|get|delete|list].")
    testPrematureExit(Array("--verbose"), "Must specify action command: [create|get|delete|list].")
  }

  test("prints error with unrecognized options") {
    testPrematureExit(Array("create", "--unknown"), "Unknown option --unknown")
    testPrematureExit(Array("--unknown"), "Unknown option --unknown")
  }

  test("test invalid arguments") {
    // for server, user option is not support
    testPrematureExit(Array("create", "--user"), "Unknown option --user")
    // for engine, user option need a value
    testPrematureExit(Array("get", "engine", "--user"), "Missing value after --user")
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
      // engine is not support, expect scopt print Unknown argument.
      testPrematureExit(args4, "Unknown argument 'engine'")
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

    // test verbose
    val args2 = Array(
      "list",
      "-zk", zkQuorum,
      "-b"
    )
    val opArgs3 = new ServiceControlCliArguments(args2)
    assert(opArgs3.cliArgs.verbose)
  }

  test("test --help") {
    val helpString =
    s"""kyuubi $KYUUBI_VERSION
      |Usage: kyuubi-ctl [create|get|delete|list] [options]
      |
      |  -zk, --zk-quorum <value>
      |                           The connection string for the zookeeper ensemble, using zk quorum manually.
      |  -n, --namespace <value>  The namespace, using kyuubi-defaults/conf if absent.
      |  -s, --host <value>       Hostname or IP address of a service.
      |  -p, --port <value>       Listening port of a service.
      |  -v, --version <value>    Using the compiled KYUUBI_VERSION default, change it if the active service is running in another.
      |  -b, --verbose            Print additional debug output.
      |
      |Command: create [server]
      |
      |Command: create server
      |	Expose Kyuubi server instance to another domain.
      |
      |Command: get [server|engine] [options]
      |	Get the service/engine node info, host and port needed.
      |Command: get server
      |	Get Kyuubi server info of domain
      |Command: get engine
      |	Get Kyuubi engine info belong to a user.
      |  -u, --user <value>       The user name this engine belong to.
      |
      |Command: delete [server|engine] [options]
      |	Delete the specified service/engine node, host and port needed.
      |Command: delete server
      |	Delete the specified service node for a domain
      |Command: delete engine
      |	Delete the specified engine node for user.
      |  -u, --user <value>       The user name this engine belong to.
      |
      |Command: list [server|engine] [options]
      |	List all the service/engine nodes for a particular domain.
      |Command: list server
      |	List all the service nodes for a particular domain
      |Command: list engine
      |	List all the engine nodes for a user
      |  -u, --user <value>       The user name this engine belong to.
      |
      |  -h, --help               Show help message and exit.""".stripMargin

    testHelpExit(Array("--help"), helpString)
  }
}
