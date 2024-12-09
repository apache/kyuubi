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
import org.apache.kyuubi.ctl.RestClientFactory.withKyuubiRestClient
import org.apache.kyuubi.ctl.cli.ControlCliArguments
import org.apache.kyuubi.ctl.opt.{ControlAction, ControlObject}
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_NAMESPACE

class ControlCliArgumentsSuite extends KyuubiFunSuite with TestPrematureExit {
  val zkQuorum = "localhost:2181"
  val namespace = "kyuubi"
  val user = "kyuubi"
  val host = "localhost"
  val port = "10000"

  /** Check whether the script exits and the given search string is printed. */
  private def testHelpExit(args: Array[String], searchString: String): Unit = {
    val logAppender = new LogAppender("test premature exit")
    withLogAppender(logAppender) {
      val thread = new Thread {
        override def run(): Unit =
          try {
            new ControlCliArguments(args) {
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
      assert(logAppender.loggingEvents.exists(
        _.getMessage.getFormattedMessage.contains(searchString)))
    }
  }

  test("test basic kyuubi service arguments parser") {
    Seq("get", "list", "delete").foreach { op =>
      Seq("server", "engine").foreach { service =>
        val engineUser =
          if (service == "engine") {
            Seq("-u", user)
          } else {
            Seq.empty[String]
          }
        val args = Seq(
          op,
          service,
          "--zk-quorum",
          zkQuorum,
          "--namespace",
          namespace,
          "--host",
          host,
          "--port",
          port,
          "--version",
          KYUUBI_VERSION) ++ engineUser
        val opArgs = new ControlCliArguments(args)
        assert(opArgs.cliConfig.action.toString.equalsIgnoreCase(op))
        assert(opArgs.cliConfig.resource.toString.equalsIgnoreCase(service))
        assert(opArgs.cliConfig.zkOpts.zkQuorum == zkQuorum)
        assert(opArgs.cliConfig.zkOpts.namespace == namespace)
        assert(opArgs.cliConfig.zkOpts.host == host)
        assert(opArgs.cliConfig.zkOpts.port == port)
        assert(opArgs.cliConfig.zkOpts.version == KYUUBI_VERSION)
        if (service == "engine") {
          assert(opArgs.cliConfig.engineOpts.user == user)
        }
      }
    }

    withSystemProperty(HA_NAMESPACE.key, namespace) {
      val op = "create"
      val service = "server"
      val newNamespace = s"${namespace}_new"
      val args = Seq(
        op,
        service,
        "--zk-quorum",
        zkQuorum,
        "--namespace",
        s"${namespace}_new",
        "--host",
        host,
        "--port",
        port,
        "--version",
        KYUUBI_VERSION)
      val opArgs = new ControlCliArguments(args)
      assert(opArgs.cliConfig.action.toString.equalsIgnoreCase(op))
      assert(opArgs.cliConfig.resource.toString.equalsIgnoreCase(service))
      assert(opArgs.cliConfig.zkOpts.zkQuorum == zkQuorum)
      assert(opArgs.cliConfig.zkOpts.namespace == newNamespace)
      assert(opArgs.cliConfig.zkOpts.host == host)
      assert(opArgs.cliConfig.zkOpts.port == port)
      assert(opArgs.cliConfig.zkOpts.version == KYUUBI_VERSION)
    }
  }

  test("prints usage on empty input") {
    testPrematureExitForControlCliArgs(
      Array.empty[String],
      "Must specify action command: [create|get|delete|list|log|submit].")
    testPrematureExitForControlCliArgs(
      Array("--verbose"),
      "Must specify action command: [create|get|delete|list|log|submit].")
  }

  test("prints error with unrecognized options") {
    testPrematureExitForControlCliArgs(Array("create", "--unknown"), "Unknown option --unknown")
    testPrematureExitForControlCliArgs(Array("--unknown"), "Unknown option --unknown")
  }

  test("test invalid arguments") {
    // for server, user option is not support
    testPrematureExitForControlCliArgs(Array("create", "--user"), "Unknown option --user")
    // for engine, user option need a value
    testPrematureExitForControlCliArgs(
      Array("get", "engine", "--user"),
      "Missing value after --user")
  }

  test("test extra unused arguments") {
    val args = Array(
      "list",
      "extraArg1",
      "extraArg2")
    testPrematureExitForControlCliArgs(args, "Unknown argument 'extraArg1'")
  }

  test("test list action arguments") {
    val args = Array(
      "list")
    testPrematureExitForControlCliArgs(args, "Zookeeper quorum is not specified")

    val args2 = Array(
      "list",
      "--zk-quorum",
      zkQuorum,
      "--namespace",
      namespace)
    var opArgs = new ControlCliArguments(args2)
    assert(opArgs.cliConfig.action == ControlAction.LIST)

    val args3 = Array(
      "list",
      "session")
    opArgs = new ControlCliArguments(args3)
    assert(opArgs.cliConfig.resource === ControlObject.SESSION)
  }

  test("test get/delete action arguments") {
    Seq("get", "delete").foreach { op =>
      val args = Array(
        op)
      testPrematureExitForControlCliArgs(args, "Zookeeper quorum is not specified")

      val args2 = Array(
        op,
        "--zk-quorum",
        zkQuorum,
        "--namespace",
        namespace)
      testPrematureExitForControlCliArgs(args2, "Must specify host")

      val args3 = Array(
        op,
        "--zk-quorum",
        zkQuorum,
        "--namespace",
        namespace,
        "--host",
        host)
      testPrematureExitForControlCliArgs(args3, "Must specify port")

      val args4 = Array(
        op,
        "engine",
        "--zk-quorum",
        zkQuorum,
        "--namespace",
        namespace,
        "--host",
        host,
        "--port",
        port)
      testPrematureExitForControlCliArgs(args4, "Must specify user name for engine")

      val args5 = Array(
        op,
        "server",
        "--zk-quorum",
        zkQuorum,
        "--namespace",
        namespace,
        "--host",
        host,
        "--port",
        port)
      val opArgs6 = new ControlCliArguments(args5)
      assert(opArgs6.cliConfig.action.toString.equalsIgnoreCase(op))
    }
  }

  test("test with unknown host") {
    val args = Array(
      "get",
      "server",
      "--zk-quorum",
      zkQuorum,
      "--namespace",
      namespace,
      "--host",
      "unknown-host",
      "--port",
      port)
    testPrematureExitForControlCliArgs(args, "Unknown host")
  }

  test("test with invalid port specification") {
    val args = Array(
      "get",
      "server",
      "--zk-quorum",
      zkQuorum,
      "--namespace",
      namespace,
      "--host",
      host,
      "--port",
      "invalid-format")
    testPrematureExitForControlCliArgs(args, "Specified port is not a valid integer number")

    val args2 = Array(
      "get",
      "server",
      "--zk-quorum",
      zkQuorum,
      "--namespace",
      namespace,
      "--host",
      host,
      "--port",
      "0")
    testPrematureExitForControlCliArgs(args2, "Specified port should be a positive number")
  }

  test("test create action arguments") {
    withSystemProperty(HA_NAMESPACE.key, namespace) {
      val newNamespace = s"${namespace}_new"
      val op = "create"
      val args = Array(
        op)
      testPrematureExitForControlCliArgs(args, "Zookeeper quorum is not specified")

      val args2 = Array(
        op,
        "server",
        "--zk-quorum",
        zkQuorum,
        "--namespace",
        newNamespace)
      val opArgs2 = new ControlCliArguments(args2)
      assert(opArgs2.cliConfig.action.toString.equalsIgnoreCase(op))

      val args4 = Array(
        op,
        "engine",
        "--zk-quorum",
        zkQuorum,
        "--namespace",
        newNamespace)
      // engine is not support, expect scopt print Unknown argument.
      testPrematureExitForControlCliArgs(args4, "Unknown argument 'engine'")
    }
  }

  test("test use default property value if missing") {
    val args = Array(
      "list",
      "--zk-quorum",
      zkQuorum)
    val opArgs = new ControlCliArguments(args)
    assert(opArgs.cliConfig.zkOpts.namespace == namespace)
    assert(opArgs.cliConfig.zkOpts.version == KYUUBI_VERSION)
  }

  test("test use short options") {
    Seq("get", "list", "delete").foreach { op =>
      Seq("server", "engine").foreach { service =>
        val engineUser =
          if (service == "engine") {
            Seq("-u", user)
          } else {
            Seq.empty[String]
          }
        val args = Seq(
          op,
          service,
          "-zk",
          zkQuorum,
          "-n",
          namespace,
          "-s",
          host,
          "-p",
          port,
          "-v",
          KYUUBI_VERSION) ++ engineUser
        val opArgs = new ControlCliArguments(args)
        assert(opArgs.cliConfig.action.toString.equalsIgnoreCase(op))
        assert(opArgs.cliConfig.resource.toString.equalsIgnoreCase(service))
        assert(opArgs.cliConfig.zkOpts.zkQuorum == zkQuorum)
        assert(opArgs.cliConfig.zkOpts.namespace == namespace)
        assert(opArgs.cliConfig.zkOpts.host == host)
        assert(opArgs.cliConfig.zkOpts.port == port)
        assert(opArgs.cliConfig.zkOpts.version == KYUUBI_VERSION)
        if (service == "engine") {
          assert(opArgs.cliConfig.engineOpts.user == user)
        }
      }
    }

    // test verbose
    val args2 = Array(
      "list",
      "-zk",
      zkQuorum,
      "-b")
    val opArgs3 = new ControlCliArguments(args2)
    assert(opArgs3.cliConfig.commonOpts.verbose)
  }

  test("test --help") {
    // some string is too long for check style
    val zkHelpString = "The connection string for the zookeeper ensemble, using zk quorum manually."
    val versionHelpString = "Using the compiled KYUUBI_VERSION default," +
      " change it if the active service is running in another."
    val waitBatchCompletionHelpString = "Boolean property. If true(default), the client process " +
      "will stay alive until the batch is in any terminal state. If false, the client will exit " +
      "when the batch is no longer in PENDING state."
    val helpString =
      s"""kyuubi $KYUUBI_VERSION
         |Usage: kyuubi-ctl [create|get|delete|list|log|submit] [options]
         |
         |  -b, --verbose            Print additional debug output.
         |  --hostUrl <value>        Host url for rest api.
         |  --authSchema <value>     Auth schema for rest api, valid values are basic, spnego.
         |  --username <value>       Username for basic authentication.
         |  --password <value>       Password for basic authentication.
         |  --spnegoHost <value>     Spnego host for spnego authentication.
         |  --hs2ProxyUser <value>   The value of hive.server2.proxy.user config.
         |  --conf <value>           Kyuubi config property pair, formatted key=value.
         |  -zk, --zk-quorum <value>
         |                           $zkHelpString
         |  -n, --namespace <value>  The namespace, using kyuubi-defaults/conf if absent.
         |  -s, --host <value>       Hostname or IP address of a service.
         |  -p, --port <value>       Listening port of a service.
         |  -v, --version <value>    $versionHelpString
         |
         |Command: create [batch|server] [options]
         |${"\t"}Create a resource.
         |  -f, --filename <value>   Filename to use to create the resource
         |Command: create batch
         |${"\t"}Open batch session.
         |Command: create server
         |${"\t"}Expose Kyuubi server instance to another domain.
         |
         |Command: get [batch|server|engine] <args>...
         |${"\t"}Display information about the specified resources.
         |Command: get batch [<batchId>]
         |${"\t"}Get batch by id.
         |  <batchId>                Batch id.
         |Command: get server
         |${"\t"}Get Kyuubi server info of domain
         |Command: get engine [options]
         |${"\t"}Get Kyuubi engine info belong to a user.
         |  -u, --user <value>       The user name this engine belong to.
         |  -et, --engine-type <value>
         |                           The engine type this engine belong to.
         |  -es, --engine-subdomain <value>
         |                           The engine subdomain this engine belong to.
         |  -esl, --engine-share-level <value>
         |                           The engine share level this engine belong to.
         |
         |Command: delete [batch|server|engine] <args>...
         |${"\t"}Delete resources.
         |Command: delete batch [<batchId>]
         |${"\t"}Close batch session.
         |  <batchId>                Batch id.
         |Command: delete server
         |${"\t"}Delete the specified service node for a domain
         |Command: delete engine [options]
         |${"\t"}Delete the specified engine node for user.
         |  -u, --user <value>       The user name this engine belong to.
         |  -et, --engine-type <value>
         |                           The engine type this engine belong to.
         |  -es, --engine-subdomain <value>
         |                           The engine subdomain this engine belong to.
         |  -esl, --engine-share-level <value>
         |                           The engine share level this engine belong to.
         |
         |Command: list [batch|session|server|engine]
         |${"\t"}List information about resources.
         |Command: list batch [options]
         |${"\t"}List batch session info.
         |  --batchType <value>      Batch type.
         |  --batchUser <value>      Batch user.
         |  --batchState <value>     Batch state.
         |  --batchName <value>      Batch name.
         |  --createTime <value>     Batch create time, should be in yyyyMMddHHmmss format.
         |  --endTime <value>        Batch end time, should be in yyyyMMddHHmmss format.
         |  --from <value>           Specify which record to start from retrieving info.
         |  --size <value>           The max number of records returned in the query.
         |  --desc <value>           List the batches in descending order.
         |Command: list session
         |${"\t"}List all the live sessions
         |Command: list server
         |${"\t"}List all the service nodes for a particular domain
         |Command: list engine [options]
         |${"\t"}List all the engine nodes for a user
         |  -u, --user <value>       The user name this engine belong to.
         |  -et, --engine-type <value>
         |                           The engine type this engine belong to.
         |  -es, --engine-subdomain <value>
         |                           The engine subdomain this engine belong to.
         |  -esl, --engine-share-level <value>
         |                           The engine share level this engine belong to.
         |
         |Command: log [batch] [options] <args>...
         |${"\t"}Print the logs for specified resource.
         |  --forward                If forward is specified, the ctl will block forever.
         |Command: log batch [options] [<batchId>]
         |${"\t"}Get batch session local log.
         |  <batchId>                Batch id.
         |  --from <value>           Specify which record to start from retrieving info.
         |  --size <value>           The max number of records returned in the query.
         |
         |Command: submit [batch] [options]
         |${"\t"}Combination of create, get and log commands.
         |  -f, --filename <value>   Filename to use to create the resource
         |Command: submit batch [options]
         |${"\t"}open batch session and wait for completion.
         |  --waitCompletion <value>
         |                           ${waitBatchCompletionHelpString}
         |
         |  -h, --help               Show help message and exit.""".stripMargin

    testHelpExit(Array("--help"), helpString)
  }

  test("test kyuubi conf property") {
    val args = Seq(
      "delete",
      "batch",
      "123",
      "--conf",
      s"${CtlConf.CTL_REST_CLIENT_REQUEST_MAX_ATTEMPTS.key}=10")
    val opArgs = new ControlCliArguments(args)
    assert(opArgs.cliConfig.action.toString.equalsIgnoreCase("DELETE"))
    assert(opArgs.cliConfig.resource.toString.equalsIgnoreCase("BATCH"))
    assert(opArgs.cliConfig.batchOpts.batchId === "123")
    assert(opArgs.cliConfig.conf ===
      Map(CtlConf.CTL_REST_CLIENT_REQUEST_MAX_ATTEMPTS.key -> "10"))

    val args2 = Seq(
      "delete",
      "batch",
      "123",
      "--conf",
      s"${CtlConf.CTL_REST_CLIENT_REQUEST_MAX_ATTEMPTS.key}")
    testPrematureExitForControlCliArgs(args2.toArray, "Kyuubi config without '='")
  }

  test("test ctl client conf") {
    val args = Seq(
      "delete",
      "batch",
      "123",
      "--hostUrl",
      "https://kyuubi.test.com",
      "--conf",
      s"${CtlConf.CTL_REST_CLIENT_CONNECT_TIMEOUT.key}=5000",
      "--conf",
      s"${CtlConf.CTL_REST_CLIENT_REQUEST_MAX_ATTEMPTS.key}=1")
    val opArgs = new ControlCliArguments(args)
    withKyuubiRestClient(opArgs.cliConfig, null, opArgs.command.conf) { kyuubiRestClient =>
      assert(kyuubiRestClient.getConf.getConnectTimeout == 5000)
      assert(kyuubiRestClient.getConf.getSocketTimeout == 120000)
      assert(kyuubiRestClient.getConf.getMaxAttempts == 1)
      assert(kyuubiRestClient.getConf.getAttemptWaitTime == 3000)
    }
  }
}
