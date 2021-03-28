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
  val zkAddress = "localhost:2181"
  val namespace = "kyuubi"
  val user = "kyuubi"
  val host = "localhost"
  val port = "10000"

  /** Check whether the script exits and the given search string is printed. */
  private def testPrematureExit(args: Array[String], searchString: String): Unit = {
    val logAppender = new LogAppender("test premature exit")
    withLogAppender(logAppender) {
      val thread = new Thread {
        override def run() = try {
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
          "--zkAddress", zkAddress,
          "--namespace", namespace,
          "--user", user,
          "--host", host,
          "--port", port,
          "--version", KYUUBI_VERSION
        )
        val opArgs = new KyuubiCtlArguments(args)
        assert(opArgs.action.toString.equalsIgnoreCase(command))
        assert(opArgs.service.toString.equalsIgnoreCase(service))
        assert(opArgs.zkAddress == zkAddress)
        assert(opArgs.nameSpace == namespace)
        assert(opArgs.user == user)
        assert(opArgs.host == host)
        assert(opArgs.port == port)
        assert(opArgs.version == KYUUBI_VERSION)
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
      "--port", port,
    )
    val opArgs2 = new KyuubiCtlArguments(args2)
    assert(opArgs2.action == KyuubiCtlAction.HELP)
    assert(opArgs2.user == user)
    assert(opArgs2.verbose)
  }

  test("prints usage on empty input") {
    testPrematureExit(Array.empty[String], "Usage: kyuubi-ctl")
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
      "--zkAddress", zkAddress,
      "--namespace", namespace,
      "--user", user,
      "--host", host,
      "--port", port,
      "--version", KYUUBI_VERSION,
      "extraArg1", "extraArg2"
    )
    testPrematureExit(args, "Unknown/unsupported param extraArg1")
  }

  test("test missing arguments") {
    val args = Array(
      "list",
      "--namespace", namespace,
      "--user", user,
      "--host", host,
      "--port", port,
      "--version", KYUUBI_VERSION
    )
    testPrematureExit(args, "Zookeeper address is not specified")
  }
}
