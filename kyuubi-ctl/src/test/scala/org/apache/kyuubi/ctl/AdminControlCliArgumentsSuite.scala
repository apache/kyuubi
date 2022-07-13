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

class AdminControlCliArgumentsSuite extends KyuubiFunSuite with TestPrematureExit {

  /** Check whether the script exits and the given search string is printed. */
  private def testHelpExit(args: Array[String], searchString: String): Unit = {
    val logAppender = new LogAppender("test premature exit")
    withLogAppender(logAppender) {
      val thread = new Thread {
        override def run(): Unit =
          try {
            new AdminControlCliArguments(args) {
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

  test("test refresh config") {
    var args = Array(
      "refresh",
      "server",
      "hadoopConf")
    testPrematureExitForAdminControlCliArgs(args, "Unknown argument 'server'")

    args = Array(
      "refresh",
      "config",
      "hadoopConf")
    val opArgs = new AdminControlCliArguments(args)
    assert(opArgs.cliConfig.action === ControlAction.REFRESH)
    assert(opArgs.cliConfig.resource === ControlObject.CONFIG)
    assert(opArgs.cliConfig.adminConfigOpts.configType === "hadoopConf")

    args = Array(
      "refresh",
      "config",
      "--hostUrl",
      "https://kyuubi.test.com",
      "otherConf")
    testPrematureExitForAdminControlCli(args, "Invalid config type:otherConf")
  }

  test("test --help") {
    // scalastyle:off
    val helpString =
      s"""kyuubi $KYUUBI_VERSION
         |Usage: kyuubi-adminctl [refresh] [options]
         |
         |  -v, --version <value>    Using the compiled KYUUBI_VERSION default, change it if the active service is running in another.
         |  -b, --verbose            Print additional debug output.
         |  --hostUrl <value>        Host url for rest api.
         |  --authSchema <value>     Auth schema for rest api, valid values are basic, spnego.
         |  --username <value>       Username for basic authentication.
         |  --password <value>       Password for basic authentication.
         |  --spnegoHost <value>     Spnego host for spnego authentication.
         |  --conf <value>           Kyuubi config property pair, formatted key=value.
         |
         |Command: refresh [config] <args>...
         |	Refresh the resource.
         |Command: refresh config [<configType>]
         |	Refresh the config with specified type.
         |  <configType>             The valid config type can be one of the following: hadoopConf.
         |
         |  -h, --help               Show help message and exit.""".stripMargin
    // scalastyle:on
    testHelpExit(Array("--help"), helpString)
  }
}
