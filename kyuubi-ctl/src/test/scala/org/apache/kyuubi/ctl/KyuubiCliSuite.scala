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

import com.beust.jcommander.JCommander

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.ctl.commands.engine.GetEngineCommand

class KyuubiCliSuite extends KyuubiFunSuite with TestPrematureExit {

  test("test jcommander parse args") {

    val args = Array(
      "--namespace", "namespace",
      "--host", "localhost",
      "--port", "65535"
    )

    val getEngineCommand = new GetEngineCommand
    val engineCommand = JCommander.newBuilder()
      .addObject(getEngineCommand)
      .build()

    engineCommand.parse(args: _*)
    assert(("namespace", "localhost", 65535).equals(getEngineCommand.test()))
  }

  test("test kyuubi cli usage") {

    val args = Array(
      "--help"
    )

    KyuubiCli.main(args)
  }

  test("test kyuubi cli parse args") {

    val args = Array(
      "engine", "get",
      "--namespace", "namespace",
      "--host", "localhost",
      "--port", "65535"
    )

    KyuubiCli.main(args)
  }

}
