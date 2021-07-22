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

package org.apache.kyuubi.ctl.commands.server

import com.beust.jcommander.{JCommander, Parameter, Parameters, UnixStyleUsageFormatter}

import org.apache.kyuubi.ctl.commands.common.AbstractCommand

@Parameters(commandNames = Array("create"))
class CreateServerCommand extends AbstractCommand {

  @Parameter(
    names = Array("-zk", "--zk-quorum"),
    description = "The connection string for the zookeeper ensemble, using zk quorum manually.",
    order = 1)
  var zkQuorum: String = _

  @Parameter(
    names = Array("-n", "--namespace"),
    description = "The namespace, using kyuubi-defaults/conf if absent.",
    order = 1)
  var namespace: String = _

  @Parameter(
    names = Array("-s", "--host"),
    description = "Hostname or IP address of a service.",
    order = 1)
  var host: String = _

  @Parameter(
    names = Array("-p", "--port"),
    description = "Listening port of a service.",
    order = 1)
  var port: String = _

  @Parameter(names = Array("-h", "--help"), help = true, order = 3)
  var help: Boolean = false

  override def run(jc: JCommander): Unit = {
    jc.setUsageFormatter(new UnixStyleUsageFormatter(jc))
    jc.usage()
    // println(s"${namespace} ${host}")
  }
}

