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

package org.apache.kyuubi.ctl.commands.engine

import com.beust.jcommander.{JCommander, Parameter, Parameters, UnixStyleUsageFormatter}
import com.google.common.annotations.VisibleForTesting

import org.apache.kyuubi.ctl.commands.common.AbstractCommand

@Parameters(commandNames = Array("get"))
class GetEngineCommand extends AbstractCommand {

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
  var port: Int = _

  @Parameter(
    names = Array("-u", "--user"),
    description = "The user name this engine belong to.",
    order = 1)
  var user: String = _

  @Parameter(names = Array("-h", "--help"), help = true, order = 3)
  var help: Boolean = false

  override def run(jc: JCommander): Unit = {
    jc.setUsageFormatter(new UnixStyleUsageFormatter(jc))
    jc.usage()
  }

  @VisibleForTesting
  def test(): (String, String, Int) = {
    (namespace, host, port)
  }
}

