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
package org.apache.kyuubi.ctl.cmd.list

import org.apache.kyuubi.ctl.cmd.Command
import org.apache.kyuubi.ctl.opt.CliConfig
import org.apache.kyuubi.ctl.util.{CtlUtils, Render, Validator}
import org.apache.kyuubi.ha.client.ServiceNodeInfo

class ListCommand(cliConfig: CliConfig) extends Command[Seq[ServiceNodeInfo]](cliConfig) {

  def validate(): Unit = {
    Validator.validateZkArguments(normalizedCliConfig)
    mergeArgsIntoKyuubiConf()
  }

  def doRun(): Seq[ServiceNodeInfo] = {
    CtlUtils.listZkServerNodes(conf, normalizedCliConfig, filterHostPort = false)
  }

  def render(nodes: Seq[ServiceNodeInfo]): Unit = {
    val title = "Zookeeper service nodes"
    info(Render.renderServiceNodesInfo(title, nodes))
  }
}
