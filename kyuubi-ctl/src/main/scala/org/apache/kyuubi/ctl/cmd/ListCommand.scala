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
package org.apache.kyuubi.ctl.cmd

import org.apache.kyuubi.ctl.{CliConfig, ControlObject, Render}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient

class ListCommand(cliConfig: CliConfig) extends Command(cliConfig) {

  override def validateArguments(): Unit = {
    validateZkArguments()
    cliArgs.service match {
      case ControlObject.ENGINE => validateUser()
      case _ =>
    }
    mergeArgsIntoKyuubiConf()
  }

  override def run(): Unit = {
    list(filterHostPort = false)
  }

  /**
   * List Kyuubi server nodes info.
   */
  private def list(filterHostPort: Boolean): Unit = {
    withDiscoveryClient(conf) { discoveryClient =>
      val znodeRoot = getZkNamespace()
      val hostPortOpt =
        if (filterHostPort) {
          Some((cliArgs.commonOpts.host, cliArgs.commonOpts.port.toInt))
        } else None
      val nodes = getServiceNodes(discoveryClient, znodeRoot, hostPortOpt)

      val title = "Zookeeper service nodes"
      info(Render.renderServiceNodesInfo(title, nodes, verbose))
    }
  }
}
