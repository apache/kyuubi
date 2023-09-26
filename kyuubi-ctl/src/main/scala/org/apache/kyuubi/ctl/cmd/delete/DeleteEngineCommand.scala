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
package org.apache.kyuubi.ctl.cmd.delete

import scala.collection.mutable.ListBuffer

import org.apache.kyuubi.ctl.opt.CliConfig
import org.apache.kyuubi.ctl.util.CtlUtils
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.ha.client.ServiceNodeInfo

class DeleteEngineCommand(cliConfig: CliConfig) extends DeleteCommand(cliConfig) {

  override def validate(): Unit = {
    super.validate()

    // validate user
    if (normalizedCliConfig.engineOpts.user == null) {
      fail("Must specify user name for engine, please use -u or --user.")
    }
  }

  override def doRun(): Iterable[ServiceNodeInfo] = {
    withDiscoveryClient(conf) { discoveryClient =>
      val hostPortOpt =
        Some((cliConfig.zkOpts.host, cliConfig.zkOpts.port.toInt))
      val candidateNodes = CtlUtils.listZkEngineNodes(conf, normalizedCliConfig, hostPortOpt)
      hostPortOpt.map { case (host, port) =>
        candidateNodes.filter { cn => cn.host == host && cn.port == port }
      }.getOrElse(candidateNodes)
      val deletedNodes = ListBuffer[ServiceNodeInfo]()
      candidateNodes.foreach { node =>
        val engineNode = discoveryClient.getChildren(node.namespace)(0)
        val nodePath = s"${node.namespace}/$engineNode"
        info(s"Deleting zookeeper service node:$nodePath")
        try {
          discoveryClient.delete(nodePath)
          deletedNodes += node
        } catch {
          case e: Exception =>
            error(s"Failed to delete zookeeper service node:$nodePath", e)
        }
      }
      deletedNodes
    }
  }
}
