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

class DeleteServerCommand(cliConfig: CliConfig) extends DeleteCommand(cliConfig) {
  override def doRun(): Iterable[ServiceNodeInfo] = {
    withDiscoveryClient(conf) { discoveryClient =>
      val znodeRoot = CtlUtils.getZkServerNamespace(conf, normalizedCliConfig)
      val hostPortOpt =
        Some((normalizedCliConfig.zkOpts.host, normalizedCliConfig.zkOpts.port.toInt))
      val nodesToDelete = CtlUtils.getServiceNodes(discoveryClient, znodeRoot, hostPortOpt)

      val deletedNodes = ListBuffer[ServiceNodeInfo]()
      nodesToDelete.foreach { node =>
        val nodePath = s"$znodeRoot/${node.nodeName}"
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
