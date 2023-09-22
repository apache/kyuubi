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
package org.apache.kyuubi.ctl.cmd.create

import scala.collection.mutable.ListBuffer

import org.apache.kyuubi.ctl.cmd.Command
import org.apache.kyuubi.ctl.opt.{CliConfig, ControlObject}
import org.apache.kyuubi.ctl.util.{CtlUtils, Render, Validator}
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.{DiscoveryClient, DiscoveryPaths, ServiceNodeInfo}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient

class CreateServerCommand(cliConfig: CliConfig)
  extends Command[Iterable[ServiceNodeInfo]](cliConfig) {

  def validate(): Unit = {
    if (normalizedCliConfig.resource != ControlObject.SERVER) {
      fail("Only support expose Kyuubi server instance to another domain")
    }

    Validator.validateZkArguments(normalizedCliConfig)

    val defaultNamespace = conf.getOption(HA_NAMESPACE.key)
      .getOrElse(HA_NAMESPACE.defaultValStr)
    if (defaultNamespace.equals(normalizedCliConfig.zkOpts.namespace)) {
      fail(
        s"""
           |Only support expose Kyuubi server instance to another domain, a different namespace
           |than the default namespace [$defaultNamespace] should be specified.
        """.stripMargin)
    }

  }

  /**
   * Expose Kyuubi server instance to another domain.
   */
  override def doRun(): Iterable[ServiceNodeInfo] = {
    val kyuubiConf = conf

    kyuubiConf.setIfMissing(HA_ADDRESSES, normalizedCliConfig.zkOpts.zkQuorum)
    withDiscoveryClient(kyuubiConf) { discoveryClient =>
      val fromNamespace =
        DiscoveryPaths.makePath(null, kyuubiConf.get(HA_NAMESPACE))
      val toNamespace = CtlUtils.getZkServerNamespace(kyuubiConf, normalizedCliConfig)

      val currentServerNodes = discoveryClient.getServiceNodesInfo(fromNamespace)
      val exposedServiceNodes = ListBuffer[ServiceNodeInfo]()

      if (currentServerNodes.nonEmpty) {
        def doCreate(zc: DiscoveryClient): Unit = {
          currentServerNodes.foreach { sn =>
            info(s"Exposing server instance:${sn.instance} with version:${sn.version}" +
              s" from $fromNamespace to $toNamespace")
            val newNodePath = zc.createAndGetServiceNode(
              kyuubiConf,
              normalizedCliConfig.zkOpts.namespace,
              sn.instance,
              sn.version,
              true)
            exposedServiceNodes += sn.copy(
              namespace = toNamespace,
              nodeName = newNodePath.split("/").last)
          }
        }

        if (kyuubiConf.get(HA_ADDRESSES) == normalizedCliConfig.zkOpts.zkQuorum) {
          doCreate(discoveryClient)
        } else {
          kyuubiConf.set(HA_ADDRESSES, normalizedCliConfig.zkOpts.zkQuorum)
          withDiscoveryClient(kyuubiConf)(doCreate)
        }
      }
      exposedServiceNodes
    }
  }

  override def render(nodes: Iterable[ServiceNodeInfo]): Unit = {
    val title = "Created zookeeper service nodes"
    info(Render.renderServiceNodesInfo(title, nodes))
  }
}
