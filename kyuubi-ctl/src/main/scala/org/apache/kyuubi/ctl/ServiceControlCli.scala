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

import scala.collection.mutable.ListBuffer

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.utils.ZKPaths

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.{ServiceDiscovery, ServiceNodeInfo}

private[ctl] object KyuubiCtlAction extends Enumeration {
  type KyuubiCtlAction = Value
  val CREATE, GET, DELETE, LIST, HELP = Value
}

private[ctl] object KyuubiCtlActionService extends Enumeration {
  type KyuubiCtlActionService = Value
  val SERVER, ENGINE = Value
}

/**
 * Main gateway of lauching a Kyuubi Ctl action.
 * See usage in [[KyuubiCtlArguments.printUsageAndExit]].
 */
private[kyuubi] class ServiceControlCli extends Logging {
  import ServiceDiscovery._

  def doAction(args: Array[String]): Unit = {
    val ctlArgs = parseArguments(args)
    if (ctlArgs.verbose) {
      info(ctlArgs.toString)
    }
    ctlArgs.action match {
      case KyuubiCtlAction.CREATE => create(ctlArgs)
      case KyuubiCtlAction.LIST => list(ctlArgs, filterHostPort = false)
      case KyuubiCtlAction.GET => list(ctlArgs, filterHostPort = true)
      case KyuubiCtlAction.DELETE => delete(ctlArgs)
      case KyuubiCtlAction.HELP => printUsage(ctlArgs)
    }
  }

  protected def parseArguments(args: Array[String]): KyuubiCtlArguments = {
    new KyuubiCtlArguments(args)
  }

  /**
   * Expose Kyuubi server instance to another domain.
   */
  private def create(args: KyuubiCtlArguments): Unit = {
    val kyuubiConf = args.defaultKyuubiConf

    kyuubiConf.setIfMissing(HA_ZK_QUORUM, args.zkQuorum)
    withZkClient(kyuubiConf) { zkClient =>
      val fromNamespace = kyuubiConf.get(HA_ZK_NAMESPACE)
      val fromZkPath = ZKPaths.makePath(null, fromNamespace)
      val currentServerNodes = getServiceNodesInfo(zkClient, fromZkPath)
      val exposedServiceNodes = ListBuffer[ServiceNodeInfo]()

      if (currentServerNodes.nonEmpty) {
        def doCreate(zc: CuratorFramework): Unit = {
          currentServerNodes.foreach { sn =>
            info(s"Exposing server instance:${sn.instance} with version:${sn.version}" +
              s" from $fromNamespace to ${args.zkQuorum}")
            val newNode = createZkServiceNode(
              kyuubiConf, zc, args.nameSpace, sn.instance, sn.version, true)
            exposedServiceNodes += sn.copy(nodeName = newNode.getActualPath.split("/").last)
          }
        }

        if (kyuubiConf.get(HA_ZK_QUORUM) == args.zkQuorum) {
          doCreate(zkClient)
        } else {
          kyuubiConf.set(HA_ZK_QUORUM, args.zkQuorum)
          withZkClient(kyuubiConf)(doCreate)
        }
      }

      info(s"Kyuubi service nodes exposed to ${args.zkQuorum}")
      renderServiceNodesInfo(exposedServiceNodes)
    }
  }

  /**
   * List Kyuubi server nodes info.
   */
  private def list(args: KyuubiCtlArguments, filterHostPort: Boolean): Unit = {
    withZkClient(args.defaultKyuubiConf) { zkClient =>
      val znodeRoot = getZkNamespace(args)
      val hostPortOpt = if (filterHostPort) Some((args.host, args.port.toInt)) else None
      val nodes = getServiceNodes(zkClient, znodeRoot, hostPortOpt)
      info("Zookeeper nodes list:")
      renderServiceNodesInfo(nodes)
    }
  }

  private def getServiceNodes(
      zkClient: CuratorFramework,
      znodeRoot: String,
      hostPortOpt: Option[(String, Int)]): Seq[ServiceNodeInfo] = {
    val serviceNodes = getServiceNodesInfo(zkClient, znodeRoot)
    hostPortOpt match {
      case Some((host, port)) => serviceNodes.filter { sn =>
        sn.host == host && sn.port == port
      }
      case _ => serviceNodes
    }
  }

  /**
   * Delete zookeeper service node with specified host port.
   */
  private def delete(args: KyuubiCtlArguments): Unit = {
    withZkClient(args.defaultKyuubiConf) { zkClient =>
      val znodeRoot = getZkNamespace(args)
      val hostPortOpt = Some((args.host, args.port.toInt))
      val nodesToDelete = getServiceNodes(zkClient, znodeRoot, hostPortOpt)

      nodesToDelete.foreach { node =>
        val nodePath = s"$znodeRoot/${node.nodeName}"
        info(s"Deleting zookeeper service node:$nodePath")
        zkClient.delete().forPath(nodePath)
      }

      val remainingNodes = getServiceNodes(zkClient, znodeRoot, hostPortOpt)
      info("The nodes that were not deleted successfully:")
      renderServiceNodesInfo(remainingNodes)

      val deletedNodes = nodesToDelete.filterNot { node =>
        remainingNodes.exists(_.nodeName == node.nodeName)
      }
      info("Deleted zookeeper nodes:")
      renderServiceNodesInfo(deletedNodes)
    }
  }

  private def getZkNamespace(args: KyuubiCtlArguments): String = {
    args.service match {
      case KyuubiCtlActionService.SERVER =>
        ZKPaths.makePath(null, args.nameSpace)
      case KyuubiCtlActionService.ENGINE =>
        ZKPaths.makePath(s"${args.nameSpace}_${ShareLevel.USER}", args.user)
    }
  }

  private def printUsage(args: KyuubiCtlArguments): Unit = {
    args.printUsageAndExit(0)
  }

  private def renderServiceNodesInfo(serviceNodeInfo: Seq[ServiceNodeInfo]): Unit = {
    val header = Seq("Service Node", "HOST", "PORT", "VERSION")
    val rows = serviceNodeInfo.sortBy(_.nodeName).map { sn =>
      Seq(sn.nodeName, sn.host, sn.port.toString, sn.version.getOrElse(""))
    }
    info(Tabulator.format(header, rows))
  }
}

object ServiceControlCli extends CommandLineUtils with Logging {
  override def main(args: Array[String]): Unit = {
    val ctl = new ServiceControlCli() {
      self =>

      override protected def parseArguments(args: Array[String]): KyuubiCtlArguments = {
        new KyuubiCtlArguments(args) {
          override def info(msg: => Any): Unit = self.info(msg)

          override def warn(msg: => Any): Unit = self.warn(msg)

          override def error(msg: => Any): Unit = self.error(msg)
        }
      }

      override def info(msg: => Any): Unit = printMessage(msg)

      override def warn(msg: => Any): Unit = printMessage(s"Warning: $msg")

      override def error(msg: => Any): Unit = printMessage(s"Error: $msg")

      override def doAction(args: Array[String]): Unit = {
        try {
          super.doAction(args)
          exitFn(0)
        } catch {
          case e: KyuubiCtlException =>
            exitFn(e.exitCode)
        }
      }
    }

    ctl.doAction(args)
  }
}
