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

private[ctl] object ServiceControlAction extends Enumeration {
  type ServiceControlAction = Value
  val CREATE, GET, DELETE, LIST, HELP = Value
}

private[ctl] object ServiceControlObject extends Enumeration {
  type ServiceControlObject = Value
  val SERVER, ENGINE = Value
}

/**
 * Main gateway of launching a Kyuubi Ctl action.
 * See usage in [[ServiceControlCliArguments.printUsageAndExit]].
 */
private[kyuubi] class ServiceControlCli extends Logging {
  import ServiceControlCli._
  import ServiceDiscovery._

  private var verbose: Boolean = false

  def doAction(args: Array[String]): Unit = {
    // Initialize logging if it hasn't been done yet.
    // Set log level ERROR
    initializeLoggerIfNecessary(true)

    val ctlArgs = parseArguments(args)
    verbose = ctlArgs.verbose
    if (verbose) {
      super.info(ctlArgs.toString)
    }
    ctlArgs.action match {
      case ServiceControlAction.CREATE => create(ctlArgs)
      case ServiceControlAction.LIST => list(ctlArgs, filterHostPort = false)
      case ServiceControlAction.GET => list(ctlArgs, filterHostPort = true)
      case ServiceControlAction.DELETE => delete(ctlArgs)
      case ServiceControlAction.HELP => printUsage(ctlArgs)
    }
  }

  protected def parseArguments(args: Array[String]): ServiceControlCliArguments = {
    new ServiceControlCliArguments(args)
  }

  /**
   * Expose Kyuubi server instance to another domain.
   */
  private def create(args: ServiceControlCliArguments): Unit = {
    val kyuubiConf = args.conf

    kyuubiConf.setIfMissing(HA_ZK_QUORUM, args.zkQuorum)
    withZkClient(kyuubiConf) { zkClient =>
      val fromNamespace = ZKPaths.makePath(null, kyuubiConf.get(HA_ZK_NAMESPACE))
      val toNamespace = getZkNamespace(args)

      val currentServerNodes = getServiceNodesInfo(zkClient, fromNamespace)
      val exposedServiceNodes = ListBuffer[ServiceNodeInfo]()

      if (currentServerNodes.nonEmpty) {
        def doCreate(zc: CuratorFramework): Unit = {
          currentServerNodes.foreach { sn =>
            info(s"Exposing server instance:${sn.instance} with version:${sn.version}" +
              s" from $fromNamespace to $toNamespace")
            val newNode = createZkServiceNode(
              kyuubiConf, zc, args.namespace, sn.instance, sn.version, true)
            exposedServiceNodes += sn.copy(
              namespace = toNamespace,
              nodeName = newNode.getActualPath.split("/").last)
          }
        }

        if (kyuubiConf.get(HA_ZK_QUORUM) == args.zkQuorum) {
          doCreate(zkClient)
        } else {
          kyuubiConf.set(HA_ZK_QUORUM, args.zkQuorum)
          withZkClient(kyuubiConf)(doCreate)
        }
      }

      val title = "Created zookeeper service nodes"
      info(renderServiceNodesInfo(title, exposedServiceNodes, verbose))
    }
  }

  /**
   * List Kyuubi server nodes info.
   */
  private def list(args: ServiceControlCliArguments, filterHostPort: Boolean): Unit = {
    withZkClient(args.conf) { zkClient =>
      val znodeRoot = getZkNamespace(args)
      val hostPortOpt = if (filterHostPort) Some((args.host, args.port.toInt)) else None
      val nodes = getServiceNodes(zkClient, znodeRoot, hostPortOpt)

      val title = "Zookeeper service nodes"
      info(renderServiceNodesInfo(title, nodes, verbose))
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
  private def delete(args: ServiceControlCliArguments): Unit = {
    withZkClient(args.conf) { zkClient =>
      val znodeRoot = getZkNamespace(args)
      val hostPortOpt = Some((args.host, args.port.toInt))
      val nodesToDelete = getServiceNodes(zkClient, znodeRoot, hostPortOpt)

      val deletedNodes = ListBuffer[ServiceNodeInfo]()
      nodesToDelete.foreach { node =>
        val nodePath = s"$znodeRoot/${node.nodeName}"
        info(s"Deleting zookeeper service node:$nodePath")
        try {
          zkClient.delete().forPath(nodePath)
          deletedNodes += node
        } catch {
          case e: Exception =>
            error(s"Failed to delete zookeeper service node:$nodePath", e)
        }
      }

      val title = "Deleted zookeeper service nodes"
      info(renderServiceNodesInfo(title, deletedNodes, verbose))
    }
  }

  private def printUsage(args: ServiceControlCliArguments): Unit = {
    args.printUsageAndExit(0)
  }
}

object ServiceControlCli extends CommandLineUtils with Logging {
  override def main(args: Array[String]): Unit = {
    val ctl = new ServiceControlCli() {
      self =>

      override protected def parseArguments(args: Array[String]): ServiceControlCliArguments = {
        new ServiceControlCliArguments(args) {
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
          case e: ServiceControlCliException =>
            exitFn(e.exitCode)
        }
      }
    }

    ctl.doAction(args)
  }

  private[ctl] def getZkNamespace(args: ServiceControlCliArguments): String = {
    args.service match {
      case ServiceControlObject.SERVER =>
        ZKPaths.makePath(null, args.namespace)
      case ServiceControlObject.ENGINE =>
        ZKPaths.makePath(s"${args.namespace}_${ShareLevel.USER}", args.user)
    }
  }

  private[ctl] def renderServiceNodesInfo(
      title: String, serviceNodeInfo: Seq[ServiceNodeInfo],
      verbose: Boolean): String = {
    val header = Seq("Namespace", "Host", "Port", "Version")
    val rows = serviceNodeInfo.sortBy(_.nodeName).map { sn =>
      Seq(sn.namespace, sn.host, sn.port.toString, sn.version.getOrElse(""))
    }
    Tabulator.format(title, header, rows, verbose)
  }
}
