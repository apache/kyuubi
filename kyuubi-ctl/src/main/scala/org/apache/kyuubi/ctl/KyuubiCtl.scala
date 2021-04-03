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

import org.apache.curator.framework.CuratorFramework
import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.ServiceDiscovery

import java.net.InetAddress

private[ctl] object  KyuubiCtlAction extends Enumeration {
  type KyuubiCtlAction = Value
  val CREATE, GET, DELETE, LIST, HELP = Value
}

private[ctl] object KyuubiCtlActionService extends Enumeration {
  type KyuubiCtlActionService = Value
  val SERVER, ENGINE = Value
}


private[kyuubi] class KyuubiCtl extends Logging {
  import ServiceDiscovery._

  def doAction(args: Array[String]): Unit = {
    val ctlArgs = parseArguments(args)
    if (ctlArgs.verbose) {
      info(ctlArgs.toString)
    }
    ctlArgs.action match {
      case KyuubiCtlAction.CREATE => create(ctlArgs)
      case KyuubiCtlAction.GET => get(ctlArgs)
      case KyuubiCtlAction.DELETE => delete(ctlArgs)
      case KyuubiCtlAction.LIST => list(ctlArgs)
      case KyuubiCtlAction.HELP => printUsage(ctlArgs)
    }
  }

  protected def parseArguments(args: Array[String]): KyuubiCtlArguments = {
    new KyuubiCtlArguments(args)
  }

  private def create(args: KyuubiCtlArguments): Unit = {
    val zkClient = getZkClient(args)
    val instance = getInstance(args.host, args.port)
    ServiceDiscovery.createZkNode(zkClient, args.nameSpace, instance)
  }

  private def get(args: KyuubiCtlArguments): Unit = {

  }

  private def delete(args: KyuubiCtlArguments): Unit = {

  }

  private def list(args: KyuubiCtlArguments): Unit = {

  }

  private def printUsage(args: KyuubiCtlArguments): Unit = {
    args.printUsageAndExit(0)
  }

  private def getZkClient(args: KyuubiCtlArguments): CuratorFramework = {
    val conf = new KyuubiConf().loadFileDefaults()
    conf.set(HA_ZK_QUORUM, args.zkAddress)
    conf.set(HA_ZK_NAMESPACE.key, args.nameSpace)
    ServiceDiscovery.startZookeeperClient(conf)
  }
}

object KyuubiCtl extends CommandLineUtils with Logging {

  override def main(args: Array[String]): Unit = {
    val ctl = new KyuubiCtl() {
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
        } catch {
          case e: KyuubiCtlException =>
            exitFn(e.exitCode)
        }
      }
    }

    ctl.doAction(args)
  }
}
