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

import java.net.InetAddress

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SHARE_LEVEL, ENGINE_SHARE_LEVEL_SUBDOMAIN, ENGINE_TYPE}
import org.apache.kyuubi.ctl.{CliConfig, ControlObject}
import org.apache.kyuubi.ctl.ControlCli.printMessage
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.{DiscoveryClient, DiscoveryPaths, ServiceNodeInfo}

abstract class Command(var cliArgs: CliConfig) extends Logging {

  val conf = KyuubiConf().loadFileDefaults()

  val verbose = cliArgs.commonOpts.verbose

  def preProcess(): Unit = {
    this.cliArgs = useDefaultPropertyValueIfMissing()
    validateArguments()
  }

  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  def validateArguments(): Unit

  def run(): Unit

  def fail(msg: String): Unit = throw new KyuubiException(msg)

  protected def validateZkArguments(): Unit = {
    if (cliArgs.commonOpts.zkQuorum == null) {
      fail("Zookeeper quorum is not specified and no default value to load")
    }
    if (cliArgs.commonOpts.namespace == null) {
      fail("Zookeeper namespace is not specified and no default value to load")
    }
  }

  protected def validateHostAndPort(): Unit = {
    if (cliArgs.commonOpts.host == null) {
      fail("Must specify host for service")
    }
    if (cliArgs.commonOpts.port == null) {
      fail("Must specify port for service")
    }

    try {
      InetAddress.getByName(cliArgs.commonOpts.host)
    } catch {
      case _: Exception =>
        fail(s"Unknown host: ${cliArgs.commonOpts.host}")
    }

    try {
      if (cliArgs.commonOpts.port.toInt <= 0) {
        fail(s"Specified port should be a positive number")
      }
    } catch {
      case _: NumberFormatException =>
        fail(s"Specified port is not a valid integer number: ${cliArgs.commonOpts.port}")
    }
  }

  protected def validateUser(): Unit = {
    if (cliArgs.service == ControlObject.ENGINE && cliArgs.engineOpts.user == null) {
      fail("Must specify user name for engine, please use -u or --user.")
    }
  }

  protected def mergeArgsIntoKyuubiConf(): Unit = {
    conf.set(HA_ADDRESSES.key, cliArgs.commonOpts.zkQuorum)
    conf.set(HA_NAMESPACE.key, cliArgs.commonOpts.namespace)
  }

  private def useDefaultPropertyValueIfMissing(): CliConfig = {
    var arguments: CliConfig = cliArgs.copy()
    if (cliArgs.commonOpts.zkQuorum == null) {
      conf.getOption(HA_ADDRESSES.key).foreach { v =>
        if (verbose) {
          super.info(s"Zookeeper quorum is not specified, use value from default conf:$v")
        }
        arguments = arguments.copy(commonOpts = arguments.commonOpts.copy(zkQuorum = v))
      }
    }

    if (arguments.commonOpts.namespace == null) {
      arguments = arguments.copy(commonOpts =
        arguments.commonOpts.copy(namespace = conf.get(HA_NAMESPACE)))
      if (verbose) {
        super.info(s"Zookeeper namespace is not specified, use value from default conf:" +
          s"${arguments.commonOpts.namespace}")
      }
    }

    if (arguments.commonOpts.version == null) {
      if (verbose) {
        super.info(s"version is not specified, use built-in KYUUBI_VERSION:$KYUUBI_VERSION")
      }
      arguments = arguments.copy(commonOpts = arguments.commonOpts.copy(version = KYUUBI_VERSION))
    }
    arguments
  }

  private[ctl] def getZkNamespace(): String = {
    cliArgs.service match {
      case ControlObject.SERVER =>
        DiscoveryPaths.makePath(null, cliArgs.commonOpts.namespace)
      case ControlObject.ENGINE =>
        val engineType = Some(cliArgs.engineOpts.engineType)
          .filter(_ != null).filter(_.nonEmpty)
          .getOrElse(conf.get(ENGINE_TYPE))
        val engineSubdomain = Some(cliArgs.engineOpts.engineSubdomain)
          .filter(_ != null).filter(_.nonEmpty)
          .getOrElse(conf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN).getOrElse("default"))
        val engineShareLevel = Some(cliArgs.engineOpts.engineShareLevel)
          .filter(_ != null).filter(_.nonEmpty)
          .getOrElse(conf.get(ENGINE_SHARE_LEVEL))
        // The path of the engine defined in zookeeper comes from
        // org.apache.kyuubi.engine.EngineRef#engineSpace
        DiscoveryPaths.makePath(
          s"${cliArgs.commonOpts.namespace}_${cliArgs.commonOpts.version}_" +
            s"${engineShareLevel}_${engineType}",
          cliArgs.engineOpts.user,
          Array(engineSubdomain))
    }
  }

  private[ctl] def getServiceNodes(
      discoveryClient: DiscoveryClient,
      znodeRoot: String,
      hostPortOpt: Option[(String, Int)]): Seq[ServiceNodeInfo] = {
    val serviceNodes = discoveryClient.getServiceNodesInfo(znodeRoot)
    hostPortOpt match {
      case Some((host, port)) => serviceNodes.filter { sn =>
          sn.host == host && sn.port == port
        }
      case _ => serviceNodes
    }
  }

  override def info(msg: => Any): Unit = printMessage(msg)

  override def warn(msg: => Any): Unit = printMessage(s"Warning: $msg")

  override def error(msg: => Any): Unit = printMessage(s"Error: $msg")

}
