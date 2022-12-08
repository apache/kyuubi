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
package org.apache.kyuubi.ctl.util

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.{Map => JMap}

import org.yaml.snakeyaml.Yaml

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SHARE_LEVEL, ENGINE_SHARE_LEVEL_SUBDOMAIN, ENGINE_TYPE}
import org.apache.kyuubi.ctl.opt.{CliConfig, ControlObject}
import org.apache.kyuubi.ha.client.{DiscoveryClient, DiscoveryPaths, ServiceNodeInfo}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient

object CtlUtils {

  private[ctl] def getZkNamespace(conf: KyuubiConf, cliConfig: CliConfig): String = {
    cliConfig.resource match {
      case ControlObject.SERVER =>
        DiscoveryPaths.makePath(null, cliConfig.zkOpts.namespace)
      case ControlObject.ENGINE =>
        val engineType = Some(cliConfig.engineOpts.engineType)
          .filter(_ != null).filter(_.nonEmpty)
          .getOrElse(conf.get(ENGINE_TYPE))
        val engineSubdomain = Some(cliConfig.engineOpts.engineSubdomain)
          .filter(_ != null).filter(_.nonEmpty)
          .getOrElse(conf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN).getOrElse("default"))
        val engineShareLevel = Some(cliConfig.engineOpts.engineShareLevel)
          .filter(_ != null).filter(_.nonEmpty)
          .getOrElse(conf.get(ENGINE_SHARE_LEVEL))
        // The path of the engine defined in zookeeper comes from
        // org.apache.kyuubi.engine.EngineRef#engineSpace
        DiscoveryPaths.makePath(
          s"${cliConfig.zkOpts.namespace}_" +
            s"${cliConfig.zkOpts.version}_" +
            s"${engineShareLevel}_${engineType}",
          cliConfig.engineOpts.user,
          engineSubdomain)
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

  /**
   * List Kyuubi server nodes info.
   */
  private[ctl] def listZkServerNodes(
      conf: KyuubiConf,
      cliConfig: CliConfig,
      filterHostPort: Boolean): Seq[ServiceNodeInfo] = {
    var nodes = Seq.empty[ServiceNodeInfo]
    withDiscoveryClient(conf) { discoveryClient =>
      val znodeRoot = getZkNamespace(conf, cliConfig)
      val hostPortOpt =
        if (filterHostPort) {
          Some((cliConfig.zkOpts.host, cliConfig.zkOpts.port.toInt))
        } else None
      nodes = getServiceNodes(discoveryClient, znodeRoot, hostPortOpt)
    }
    nodes
  }

  private[ctl] def loadYamlAsMap(cliConfig: CliConfig): JMap[String, Object] = {
    val filename = cliConfig.createOpts.filename

    var map: JMap[String, Object] = null
    var br: BufferedReader = null
    try {
      val yaml = new Yaml()
      val input = new FileInputStream(new File(filename))
      br = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))
      map = yaml.load(br).asInstanceOf[JMap[String, Object]]
    } catch {
      case e: Exception => throw new KyuubiException(s"Failed to read yaml file[$filename]: $e")
    } finally {
      if (br != null) {
        br.close()
      }
    }
    map
  }
}
