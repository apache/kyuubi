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

package org.apache.kyuubi.ha.client

import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.google.common.annotations.VisibleForTesting
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.utils.ZKPaths

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.zookeeper.ServiceDiscoveryClient
import org.apache.kyuubi.ha.client.zookeeper.ServiceDiscoveryClient.createServiceNode
import org.apache.kyuubi.service.{AbstractService, FrontendService}

/**
 * A abstract service for service discovery
 *
 * @param name   the name of the service itself
 * @param fe the frontend service to publish for service discovery
 */
abstract class ServiceDiscovery(
    name: String,
    val fe: FrontendService) extends AbstractService(name) {

  private var _discoveryClient: ServiceDiscoveryClient = _

  def discoveryClient: ServiceDiscoveryClient = _discoveryClient

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf

    _discoveryClient = new ServiceDiscoveryClient(this)
    discoveryClient.createClient(conf)

    super.initialize(conf)
  }

  override def start(): Unit = {
    discoveryClient.registerService(conf)
    super.start()
  }

  override def stop(): Unit = {
    discoveryClient.closeClient()
    super.stop()
  }

  // stop the server genteelly
  def stopGracefully(): Unit = {
    stop()
    while (fe.be != null && fe.be.sessionManager.getOpenSessionCount > 0) {
      Thread.sleep(1000 * 60)
    }
    fe.serverable.stop()
  }

}

object ServiceDiscovery extends Logging {

  def supportServiceDiscovery(conf: KyuubiConf): Boolean = {
    val zkEnsemble = conf.get(HA_ZK_QUORUM)
    zkEnsemble != null && zkEnsemble.nonEmpty
  }

  def getServerHost(zkClient: CuratorFramework, namespace: String): Option[(String, Int)] = {
    // TODO: use last one because to avoid touching some maybe-crashed engines
    // We need a big improvement here.
    getServiceNodesInfo(zkClient, namespace, Some(1), silent = true) match {
      case Seq(sn) => Some((sn.host, sn.port))
      case _ => None
    }
  }

  def getEngineByRefId(
      zkClient: CuratorFramework,
      namespace: String,
      engineRefId: String): Option[(String, Int)] = {
    getServiceNodesInfo(zkClient, namespace, silent = true)
      .find(_.engineRefId.exists(_.equals(engineRefId)))
      .map(data => (data.host, data.port))
  }

  def getServiceNodesInfo(
      zkClient: CuratorFramework,
      namespace: String,
      sizeOpt: Option[Int] = None,
      silent: Boolean = false): Seq[ServiceNodeInfo] = {
    try {
      val hosts = zkClient.getChildren.forPath(namespace)
      val size = sizeOpt.getOrElse(hosts.size())
      hosts.asScala.takeRight(size).map { p =>
        val path = ZKPaths.makePath(namespace, p)
        val instance = new String(zkClient.getData.forPath(path), StandardCharsets.UTF_8)
        val (host, port) = parseInstanceHostPort(instance)
        val version = p.split(";").find(_.startsWith("version=")).map(_.stripPrefix("version="))
        val engineRefId = p.split(";").find(_.startsWith("refId=")).map(_.stripPrefix("refId="))
        info(s"Get service instance:$instance and version:$version under $namespace")
        ServiceNodeInfo(namespace, p, host, port, version, engineRefId)
      }
    } catch {
      case _: Exception if silent => Nil
      case e: Exception =>
        error(s"Failed to get service node info", e)
        Nil
    }
  }

  @VisibleForTesting
  private[client] def parseInstanceHostPort(instance: String): (String, Int) = {
    val maybeInfos = instance.split(";")
      .map(_.split("=", 2))
      .filter(_.size == 2)
      .map(i => (i(0), i(1)))
      .toMap
    if (maybeInfos.size > 0) {
      (
        maybeInfos.get("hive.server2.thrift.bind.host").get,
        maybeInfos.get("hive.server2.thrift.port").get.toInt)
    } else {
      val strings = instance.split(":")
      (strings(0), strings(1).toInt)
    }
  }

  def createAndGetServiceNode(
      conf: KyuubiConf,
      zkClient: CuratorFramework,
      namespace: String,
      instance: String,
      version: Option[String] = None,
      external: Boolean = false): String = {
    createServiceNode(conf, zkClient, namespace, instance, version, external).getActualPath
  }
}
