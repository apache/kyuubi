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

import scala.util.Random
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode.PERSISTENT

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf.ENGINE_SHARE_LEVEL
import org.apache.kyuubi.engine.ProvidePolicy.{ProvidePolicy, RANDOM}
import org.apache.kyuubi.ha.client.ServiceDiscovery.getServiceNodesInfo
import org.apache.kyuubi.service.Serverable

/**
 * A service for service discovery used by engine side.
 *
 * @param name the name of the service itself
 * @param server the instance uri a service that used to publish itself
 */
class EngineServiceDiscovery private(
    name: String,
    server: Serverable) extends ServiceDiscovery(name, server) {
  def this(server: Serverable) =
    this(classOf[EngineServiceDiscovery].getSimpleName, server)

  override def stop(): Unit = synchronized {
    closeServiceNode()
    conf.get(ENGINE_SHARE_LEVEL) match {
      // For connection level, we should clean up the namespace in zk in case the disk stress.
      case "CONNECTION" if namespace != null =>
        try {
          zkClient.delete().deletingChildrenIfNeeded().forPath(namespace)
          info("Clean up discovery service due to this is connection share level.")
        } catch {
          case NonFatal(e) =>
            warn("Failed to clean up Spark engine before stop.", e)
        }

      case _ =>
    }
    super.stop()
  }
}

object EngineServiceDiscovery extends Logging {

  val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def getEngineByPolicy(
      zkClient: CuratorFramework,
      namespace: String,
      providePolicy: ProvidePolicy): Option[(String, Int)] = {
    providePolicy match {
      case RANDOM =>
        Random.shuffle(getServiceNodesInfo(zkClient, namespace, silent = true))
          .headOption.map(node => (node.host, node.port))
      case _ => throw new IllegalArgumentException(s"Not support provide policy $providePolicy")
    }
  }

  def getEngineBySessionId(
      zkClient: CuratorFramework,
      namespace: String,
      sessionId: String): Option[(String, Int)] = {
    getServiceNodesInfo(zkClient, namespace, silent = true)
      .find(_.createSessionId.exists(_.equals(sessionId)))
      .map(data => (data.host, data.port))
  }

  def createEngineSpaceIfNotExists(
      zkClient: CuratorFramework,
      engineSpace: String,
      poolSize: Int): Unit = {

    val data = mapper.writeValueAsBytes(EngineSpaceData(poolSize))
    if (zkClient.checkExists().forPath(engineSpace) == null) {
      zkClient
        .create()
        .creatingParentsIfNeeded()
        .withMode(PERSISTENT)
        .forPath(engineSpace, data)
    }
  }

  def checkEnginePoolCapacity(
      zkClient: CuratorFramework,
      engineSpace: String): Boolean = {

    val data = zkClient.getData.forPath(engineSpace)
    val poolSize = mapper.readValue(data, classOf[EngineSpaceData]).poolSize
    val engineNum = zkClient.getChildren.forPath(engineSpace).size()

    engineNum < poolSize
  }
}

case class EngineSpaceData(poolSize: Int)
