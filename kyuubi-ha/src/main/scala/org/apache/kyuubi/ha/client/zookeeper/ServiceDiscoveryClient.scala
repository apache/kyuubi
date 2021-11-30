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

package org.apache.kyuubi.ha.client.zookeeper

import java.io.IOException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.nodes.PersistentNode
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.state.ConnectionState.CONNECTED
import org.apache.curator.framework.state.ConnectionState.LOST
import org.apache.curator.framework.state.ConnectionState.RECONNECTED
import org.apache.curator.framework.state.ConnectionStateListener
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher

import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_CONN_MAX_RETRIES
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_CONN_MAX_RETRY_WAIT
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_NAMESPACE
import org.apache.kyuubi.ha.client.ServiceDiscovery
import org.apache.kyuubi.ha.client.ServiceDiscovery.createServiceNode
import org.apache.kyuubi.ha.client.ZooKeeperClientProvider.buildZookeeperClient
import org.apache.kyuubi.ha.client.zookeeper.ServiceDiscoveryClient.connectionChecker
import org.apache.kyuubi.util.ThreadUtils

class ServiceDiscoveryClient(serviceDiscovery: ServiceDiscovery) extends Logging {

  /**
   * a pre-defined namespace used to publish the instance of the associate service
   */
  protected var _namespace: String = _

  private lazy val instance: String = serviceDiscovery.fe.connectionUrl
  private var zkClient: CuratorFramework = _
  private var serviceNode: PersistentNode = _

  def namespace: String = _namespace

  def createClient(conf: KyuubiConf): Unit = {
    _namespace = conf.get(HA_ZK_NAMESPACE)
    val maxSleepTime = conf.get(HA_ZK_CONN_MAX_RETRY_WAIT)
    val maxRetries = conf.get(HA_ZK_CONN_MAX_RETRIES)
    zkClient = buildZookeeperClient(conf)
    zkClient.getConnectionStateListenable.addListener(new ConnectionStateListener {
      private val isConnected = new AtomicBoolean(false)

      override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
        info(s"Zookeeper client connection state changed to: $newState")
        newState match {
          case CONNECTED | RECONNECTED => isConnected.set(true)
          case LOST =>
            isConnected.set(false)
            val delay = maxRetries.toLong * maxSleepTime
            connectionChecker.schedule(
              new Runnable {
                override def run(): Unit = if (!isConnected.get()) {
                  error(s"Zookeeper client connection state changed to: $newState, but failed to" +
                    s" reconnect in ${delay / 1000} seconds. Give up retry. ")
                  serviceDiscovery.stopGracefully()
                }
              },
              delay,
              TimeUnit.MILLISECONDS)
          case _ =>
        }
      }
    })
    zkClient.start()
  }

  def registerService(conf: KyuubiConf): Unit = {
    serviceNode = createServiceNode(conf, zkClient, namespace, instance)
    // Set a watch on the serviceNode
    val watcher = new DeRegisterWatcher
    if (zkClient.checkExists.usingWatcher(watcher).forPath(serviceNode.getActualPath) == null) {
      // No node exists, throw exception
      throw new KyuubiException(s"Unable to create znode for this Kyuubi " +
        s"instance[${instance}] on ZooKeeper.")
    }
  }

  // close the EPHEMERAL_SEQUENTIAL node in zk
  def deregisterService(): Unit = {
    if (serviceNode != null) {
      try {
        serviceNode.close()
      } catch {
        case e: IOException =>
          error("Failed to close the persistent ephemeral znode" + serviceNode.getActualPath, e)
      } finally {
        serviceNode = null
      }
    }
  }

  def postDeregisterService(path: String): Unit = {
    zkClient.delete().deletingChildrenIfNeeded().forPath(path)
  }

  def closeClient(): Unit = {
    deregisterService()
    if (zkClient != null) zkClient.close()
  }

  class DeRegisterWatcher extends Watcher {
    override def process(event: WatchedEvent): Unit = {
      if (event.getType == Watcher.Event.EventType.NodeDeleted) {
        warn(s"This Kyuubi instance ${instance} is now de-registered from" +
          s" ZooKeeper. The server will be shut down after the last client session completes.")
        serviceDiscovery.stopGracefully()
      }
    }
  }
}

object ServiceDiscoveryClient {
  final private lazy val connectionChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("zk-connection-checker")
}
