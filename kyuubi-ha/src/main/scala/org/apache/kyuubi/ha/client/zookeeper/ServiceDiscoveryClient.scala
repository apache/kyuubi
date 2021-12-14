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
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.nodes.PersistentNode
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.state.ConnectionState.CONNECTED
import org.apache.curator.framework.state.ConnectionState.LOST
import org.apache.curator.framework.state.ConnectionState.RECONNECTED
import org.apache.curator.framework.state.ConnectionStateListener
import org.apache.curator.utils.ZKPaths
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.CreateMode.PERSISTENT
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher

import org.apache.kyuubi.KYUUBI_VERSION
import org.apache.kyuubi.KyuubiException
import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_CONN_MAX_RETRIES
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_CONN_MAX_RETRY_WAIT
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_ENGINE_REF_ID
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_NAMESPACE
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_NODE_TIMEOUT
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_PUBLISH_CONFIGS
import org.apache.kyuubi.ha.client.ServiceDiscovery
import org.apache.kyuubi.ha.client.ZooKeeperClientProvider.buildZookeeperClient
import org.apache.kyuubi.ha.client.zookeeper.ServiceDiscoveryClient.connectionChecker
import org.apache.kyuubi.ha.client.zookeeper.ServiceDiscoveryClient.createServiceNode
import org.apache.kyuubi.util.KyuubiHadoopUtils
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

  /**
   * Close the serviceNode if not closed yet
   * and the znode will be deleted upon the serviceNode closed.
   */
  def deregisterService(): Unit = {
    // close the EPHEMERAL_SEQUENTIAL node in zk
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

  def postDeregisterService(): Boolean = {
    if (namespace != null) {
      zkClient.delete().deletingChildrenIfNeeded().forPath(namespace)
      true
    } else {
      false
    }
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

object ServiceDiscoveryClient extends Logging {
  final private lazy val connectionChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("zk-connection-checker")

  private[client] def createServiceNode(
      conf: KyuubiConf,
      zkClient: CuratorFramework,
      namespace: String,
      instance: String,
      version: Option[String] = None,
      external: Boolean = false): PersistentNode = {
    val ns = ZKPaths.makePath(null, namespace)
    try {
      zkClient
        .create()
        .creatingParentsIfNeeded()
        .withMode(PERSISTENT)
        .forPath(ns)
    } catch {
      case _: NodeExistsException => // do nothing
      case e: KeeperException =>
        throw new KyuubiException(s"Failed to create namespace '$ns'", e)
    }

    val session = conf.get(HA_ZK_ENGINE_REF_ID)
      .map(refId => s"refId=$refId;").getOrElse("")
    val pathPrefix = ZKPaths.makePath(
      namespace,
      s"serviceUri=$instance;version=${version.getOrElse(KYUUBI_VERSION)};${session}sequence=")
    var serviceNode: PersistentNode = null
    val createMode =
      if (external) CreateMode.PERSISTENT_SEQUENTIAL
      else CreateMode.EPHEMERAL_SEQUENTIAL
    val znodeData =
      if (conf.get(HA_ZK_PUBLISH_CONFIGS) && session.isEmpty) {
        addConfsToPublish(conf, instance)
      } else {
        instance
      }
    try {
      serviceNode = new PersistentNode(
        zkClient,
        createMode,
        false,
        pathPrefix,
        znodeData.getBytes(StandardCharsets.UTF_8))
      serviceNode.start()
      val znodeTimeout = conf.get(HA_ZK_NODE_TIMEOUT)
      if (!serviceNode.waitForInitialCreate(znodeTimeout, TimeUnit.MILLISECONDS)) {
        throw new KyuubiException(s"Max znode creation wait time $znodeTimeout s exhausted")
      }
      info(s"Created a ${serviceNode.getActualPath} on ZooKeeper for KyuubiServer uri: " + instance)
    } catch {
      case e: Exception =>
        if (serviceNode != null) {
          serviceNode.close()
        }
        throw new KyuubiException(
          s"Unable to create a znode for this server instance: $instance",
          e)
    }
    serviceNode
  }

  /**
   * Refer to the implementation of HIVE-11581 to simplify user connection parameters.
   * https://issues.apache.org/jira/browse/HIVE-11581
   * HiveServer2 should store connection params in ZK
   * when using dynamic service discovery for simpler client connection string.
   */
  private[client] def addConfsToPublish(conf: KyuubiConf, instance: String): String = {
    if (!instance.contains(":")) {
      return instance
    }
    val hostPort = instance.split(":", 2)
    val confsToPublish = collection.mutable.Map[String, String]()

    // Hostname
    confsToPublish += ("hive.server2.thrift.bind.host" -> hostPort(0))
    // Transport mode
    confsToPublish += ("hive.server2.transport.mode" -> "binary")
    // Transport specific confs
    confsToPublish += ("hive.server2.thrift.port" -> hostPort(1))
    confsToPublish += ("hive.server2.thrift.sasl.qop" -> conf.get(KyuubiConf.SASL_QOP))
    // Auth specific confs
    val authenticationMethod = conf.get(KyuubiConf.AUTHENTICATION_METHOD).mkString(",")
    confsToPublish += ("hive.server2.authentication" -> authenticationMethod)
    if (authenticationMethod.equalsIgnoreCase("KERBEROS")) {
      confsToPublish += ("hive.server2.authentication.kerberos.principal" ->
        conf.get(KyuubiConf.SERVER_PRINCIPAL).map(KyuubiHadoopUtils.getServerPrincipal)
          .getOrElse(""))
    }
    confsToPublish.map { case (k, v) => k + "=" + v }.mkString(";")
  }
}
