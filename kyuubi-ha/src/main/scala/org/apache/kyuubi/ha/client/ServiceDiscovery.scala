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

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import javax.security.auth.login.Configuration

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode
import org.apache.curator.framework.state.{ConnectionState, ConnectionStateListener}
import org.apache.curator.framework.state.ConnectionState.{CONNECTED, LOST, RECONNECTED}
import org.apache.curator.retry._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.JaasConfiguration
import org.apache.zookeeper.{KeeperException, WatchedEvent, Watcher}
import org.apache.zookeeper.CreateMode.PERSISTENT
import org.apache.zookeeper.KeeperException.NodeExistsException

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiException}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.ServiceDiscovery._
import org.apache.kyuubi.service.{AbstractService, Serverable}
import org.apache.kyuubi.util.{KyuubiHadoopUtils, ThreadUtils}

/**
 * A service for service discovery
 *
 * @param name the name of the service itself
 * @param server the instance uri a service that used to publish itself
 * @param namespace a pre-defined namespace used to publish the instance of the associate service
 */
class ServiceDiscovery private (
    name: String,
    server: Serverable,
    namespace: String) extends AbstractService(name) {

  def this(server: Serverable, namespace: String) =
    this(classOf[ServiceDiscovery].getSimpleName, server, namespace)

  private var zkClient: CuratorFramework = _
  private var serviceNode: PersistentEphemeralNode = _

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
    val maxSleepTime = conf.get(HA_ZK_CONN_MAX_RETRY_WAIT)
    val maxRetries = conf.get(HA_ZK_CONN_MAX_RETRIES)
    setUpZooKeeperAuth(conf)
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
            connectionChecker.schedule(new Runnable {
              override def run(): Unit = if (!isConnected.get()) {
                error(s"Zookeeper client connection state changed to: $newState, but failed to" +
                  s" reconnect in ${delay / 1000} seconds. Give up retry. ")
                client.close()
                stopGracefully()
              }
            }, delay, TimeUnit.MILLISECONDS)
          case _ =>
        }
      }
    })
    zkClient.start()

    try {
      zkClient
        .create()
        .creatingParentsIfNeeded()
        .withMode(PERSISTENT)
        .forPath(s"/$namespace")
    } catch {
      case _: NodeExistsException =>  // do nothing
      case e: KeeperException =>
        throw new KyuubiException(s"Failed to create namespace '/$namespace'", e)
    }
    super.initialize(conf)
  }


  override def start(): Unit = {
    val instance = server.connectionUrl
    val pathPrefix = s"/$namespace/serviceUri=$instance;version=$KYUUBI_VERSION;sequence="
    try {
      serviceNode = new PersistentEphemeralNode(
        zkClient,
        PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL,
        pathPrefix,
        instance.getBytes(StandardCharsets.UTF_8))
      serviceNode.start()
      val znodeTimeout = 120
      if (!serviceNode.waitForInitialCreate(znodeTimeout, TimeUnit.SECONDS)) {
        throw new KyuubiException(s"Max znode creation wait time $znodeTimeout s exhausted")
      }
      // Set a watch on the serviceNode
      val watcher = new DeRegisterWatcher
      if (zkClient.checkExists.usingWatcher(watcher).forPath(serviceNode.getActualPath) == null) {
        // No node exists, throw exception
        throw new KyuubiException(s"Unable to create znode for this Kyuubi instance[$instance]" +
          s" on ZooKeeper.")
      }
      info("Created a serviceNode on ZooKeeper for KyuubiServer uri: " + instance)
    } catch {
      case e: Exception =>
        if (serviceNode != null) {
          serviceNode.close()
        }
        throw new KyuubiException(
          s"Unable to create a znode for this server instance: $instance", e)
    }
    super.start()
  }

  override def stop(): Unit = {
    if (serviceNode != null) {
      try {
        serviceNode.close()
      } catch {
        case e: IOException =>
          error("Failed to close the persistent ephemeral znode" + serviceNode.getActualPath, e)
      }
    }

    if (zkClient != null) zkClient.close()
    super.stop()
  }

  private def stopGracefully(): Unit = {
    stop()
    while (server.backendService != null &&
      server.backendService.sessionManager.getOpenSessionCount > 0) {
      Thread.sleep(1000 * 60)
    }
    server.stop()
  }


  class DeRegisterWatcher extends Watcher {
    override def process(event: WatchedEvent): Unit = {
      if (event.getType == Watcher.Event.EventType.NodeDeleted) {
        warn(s"This Kyuubi instance ${server.connectionUrl} is now de-registered from" +
          s" ZooKeeper. The server will be shut down after the last client session completes.")
        ServiceDiscovery.this.stopGracefully()
      }
    }
  }
}

object ServiceDiscovery {
  import RetryPolicies._

  private final lazy val connectionChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("zk-connection-checker")

  def buildZookeeperClient(conf: KyuubiConf): CuratorFramework = {
    val connectionStr = conf.get(HA_ZK_QUORUM)
    val sessionTimeout = conf.get(HA_ZK_SESSION_TIMEOUT)
    val connectionTimeout = conf.get(HA_ZK_CONN_TIMEOUT)
    val baseSleepTime = conf.get(HA_ZK_CONN_BASE_RETRY_WAIT)
    val maxSleepTime = conf.get(HA_ZK_CONN_MAX_RETRY_WAIT)
    val maxRetries = conf.get(HA_ZK_CONN_MAX_RETRIES)
    val retryPolicyName = conf.get(HA_ZK_CONN_RETRY_POLICY)

    val retryPolicy = RetryPolicies.withName(retryPolicyName) match {
      case ONE_TIME => new RetryOneTime(baseSleepTime)
      case N_TIME => new RetryNTimes(maxRetries, baseSleepTime)
      case BONDED_EXPONENTIAL_BACKOFF =>
        new BoundedExponentialBackoffRetry(baseSleepTime, maxSleepTime, maxRetries)
      case UNTIL_ELAPSED => new RetryUntilElapsed(maxSleepTime, baseSleepTime)
      case _ => new ExponentialBackoffRetry(baseSleepTime, maxRetries)
    }

    CuratorFrameworkFactory.builder()
      .connectString(connectionStr)
      .sessionTimeoutMs(sessionTimeout)
      .connectionTimeoutMs(connectionTimeout)
      .aclProvider(new ZooKeeperACLProvider(conf))
      .retryPolicy(retryPolicy)
      .build()
  }

  /**
   * Create a [[CuratorFramework]] instance to be used as the ZooKeeper client
   * Use the [[ZooKeeperACLProvider]] to create appropriate ACLs
   */
  def startZookeeperClient(conf: KyuubiConf): CuratorFramework = {
    val client = buildZookeeperClient(conf)
    client.start()
    client
  }

  /**
   * For a kerberized cluster, we dynamically set up the client's JAAS conf.
   *
   * @param conf SparkConf
   * @return
   */
  @throws[Exception]
  def setUpZooKeeperAuth(conf: KyuubiConf): Unit = {
    if (conf.get(HA_ZK_ACL_ENABLED)) {
      val keyTabFile = conf.get(KyuubiConf.SERVER_KEYTAB)
      val maybePrincipal = conf.get(KyuubiConf.SERVER_PRINCIPAL)
      val kerberized = maybePrincipal.isDefined && keyTabFile.isDefined
      if (UserGroupInformation.isSecurityEnabled && kerberized) {
        if (!new File(keyTabFile.get).exists()) {
          throw new IOException(s"${KyuubiConf.SERVER_KEYTAB.key} does not exists")
        }
        System.setProperty("zookeeper.sasl.clientconfig", "KyuubiZooKeeperClient")
        var principal = maybePrincipal.get
        principal = KyuubiHadoopUtils.getServerPrincipal(principal)
        val jaasConf = new JaasConfiguration("KyuubiZooKeeperClient", principal, keyTabFile.get)
        Configuration.setConfiguration(jaasConf)
      }
    }
  }
}
