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

import scala.collection.JavaConverters._

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode
import org.apache.curator.framework.state.{ConnectionState, ConnectionStateListener}
import org.apache.curator.framework.state.ConnectionState.{CONNECTED, LOST, RECONNECTED}
import org.apache.curator.retry._
import org.apache.curator.utils.ZKPaths
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.JaasConfiguration
import org.apache.zookeeper.CreateMode.PERSISTENT
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.NodeExistsException

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiException}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.ServiceDiscovery._
import org.apache.kyuubi.service.{AbstractService, Serverable}
import org.apache.kyuubi.util.{KyuubiHadoopUtils, ThreadUtils}

/**
 * A abstract service for service discovery
 *
 * @param name the name of the service itself
 * @param server the instance uri a service that used to publish itself
 */
abstract class ServiceDiscovery private (
    name: String,
    server: Serverable) extends AbstractService(name) {

  def this(server: Serverable) =
    this(classOf[ServiceDiscovery].getSimpleName, server)

  private var _zkClient: CuratorFramework = _
  private var _serviceNode: PersistentEphemeralNode = _
  /**
   * a pre-defined namespace used to publish the instance of the associate service
   */
  private var _namespace: String = _

  def zkClient: CuratorFramework = _zkClient
  def serviceNode: PersistentEphemeralNode = _serviceNode
  def namespace: String = _namespace

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
    _namespace = conf.get(HA_ZK_NAMESPACE)
    val maxSleepTime = conf.get(HA_ZK_CONN_MAX_RETRY_WAIT)
    val maxRetries = conf.get(HA_ZK_CONN_MAX_RETRIES)
    setUpZooKeeperAuth(conf)
    _zkClient = buildZookeeperClient(conf)
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
                stopGracefully()
              }
            }, delay, TimeUnit.MILLISECONDS)
          case _ =>
        }
      }
    })
    zkClient.start()
    super.initialize(conf)
  }

  override def start(): Unit = {
    val ns = ZKPaths.makePath(null, namespace)
    try {
      zkClient
        .create()
        .creatingParentsIfNeeded()
        .withMode(PERSISTENT)
        .forPath(ns)
    } catch {
      case _: NodeExistsException =>  // do nothing
      case e: KeeperException =>
        throw new KyuubiException(s"Failed to create namespace '$ns'", e)
    }
    val instance = server.connectionUrl
    val pathPrefix = ZKPaths.makePath(
      namespace,
      s"serviceUri=$instance;version=$KYUUBI_VERSION;sequence=")
    try {
      _serviceNode = new PersistentEphemeralNode(
        zkClient,
        PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL,
        pathPrefix,
        instance.getBytes(StandardCharsets.UTF_8))
      serviceNode.start()
      val znodeTimeout = 120
      if (!serviceNode.waitForInitialCreate(znodeTimeout, TimeUnit.SECONDS)) {
        throw new KyuubiException(s"Max znode creation wait time $znodeTimeout s exhausted")
      }
      info(s"Created a ${serviceNode.getActualPath} on ZooKeeper for KyuubiServer uri: " + instance)
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
    closeServiceNode()

    if (zkClient != null) zkClient.close()
    super.stop()
  }

  // close the EPHEMERAL_SEQUENTIAL node in zk
  protected def closeServiceNode(): Unit = {
    if (_serviceNode != null) {
      try {
        _serviceNode.close()
      } catch {
        case e: IOException =>
          error("Failed to close the persistent ephemeral znode" + serviceNode.getActualPath, e)
      } finally {
        _serviceNode = null
      }
    }
  }

  // stop the server genteelly
  def stopGracefully(): Unit = {
    stop()
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
    val connectionStr = conf.get(HA_ZK_QUORUM)
    val sessionTimeout = conf.get(HA_ZK_SESSION_TIMEOUT)
    val connectionTimeout = conf.get(HA_ZK_CONN_TIMEOUT)
    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    val client = CuratorFrameworkFactory.builder()
      .connectString(connectionStr)
      .sessionTimeoutMs(sessionTimeout)
      .connectionTimeoutMs(connectionTimeout)
      .retryPolicy(retryPolicy)
      .build()
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

  def supportServiceDiscovery(conf: KyuubiConf): Boolean = {
    val zkEnsemble = conf.get(HA_ZK_QUORUM)
    zkEnsemble != null && zkEnsemble.nonEmpty
  }

  def getServerHost(zkClient: CuratorFramework, namespace: String): Option[(String, Int)] = {
    try {
      val hosts = zkClient.getChildren.forPath(namespace)
      // TODO: use last one because to avoid touching some maybe-crashed engines
      // We need a big improvement here.
      hosts.asScala.lastOption.map { p =>
        val path = ZKPaths.makePath(namespace, p)
        val hostPort = new String(zkClient.getData.forPath(path), StandardCharsets.UTF_8)
        val strings = hostPort.split(":")
        val host = strings.head
        val port = strings(1).toInt
        (host, port)
      }
    } catch {
      case _: Exception => None
    }
  }
}
