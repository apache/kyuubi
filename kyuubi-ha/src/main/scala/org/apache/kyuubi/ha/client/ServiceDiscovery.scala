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
import javax.security.auth.login.Configuration

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hadoop.security.{SecurityUtil, UserGroupInformation}
import org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.JaasConfiguration
import org.apache.zookeeper.{KeeperException, WatchedEvent, Watcher}
import org.apache.zookeeper.CreateMode.PERSISTENT
import org.apache.zookeeper.KeeperException.NodeExistsException

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiException}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.AbstractService

/**
 * A service for service discovery
 *
 * @param name the name of the service itself
 * @param instance the instance uri a service that used to publish itself
 * @param namespace a pre-defined namespace used to publish the instance of the associate service
 */
class ServiceDiscovery private (
    name: String,
    instance: String,
    namespace: String,
    postHook: Thread) extends AbstractService(name) {

  postHook.setDaemon(true)

  def this(instance: String, namespace: String, postHook: Thread) =
    this(classOf[ServiceDiscovery].getSimpleName, instance, namespace, postHook)

  private var zkClient: CuratorFramework = _
  private var serviceNode: PersistentEphemeralNode = _

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
    ServiceDiscovery.setUpZooKeeperAuth(conf)
    zkClient = ServiceDiscovery.newZookeeperClient(conf)

    try {
      zkClient
        .create()
        .creatingParentsIfNeeded()
        .withMode(PERSISTENT)
        .forPath(s"/$namespace")
    } catch {
      case _: NodeExistsException =>  // do nothing
      case e: KeeperException => throw e
    }
    super.initialize(conf)
  }


  override def start(): Unit = {
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
        throw new KyuubiException("Unable to create znode for this Kyuubi instance on ZooKeeper.")
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

  class DeRegisterWatcher extends Watcher {
    override def process(event: WatchedEvent): Unit = {
      if (event.getType == Watcher.Event.EventType.NodeDeleted) {
        if (serviceNode != null) {
          try {
            serviceNode.close()
            warn(s"This Kyuubi instance ${instance} is now de-registered from ZooKeeper. " +
              "The server will be shut down after the last client session completes.")
          } catch {
            case e: IOException =>
              error(s"Failed to close the persistent ephemeral znode: ${serviceNode.getActualPath}",
                e)
          } finally {
            postHook.start()
            ServiceDiscovery.this.stop()
          }
        }
      }
    }
  }

}

object ServiceDiscovery {
  import org.apache.kyuubi.config.KyuubiConf._

  private final val DEFAULT_ACL_PROVIDER = ZooKeeperACLProvider()
  /**
   * Create a [[CuratorFramework]] instance to be used as the ZooKeeper client
   * Use the [[ZooKeeperACLProvider]] to create appropriate ACLs
   */
  def newZookeeperClient(conf: KyuubiConf): CuratorFramework = {
    val connectionStr = conf.get(HA_ZK_QUORUM)
    val sessionTimeout = conf.get(HA_ZK_SESSION_TIMEOUT)
    val connectionTimeout = conf.get(HA_ZK_CONNECTION_TIMEOUT)
    val baseSleepTime = conf.get(HA_ZK_CONNECTION_RETRY_WAIT)
    val maxRetries = conf.get(HA_ZK_CONNECTION_MAX_RETRIES)
    val retryPolicy = new ExponentialBackoffRetry(baseSleepTime, maxRetries)
    val client = CuratorFrameworkFactory.builder()
      .connectString(connectionStr)
      .sessionTimeoutMs(sessionTimeout)
      .connectionTimeoutMs(connectionTimeout)
      .aclProvider(DEFAULT_ACL_PROVIDER)
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
    val keyTabFile = conf.get(SERVER_KEYTAB)
    val maybePrincipal = conf.get(SERVER_PRINCIPAL)
    val kerberized = maybePrincipal.isDefined && keyTabFile.isDefined
    if (UserGroupInformation.isSecurityEnabled && kerberized) {
      if (!new File(keyTabFile.get).exists()) {
        throw new IOException(s"${SERVER_KEYTAB.key} does not exists")
      }
      System.setProperty("zookeeper.sasl.clientconfig", "KyuubiZooKeeperClient")
      var principal = maybePrincipal.get
      principal = SecurityUtil.getServerPrincipal(principal, "0.0.0.0")
      val jaasConf = new JaasConfiguration("KyuubiZooKeeperClient", principal, keyTabFile.get)
      Configuration.setConfiguration(jaasConf)
    }
  }
}
