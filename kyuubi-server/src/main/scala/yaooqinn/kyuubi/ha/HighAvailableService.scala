/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.ha

import java.io.{File, IOException}
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit
import javax.security.auth.login.Configuration

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.api.ACLProvider
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hadoop.security.{SecurityUtil, UserGroupInformation}
import org.apache.kyuubi.Logging
import org.apache.spark.{KyuubiSparkUtil, SparkConf}
import org.apache.spark.KyuubiConf._
import org.apache.zookeeper.{KeeperException, WatchedEvent, Watcher}
import org.apache.zookeeper.CreateMode.PERSISTENT
import org.apache.zookeeper.KeeperException.NodeExistsException

import yaooqinn.kyuubi.KYUUBI_VERSION
import yaooqinn.kyuubi.server.{FrontendService, KyuubiServer}
import yaooqinn.kyuubi.service.{AbstractService, ServiceException}

/**
 * An abstract class provides [[KyuubiServer]] with high availability functions using Zookeeper
 * dynamic service discovery.
 *
 */
private[kyuubi] abstract class HighAvailableService(name: String, server: KyuubiServer)
  extends AbstractService(name) with Logging {

  import HighAvailableService._

  protected final var zkClient: CuratorFramework = _
  protected final var serviceRootNamespace: String = _

  private var serviceNode: PersistentEphemeralNode = _
  private var servicePath: String = _

  /**
   * reset current service
   * Visible for testing
   */
  private[ha] def reset(): Unit

  /**
   * Expose Kyuubi service instance uri in [[HA_ZOOKEEPER_NAMESPACE]]
   */
  protected final def publishService(): Unit = {
    try {
      val instanceURI = getServerInstanceURI(server.feService)
      val pathPrefix = serviceRootNamespace + "/" +
        "serverUri=" + instanceURI + ";" +
        "version=" + KYUUBI_VERSION + ";" +
        "sequence="
      val znodeData = instanceURI.getBytes(Charset.forName("UTF-8"))
      serviceNode = new PersistentEphemeralNode(
        zkClient,
        PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL,
        pathPrefix,
        znodeData)
      serviceNode.start()
      val znodeTimeout = 120
      if (!serviceNode.waitForInitialCreate(znodeTimeout, TimeUnit.SECONDS)) {
        throw new ServiceException(s"Max znode creation wait time $znodeTimeout s exhausted")
      }
      servicePath = serviceNode.getActualPath
      // Set a watch on the serviceNode
      val watcher = new DeRegisterWatcher
      if (zkClient.checkExists.usingWatcher(watcher).forPath(servicePath) == null) {
        // No node exists, throw exception
        throw new ServiceException("Unable to create znode for this Kyuubi instance on ZooKeeper.")
      }
      info("Created a serviceNode on ZooKeeper for KyuubiServer uri: " + instanceURI)
    } catch {
      case e: Exception =>
        if (serviceNode != null) serviceNode.close()
        throw new ServiceException("Unable to create a znode for this server instance", e)
    }
  }

  /**
   * Offline Kyuubi service instance uri in [[HA_ZOOKEEPER_NAMESPACE]]
   */
  protected final def offlineService(): Unit = {
    if (serviceNode != null) {
      try {
        serviceNode.close()
      } catch {
        case e: IOException =>
          error("Failed to close the persistent ephemeral znode" + servicePath, e)
      }
    }
    info("Kyuubi instance has been removed from ZooKeeper")
  }

  final override def init(conf: SparkConf): Unit = {
    // service root namespace
    serviceRootNamespace = "/" + conf.get(HA_ZOOKEEPER_NAMESPACE.key)
    setUpZooKeeperAuth(conf)
    zkClient = newZookeeperClient(conf)
    // Create the parent znodes recursively; ignore if the parent already exists.
    try {
      zkClient
        .create()
        .creatingParentsIfNeeded()
        .withMode(PERSISTENT)
        .forPath(serviceRootNamespace)
    } catch {
      case _: NodeExistsException =>  // do nothing
      case e: KeeperException =>
        throw new ServiceException(s"Unable to create Kyuubi namespace $serviceRootNamespace" +
          s" on ZooKeeper", e)
    }
    super.init(conf)
  }

  class DeRegisterWatcher extends Watcher {
    override def process(event: WatchedEvent): Unit = {
      if (event.getType == Watcher.Event.EventType.NodeDeleted) {
        if (serviceNode != null) {
          try {
            serviceNode.close()
            warn("This Kyuubi instance is now de-registered from ZooKeeper. " +
              "The server will be shut down after the last client session completes.")
          } catch {
            case e: IOException =>
              error("Failed to close the persistent ephemeral znode", e)
          } finally {
            reset()
          }
        }
      }
    }
  }

}

object HighAvailableService {

  /**
   * ACLProvider for providing appropriate ACLs to CuratorFrameworkFactory
   */
  val aclProvider: ACLProvider = new ZooKeeperACLProvider

  /**
   * Get the ensemble server addresses from the configuration. The format is: host1:port,
   * host2:port...
   *
   * @param conf SparkConf
   */
  def getQuorumServers(conf: SparkConf): String = {
    val port = conf.get(HA_ZOOKEEPER_CLIENT_PORT.key, "2181")

    conf.getOption(HA_ZOOKEEPER_QUORUM.key) match {
      case Some(h) if h.nonEmpty =>
        h.split(",").map {
          host => if (!host.contains(":")) host + ":" + port else host
        }.mkString(",")
      case _ =>
        throw new IllegalArgumentException(
          s"${HA_ZOOKEEPER_QUORUM.key} should be configured if ha enabled")
    }
  }

  /**
   * Create a [[CuratorFramework]] instance to be used as the ZooKeeper client
   * Use the [[ZooKeeperACLProvider]] to create appropriate ACLs
   *
   * @param conf a [[SparkConf]] instance
   *
   * @return a [[CuratorFramework]] instance
   */
  def newZookeeperClient(conf: SparkConf): CuratorFramework = {
    val baseSleepTime = conf.getTimeAsMs(HA_ZOOKEEPER_CONNECTION_BASESLEEPTIME.key).toInt
    val maxRetries = conf.get(HA_ZOOKEEPER_CONNECTION_MAX_RETRIES.key).toInt
    val client = CuratorFrameworkFactory.builder()
      .connectString(getQuorumServers(conf))
      .sessionTimeoutMs(conf.getTimeAsMs(HA_ZOOKEEPER_SESSION_TIMEOUT.key).toInt)
      .aclProvider(aclProvider)
      .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries))
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
  def setUpZooKeeperAuth(conf: SparkConf): Unit = {
    if (UserGroupInformation.isSecurityEnabled) {
      var principal = conf.get(KyuubiSparkUtil.PRINCIPAL, "")
      val keyTabFile = conf.get(KyuubiSparkUtil.KEYTAB, "")
      if (!new File(keyTabFile).exists()) {
        throw new IOException(s"${KyuubiSparkUtil.KEYTAB} does not exists")
      }
      val SASL_LOGIN_CONTEXT_NAME = "KyuubiZooKeeperClient"
      System.setProperty("zookeeper.sasl.clientconfig", SASL_LOGIN_CONTEXT_NAME)
      principal = SecurityUtil.getServerPrincipal(principal, "0.0.0.0")
      val jaasConf = new JaasConfiguration(SASL_LOGIN_CONTEXT_NAME, principal, keyTabFile)
      Configuration.setConfiguration(jaasConf)
    }
  }

  def getServerInstanceURI(service: FrontendService): String = {
    if (service == null || service.getServerIPAddress == null) {
      throw new ServiceException("Unable to address the server; it hasn't been initialized yet.")
    }
    service.getServerIPAddress.getHostName + ":" + service.getPortNumber
  }
}
