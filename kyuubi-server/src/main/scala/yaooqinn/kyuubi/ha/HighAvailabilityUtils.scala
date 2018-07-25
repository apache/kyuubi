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

package yaooqinn.kyuubi.ha

import java.io.{File, IOException}
import java.nio.charset.Charset
import java.util.{ArrayList, HashMap, List}
import java.util.concurrent.TimeUnit
import javax.security.auth.login.{AppConfigurationEntry, Configuration}
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.api.ACLProvider
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hadoop.security.{SecurityUtil, UserGroupInformation}
import org.apache.hadoop.security.authentication.util.KerberosUtil
import org.apache.spark.{KyuubiSparkUtil, SparkConf}
import org.apache.spark.KyuubiConf._
import org.apache.zookeeper._
import org.apache.zookeeper.KeeperException.{ConnectionLossException, NodeExistsException}
import org.apache.zookeeper.data.ACL

import yaooqinn.kyuubi.{Logging, _}
import yaooqinn.kyuubi.server.{FrontendService, KyuubiServer}
import yaooqinn.kyuubi.service.ServiceException

private[kyuubi] object HighAvailabilityUtils extends Logging {

  private[this] val ZK_PATH_SEPARATOR = "/"

  private[this] var zkClient: CuratorFramework = _
  private[this] var znode: PersistentEphemeralNode = _
  private[this] var znodePath: String = _
  // Set to true only when deregistration happens
  private[this] var deregisteredWithZooKeeper = false

  def isSupportDynamicServiceDiscovery(conf: SparkConf): Boolean = {
    conf.getBoolean(HA_ENABLED.key, defaultValue = false) &&
      conf.get(HA_ZOOKEEPER_QUORUM.key, "").nonEmpty
  }

  @throws[Exception]
  def addServerInstanceToZooKeeper(server: KyuubiServer): Unit = {
    val conf = server.getConf
    val zooKeeperEnsemble = getQuorumServers(conf)
    val rootNamespace = conf.get(HA_ZOOKEEPER_NAMESPACE.key)
    val instanceURI = getServerInstanceURI(server.feService)
    setUpZooKeeperAuth(conf)
    val sessionTimeout = conf.getTimeAsMs(HA_ZOOKEEPER_SESSION_TIMEOUT.key).toInt
    val baseSleepTime = conf.getTimeAsMs(HA_ZOOKEEPER_CONNECTION_BASESLEEPTIME.key).toInt
    val maxRetries = conf.get(HA_ZOOKEEPER_CONNECTION_MAX_RETRIES.key).toInt
    val znodeTimeout = conf.getTimeAsSeconds(HA_ZOOKEEPER_ZNODE_CREATION_TIMEOUT.key)

    // Create a CuratorFramework instance to be used as the ZooKeeper client
    // Use the zooKeeperAclProvider to create appropriate ACLs
    zkClient =
      CuratorFrameworkFactory.builder.connectString(zooKeeperEnsemble)
        .sessionTimeoutMs(sessionTimeout)
        .aclProvider(zooKeeperAclProvider)
        .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries))
        .build
    zkClient.start()
    // Create the parent znodes recursively; ignore if the parent already exists.
    try {
      zkClient
        .create
        .creatingParentsIfNeeded
        .withMode(CreateMode.PERSISTENT)
        .forPath(ZK_PATH_SEPARATOR + rootNamespace)
      info("Created the root name space: " + rootNamespace + " on ZooKeeper for KyuubiServer")
    } catch {
      case e: ConnectionLossException =>
        throwServiceEx( s"ZooKeeper is still unreachable after ${sessionTimeout / 1000}s", e)
      case _: NodeExistsException =>
      case e: KeeperException =>
        throwServiceEx( s"Unable to create KyuubiServer namespace $rootNamespace on ZooKeeper", e)
    }
    // Create a znode under the rootNamespace parent for this instance of the server
    // Znode name: serverUri=host:port;version=versionInfo;sequence=sequenceNumber
    try {
      val pathPrefix = ZK_PATH_SEPARATOR + rootNamespace + ZK_PATH_SEPARATOR +
        "serverUri=" + instanceURI + ";" + "version=" + KYUUBI_VERSION + ";" + "sequence="
      val znodeData = instanceURI
      val znodeDataUTF8 = znodeData.getBytes(Charset.forName("UTF-8"))
      znode = new PersistentEphemeralNode(
        zkClient,
        PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL,
        pathPrefix,
        znodeDataUTF8)
      znode.start()
      if (!znode.waitForInitialCreate(znodeTimeout, TimeUnit.SECONDS)) {
        throwServiceEx(s"Max znode creation wait time $znodeTimeout s exhausted")
      }
      setDeregisteredWithZooKeeper(false)
      znodePath = znode.getActualPath
      // Set a watch on the znode
      if (zkClient.checkExists.usingWatcher(new DeRegisterWatcher(server))
        .forPath(znodePath) == null) {
        // No node exists, throw exception
        throwServiceEx("Unable to create znode for this KyuubiServer instance on ZooKeeper.")
      }
      info("Created a znode on ZooKeeper for KyuubiServer uri: " + instanceURI)
    } catch {
      case e: Exception =>
        if (znode != null) znode.close()
        throwServiceEx("Unable to create a znode for this server instance", e)
    }
  }

  private[this] def throwServiceEx(msg: String, e: Exception = null): Unit = {
    error(msg, e)
    throw new ServiceException(msg, e)
  }

  /**
   * Get the ensemble server addresses from the configuration. The format is: host1:port,
   * host2:port...
   *
   * @param conf SparkConf
   */
  private def getQuorumServers(conf: SparkConf): String = {
    val hosts = conf.get(HA_ZOOKEEPER_QUORUM.key, "").split(",")
    val port = conf.get(HA_ZOOKEEPER_CLIENT_PORT.key, "2181")
    val quorum = new StringBuilder
    hosts.foreach { host =>
      quorum.append(host)
      if (!host.contains(":")) {
        quorum.append(":")
        quorum.append(port)
      }
      quorum.append(",")
    }
    quorum.stripSuffix(",")
  }

  private class DeRegisterWatcher(kyuubiServer: KyuubiServer) extends Watcher {
    override def process(event: WatchedEvent): Unit = {
      if (event.getType == Watcher.Event.EventType.NodeDeleted) {
        if (znode != null) {
          try {
            znode.close()
            warn("This Kyuubi instance is now de-registered from ZooKeeper. " +
              "The server will be shut down after the last client session completes.")
          } catch {
            case e: IOException =>
              error("Failed to close the persistent ephemeral znode", e)
          } finally {
            setDeregisteredWithZooKeeper(true)
            // If there are no more active client sessions, stop the server
            if (kyuubiServer.beService.getSessionManager.getOpenSessionCount == 0) {
              warn("This Kyuubi instance  has been removed from the list of " +
                "server instances available for dynamic service discovery. The last client " +
                "session has ended - will shutdown now.")
              kyuubiServer.stop()
            }
          }
        }
      }
    }
  }

  /**
   * For a kerberized cluster, we dynamically set up the client's JAAS conf.
   *
   * @param conf SparkConf
   * @return
   */
  @throws[Exception]
  private[this] def setUpZooKeeperAuth(conf: SparkConf): Unit = {
    if (UserGroupInformation.isSecurityEnabled) {
      var principal = conf.get(KyuubiSparkUtil.PRINCIPAL)
      val keyTabFile = conf.get(KyuubiSparkUtil.KEYTAB)
      if (!new File(keyTabFile).exists()) {
        throw new IOException("key tab does not exists")
      }
      val SASL_LOGIN_CONTEXT_NAME = "KyuubiZooKeeperClient"
      System.setProperty("zookeeper.sasl.clientconfig", SASL_LOGIN_CONTEXT_NAME)
      principal = SecurityUtil.getServerPrincipal(principal, "0.0.0.0")
      val jaasConf = new JaasConfiguration(SASL_LOGIN_CONTEXT_NAME, principal, keyTabFile)
      Configuration.setConfiguration(jaasConf)
    }
  }

  @throws[Exception]
  private def getServerInstanceURI(service: FrontendService): String = {
    if ((service == null) || (service.getServerIPAddress == null)) {
      throw new Exception("Unable to get the server address; it hasn't been initialized yet.")
    }
    service.getServerIPAddress.getHostName + ":" + service.getPortNumber
  }

  /**
   * ACLProvider for providing appropriate ACLs to CuratorFrameworkFactory
   */
  private val zooKeeperAclProvider = new ACLProvider() {
    override def getDefaultAcl: List[ACL] = {
      val nodeAcls = new ArrayList[ACL]
      if (UserGroupInformation.isSecurityEnabled) {
        // Read all to the world
        nodeAcls.addAll(ZooDefs.Ids.READ_ACL_UNSAFE)
        // Create/Delete/Write/Admin to the authenticated user
        nodeAcls.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS))
      } else {
        // ACLs for znodes on a non-kerberized cluster
        // Create/Read/Delete/Write/Admin to the world
        nodeAcls.addAll(ZooDefs.Ids.OPEN_ACL_UNSAFE)
      }
      nodeAcls
    }

    override def getAclForPath(path: String): List[ACL] = getDefaultAcl
  }

  private[this] def setDeregisteredWithZooKeeper(deregisteredWithZooKeeper: Boolean): Unit = {
    this.deregisteredWithZooKeeper = deregisteredWithZooKeeper
  }

  private[this] class JaasConfiguration(
      loginContextName: String, principal: String, keyTabFile: String) extends Configuration {

    final private val baseConfig: Configuration = Configuration.getConfiguration

    override def getAppConfigurationEntry(appName: String): Array[AppConfigurationEntry] = {
      if (this.loginContextName == appName) {
        val krbOptions = new HashMap[String, String]
        krbOptions.put("doNotPrompt", "true")
        krbOptions.put("storeKey", "true")
        krbOptions.put("useKeyTab", "true")
        krbOptions.put("principal", this.principal)
        krbOptions.put("keyTab", this.keyTabFile)
        krbOptions.put("refreshKrb5Config", "true")
        val kyuubiZooKeeperClientEntry: AppConfigurationEntry =
          new AppConfigurationEntry(
            KerberosUtil.getKrb5LoginModuleName,
            LoginModuleControlFlag.REQUIRED,
            krbOptions)

        Array[AppConfigurationEntry](kyuubiZooKeeperClientEntry)
      } else if (this.baseConfig != null) {
        this.baseConfig.getAppConfigurationEntry(appName)
      } else {
        null
      }
    }
  }
}


