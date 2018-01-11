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
import org.apache.hive.common.util.HiveVersionInfo
import org.apache.spark.KyuubiConf._
import org.apache.spark.SparkConf
import org.apache.zookeeper._
import org.apache.zookeeper.data.ACL

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.server.{FrontendService, KyuubiServer}

object HighAvailabilityUtils extends Logging {

  val ZOOKEEPER_PATH_SEPARATOR = "/"

  private[this] var zooKeeperClient: CuratorFramework = _
  private[this] var znode: PersistentEphemeralNode = _
  private[this] var znodePath: String = _
  // Set to true only when deregistration happens
  private[this] var deregisteredWithZooKeeper = false

  def isSupportDynamicServiceDiscovery(conf: SparkConf): Boolean = {
    conf.getBoolean(SUPPORT_DYNAMIC_SERVICE_DISCOVERY.key, defaultValue = false) &&
      conf.get(KYUUBI_ZOOKEEPER_QUORUM.key, "").split(",").nonEmpty
  }

  @throws[Exception]
  def addServerInstanceToZooKeeper(kyuubiServer: KyuubiServer): Unit = {
    val conf = kyuubiServer.getConf
    val zooKeeperEnsemble = getQuorumServers(conf)
    val rootNamespace = conf.get(KYUUBI_ZOOKEEPER_NAMESPACE.key, "kyuubiserver")
    val instanceURI = getServerInstanceURI(kyuubiServer.feService)

    setUpZooKeeperAuth(conf)

    val sessionTimeout = conf.getInt(KYUUBI_ZOOKEEPER_SESSION_TIMEOUT.key, 1200000)
    val baseSleepTime = conf.getInt(KYUUBI_ZOOKEEPER_CONNECTION_BASESLEEPTIME.key, 1000)
    val maxRetries = conf.getInt(KYUUBI_ZOOKEEPER_CONNECTION_MAX_RETRIES.key, 3)
    // Create a CuratorFramework instance to be used as the ZooKeeper client
    // Use the zooKeeperAclProvider to create appropriate ACLs
    zooKeeperClient =
      CuratorFrameworkFactory.builder.connectString(zooKeeperEnsemble)
        .sessionTimeoutMs(sessionTimeout)
        .aclProvider(zooKeeperAclProvider)
        .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries))
        .build
    zooKeeperClient.start()
    // Create the parent znodes recursively; ignore if the parent already exists.
    try {
      zooKeeperClient
        .create
        .creatingParentsIfNeeded
        .withMode(CreateMode.PERSISTENT)
        .forPath(ZOOKEEPER_PATH_SEPARATOR + rootNamespace)
      info("Created the root name space: " + rootNamespace + " on ZooKeeper for KyuubiServer")
    } catch {
      case e: KeeperException if e.code ne KeeperException.Code.NODEEXISTS =>
        error("Unable to create KyuubiServer namespace: " + rootNamespace + " on ZooKeeper", e)
        throw e
    }
    // Create a znode under the rootNamespace parent for this instance of the server
    // Znode name: serverUri=host:port;version=versionInfo;sequence=sequenceNumber
    try {
      val pathPrefix = ZOOKEEPER_PATH_SEPARATOR + rootNamespace + ZOOKEEPER_PATH_SEPARATOR +
        "serverUri=" + instanceURI + ";" +
        "version=" + HiveVersionInfo.getVersion + ";" + "sequence="
      var znodeData = ""
      znodeData = instanceURI
      val znodeDataUTF8 = znodeData.getBytes(Charset.forName("UTF-8"))
      znode = new PersistentEphemeralNode(
        zooKeeperClient,
        PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL,
        pathPrefix,
        znodeDataUTF8)
      znode.start()
      // We'll wait for 120s for node creation
      val znodeCreationTimeout = 120
      if (!znode.waitForInitialCreate(znodeCreationTimeout, TimeUnit.SECONDS)) {
        throw new Exception("Max znode creation wait time: " + znodeCreationTimeout + "s exhausted")
      }
      setDeregisteredWithZooKeeper(false)
      znodePath = znode.getActualPath
      // Set a watch on the znode
      if (zooKeeperClient.checkExists.usingWatcher(new DeRegisterWatcher(kyuubiServer))
        .forPath(znodePath) == null) {
        // No node exists, throw exception
        throw new Exception("Unable to create znode for this KyuubiServer instance on ZooKeeper.")
      }
      info("Created a znode on ZooKeeper for KyuubiServer uri: " + instanceURI)
    } catch {
      case e: Exception =>
        error("Unable to create a znode for this server instance", e)
        if (znode != null) znode.close()
        throw e
    }
  }

  /**
   * Get the ensemble server addresses from the configuration. The format is: host1:port,
   * host2:port...
   *
   * @param conf SparkConf
   */
  private def getQuorumServers(conf: SparkConf): String = {
    val hosts = conf.get(KYUUBI_ZOOKEEPER_QUORUM.key, "").split(",")
    val port = conf.get(KYUUBI_ZOOKEEPER_CLIENT_PORT.key, "2181")
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
              "The server will be shut down after the last client sesssion completes.")
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
  private def setUpZooKeeperAuth(conf: SparkConf): Unit = {
    if (UserGroupInformation.isSecurityEnabled) {
      var principal = conf.get("spark.yarn.principal")
      val keyTabFile = conf.get("spark.yarn.keytab")
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

  class JaasConfiguration(
    loginContextName: String,
    principal: String,
    keyTabFile: String) extends Configuration {

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
      }
      else if (this.baseConfig != null) {
        this.baseConfig.getAppConfigurationEntry(appName)
      }
      else {
        null
      }
    }
  }
}


