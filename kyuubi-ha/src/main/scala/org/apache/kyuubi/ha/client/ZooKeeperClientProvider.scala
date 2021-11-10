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
import javax.security.auth.login.Configuration

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry._
import org.apache.curator.utils.ZKPaths
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.JaasConfiguration
import org.apache.zookeeper.CreateMode.PERSISTENT
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.NodeExistsException

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.util.KyuubiHadoopUtils

object ZooKeeperClientProvider extends Logging {

  import RetryPolicies._

  /**
   * Create a [[CuratorFramework]] instance to be used as the ZooKeeper client
   * Use the [[ZooKeeperACLProvider]] to create appropriate ACLs
   */
  def buildZookeeperClient(conf: KyuubiConf): CuratorFramework = {
    setUpZooKeeperAuth(conf)
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
      case BOUNDED_EXPONENTIAL_BACKOFF =>
        new BoundedExponentialBackoffRetry(baseSleepTime, maxSleepTime, maxRetries)
      case UNTIL_ELAPSED => new RetryUntilElapsed(maxSleepTime, baseSleepTime)
      case _ => new ExponentialBackoffRetry(baseSleepTime, maxRetries)
    }
    val chrootIndex = connectionStr.indexOf("/")
    val chrootOption = {
      if (chrootIndex > 0) Some(connectionStr.substring(chrootIndex))
      else None
    }
    // make sure zookeeper chroot path exists
    chrootOption.foreach { chroot =>
      val zkConnectionForChrootCreation = connectionStr.substring(0, chrootIndex)
      val clonedConf = KyuubiConf(false)
      clonedConf.set(HA_ZK_QUORUM, zkConnectionForChrootCreation)
      val zkClient = buildZookeeperClient(clonedConf)
      zkClient.start()
      if (zkClient.checkExists().forPath(chroot) == null) {
        val chrootPath = ZKPaths.makePath(null, chroot)
        try {
          zkClient
            .create()
            .creatingParentsIfNeeded()
            .withMode(PERSISTENT)
            .forPath(chrootPath)
        } catch {
          case _: NodeExistsException => // do nothing
          case e: KeeperException =>
            throw new KyuubiException(s"Failed to create chroot path '$chrootPath'", e)
        }
        info(s"Created zookeeper chroot path $chrootPath")
      }
      zkClient.close()
    }
    val builder = CuratorFrameworkFactory.builder()
      .connectString(connectionStr)
      .sessionTimeoutMs(sessionTimeout)
      .connectionTimeoutMs(connectionTimeout)
      .aclProvider(new ZooKeeperACLProvider(conf))
      .retryPolicy(retryPolicy)

    conf.get(HA_ZK_AUTH_DIGEST) match {
      case Some(anthString) =>
        builder.authorization("digest", anthString.getBytes("UTF-8"))
      case _ =>
    }

    builder.build()
  }

  /**
   * Creates a zookeeper client before calling `f` and close it after calling `f`.
   */
  def withZkClient(conf: KyuubiConf)(f: CuratorFramework => Unit): Unit = {
    val zkClient = buildZookeeperClient(conf)
    try {
      zkClient.start()
      f(zkClient)
    } finally {
      try {
        zkClient.close()
      } catch {
        case e: IOException => error("Failed to release the zkClient", e)
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
  def setUpZooKeeperAuth(conf: KyuubiConf): Unit = {
    def setupZkAuth(): Unit = {
      val keyTabFile = getKeyTabFile(conf)
      val maybePrincipal = conf.get(HA_ZK_AUTH_PRINCIPAL)
      val kerberized = maybePrincipal.isDefined && keyTabFile.isDefined
      if (UserGroupInformation.isSecurityEnabled && kerberized) {
        if (!new File(keyTabFile.get).exists()) {
          throw new IOException(s"${HA_ZK_AUTH_KEYTAB.key} does not exists")
        }
        System.setProperty("zookeeper.sasl.clientconfig", "KyuubiZooKeeperClient")
        var principal = maybePrincipal.get
        principal = KyuubiHadoopUtils.getServerPrincipal(principal)
        val jaasConf = new JaasConfiguration("KyuubiZooKeeperClient", principal, keyTabFile.get)
        Configuration.setConfiguration(jaasConf)
      }
    }

    if (conf.get(HA_ZK_ENGINE_REF_ID).isEmpty
      && ZooKeeperAuthTypes.withName(conf.get(HA_ZK_AUTH_TYPE)) == ZooKeeperAuthTypes.KERBEROS) {
      setupZkAuth()
    } else if (conf.get(HA_ZK_ENGINE_REF_ID).nonEmpty && ZooKeeperAuthTypes
      .withName(conf.get(HA_ZK_ENGINE_AUTH_TYPE)) == ZooKeeperAuthTypes.KERBEROS) {
      setupZkAuth()
    }

  }

  private def getKeyTabFile(conf: KyuubiConf): Option[String] = {
    val zkAuthKeytab = conf.get(HA_ZK_AUTH_KEYTAB)
    if (zkAuthKeytab.isDefined) {
      val zkAuthKeytabPath = zkAuthKeytab.get
      val relativeFileName = new File(zkAuthKeytabPath).getName
      if (new File(relativeFileName).exists()) {
        Some(relativeFileName)
      } else {
        Some(zkAuthKeytabPath)
      }
    } else {
      None
    }
  }

}
