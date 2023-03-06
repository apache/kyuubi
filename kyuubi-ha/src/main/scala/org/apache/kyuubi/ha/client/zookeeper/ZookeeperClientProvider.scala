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

import java.io.{File, IOException}
import javax.security.auth.login.Configuration

import scala.util.Random

import com.google.common.annotations.VisibleForTesting
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry._
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.JaasConfiguration

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.{AuthTypes, RetryPolicies}
import org.apache.kyuubi.ha.client.RetryPolicies._
import org.apache.kyuubi.util.KyuubiHadoopUtils

object ZookeeperClientProvider extends Logging {

  /**
   * Create a [[CuratorFramework]] instance to be used as the ZooKeeper client
   * Use the [[ZookeeperACLProvider]] to create appropriate ACLs
   */
  def buildZookeeperClient(conf: KyuubiConf): CuratorFramework = {
    setUpZooKeeperAuth(conf)
    val connectionStr = conf.get(HA_ADDRESSES)
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
    val builder = CuratorFrameworkFactory.builder()
      .connectString(connectionStr)
      .sessionTimeoutMs(sessionTimeout)
      .connectionTimeoutMs(connectionTimeout)
      .aclProvider(new ZookeeperACLProvider(conf))
      .retryPolicy(retryPolicy)

    conf.get(HA_ZK_AUTH_DIGEST) match {
      case Some(anthString) =>
        builder.authorization("digest", anthString.getBytes("UTF-8"))
      case _ =>
    }

    builder.build()
  }

  def getGracefulStopThreadDelay(conf: KyuubiConf): Long = {
    val baseSleepTime = conf.get(HA_ZK_CONN_BASE_RETRY_WAIT)
    val maxSleepTime = conf.get(HA_ZK_CONN_MAX_RETRY_WAIT)
    val maxRetries = conf.get(HA_ZK_CONN_MAX_RETRIES)
    val retryPolicyName = conf.get(HA_ZK_CONN_RETRY_POLICY)
    RetryPolicies.withName(retryPolicyName) match {
      case ONE_TIME => baseSleepTime
      case N_TIME => maxRetries * baseSleepTime
      case BOUNDED_EXPONENTIAL_BACKOFF =>
        (0 until maxRetries).map { retryCount =>
          val retryWait = baseSleepTime * Math.max(1, Random.nextInt(1 << (retryCount + 1)))
          Math.min(retryWait, maxSleepTime)
        }.sum
      case UNTIL_ELAPSED => maxSleepTime
      case EXPONENTIAL_BACKOFF =>
        (0 until maxRetries).map { retryCount =>
          baseSleepTime * Math.max(1, Random.nextInt(1 << (retryCount + 1)))
        }.sum
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
          throw new IOException(s"${HA_ZK_AUTH_KEYTAB.key}: $keyTabFile does not exists")
        }
        System.setProperty("zookeeper.sasl.clientconfig", "KyuubiZooKeeperClient")
        var principal = maybePrincipal.get
        principal = KyuubiHadoopUtils.getServerPrincipal(principal)
        val jaasConf = new JaasConfiguration("KyuubiZooKeeperClient", principal, keyTabFile.get)
        Configuration.setConfiguration(jaasConf)
      }
    }

    if (conf.get(HA_ENGINE_REF_ID).isEmpty
      && AuthTypes.withName(conf.get(HA_ZK_AUTH_TYPE)) == AuthTypes.KERBEROS) {
      setupZkAuth()
    } else if (conf.get(HA_ENGINE_REF_ID).nonEmpty && AuthTypes
        .withName(conf.get(HA_ZK_ENGINE_AUTH_TYPE)) == AuthTypes.KERBEROS) {
      setupZkAuth()
    }

  }

  @VisibleForTesting
  def getKeyTabFile(conf: KyuubiConf): Option[String] = {
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
