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
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import javax.security.auth.login.Configuration

import scala.util.Random

import com.google.common.annotations.VisibleForTesting
import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.{ConfigEntry, KyuubiConf}
import org.apache.kyuubi.ha.HighAvailabilityConf._
import org.apache.kyuubi.ha.client.{AuthTypes, RetryPolicies}
import org.apache.kyuubi.ha.client.RetryPolicies._
import org.apache.kyuubi.shaded.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.kyuubi.shaded.curator.retry._
import org.apache.kyuubi.util.KyuubiHadoopUtils
import org.apache.kyuubi.util.reflect.DynConstructors

object ZookeeperClientProvider extends Logging {

  /**
   * Share JAAS configuration for Zookeeper client with same keytab and principal to
   * avoid server OOM due to each new JAAS configuration references the previous instance.
   * See KYUUBI #7154 for more details.
   */
  val jaasConfigurationCache = new ConcurrentHashMap[(String, String), Configuration]()

  /**
   * Create a [[CuratorFramework]] instance to be used as the ZooKeeper client
   * Use the [[ZookeeperACLProvider]] to create appropriate ACLs
   */
  def buildZookeeperClient(kyuubiConf: KyuubiConf): CuratorFramework = {
    setUpZooKeeperAuth(kyuubiConf)
    val connectionStr = kyuubiConf.get(HA_ADDRESSES)
    val sessionTimeout = kyuubiConf.get(HA_ZK_SESSION_TIMEOUT)
    val connectionTimeout = kyuubiConf.get(HA_ZK_CONN_TIMEOUT)
    val baseSleepTime = kyuubiConf.get(HA_ZK_CONN_BASE_RETRY_WAIT)
    val maxSleepTime = kyuubiConf.get(HA_ZK_CONN_MAX_RETRY_WAIT)
    val maxRetries = kyuubiConf.get(HA_ZK_CONN_MAX_RETRIES)
    val retryPolicyName = kyuubiConf.get(HA_ZK_CONN_RETRY_POLICY)
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
      .aclProvider(new ZookeeperACLProvider(kyuubiConf))
      .retryPolicy(retryPolicy)

    kyuubiConf.get(HA_ZK_AUTH_DIGEST).foreach { authString =>
      builder.authorization("digest", authString.getBytes(StandardCharsets.UTF_8))
    }

    builder.build()
  }

  def getGracefulStopThreadDelay(kyuubiConf: KyuubiConf): Long = {
    val baseSleepTime = kyuubiConf.get(HA_ZK_CONN_BASE_RETRY_WAIT)
    val maxSleepTime = kyuubiConf.get(HA_ZK_CONN_MAX_RETRY_WAIT)
    val maxRetries = kyuubiConf.get(HA_ZK_CONN_MAX_RETRIES)
    val retryPolicyName = kyuubiConf.get(HA_ZK_CONN_RETRY_POLICY)
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
   * @param kyuubiConf SparkConf
   * @return
   */
  @throws[Exception]
  def setUpZooKeeperAuth(kyuubiConf: KyuubiConf): Unit = {
    def setupZkAuth(
        principalConfKey: ConfigEntry[Option[String]],
        keytabConfKey: ConfigEntry[Option[String]]): Unit =
      (kyuubiConf.get(principalConfKey), getKeyTabFile(kyuubiConf, keytabConfKey)) match {
        case (Some(principal), Some(keytab)) if UserGroupInformation.isSecurityEnabled =>
          if (!new File(keytab).exists()) {
            throw new IOException(s"${keytabConfKey.key}: $keytab does not exists")
          }
          System.setProperty("zookeeper.sasl.clientconfig", "KyuubiZooKeeperClient")
          kyuubiConf.get(HA_ZK_AUTH_SERVER_PRINCIPAL).foreach { zkServerPrincipal =>
            // ZOOKEEPER-1467 allows configuring SPN in client
            System.setProperty("zookeeper.server.principal", zkServerPrincipal)
          }
          val zkClientPrincipal = KyuubiHadoopUtils.getServerPrincipal(principal)
          val jaasConf = jaasConfigurationCache.computeIfAbsent(
            (principal, keytab),
            _ => {
              // HDFS-16591 makes breaking change on JaasConfiguration
              DynConstructors.builder()
                .impl( // Hadoop 3.3.5 and above
                  "org.apache.hadoop.security.authentication.util.JaasConfiguration",
                  classOf[String],
                  classOf[String],
                  classOf[String])
                .impl( // Hadoop 3.3.4 and previous
                  // scalastyle:off
                  "org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager$JaasConfiguration",
                  // scalastyle:on
                  classOf[String],
                  classOf[String],
                  classOf[String])
                .build[Configuration]()
                .newInstance("KyuubiZooKeeperClient", zkClientPrincipal, keytab)
            })
          Configuration.setConfiguration(jaasConf)
        case _ =>
      }

    if (kyuubiConf.get(HA_ENGINE_REF_ID).isEmpty &&
      AuthTypes.withName(kyuubiConf.get(HA_ZK_AUTH_TYPE)) == AuthTypes.KERBEROS) {
      setupZkAuth(HA_ZK_AUTH_PRINCIPAL, HA_ZK_AUTH_KEYTAB)
    } else if (kyuubiConf.get(HA_ENGINE_REF_ID).nonEmpty &&
      AuthTypes.withName(kyuubiConf.get(HA_ZK_ENGINE_AUTH_TYPE)) == AuthTypes.KERBEROS) {
      setupZkAuth(HA_ZK_ENGINE_AUTH_PRINCIPAL, HA_ZK_ENGINE_AUTH_KEYTAB)
    }
  }

  @VisibleForTesting
  def getKeyTabFile(kyuubiConf: KyuubiConf, key: ConfigEntry[Option[String]]): Option[String] = {
    kyuubiConf.get(key).map { fullPath =>
      val filename = new File(fullPath).getName
      if (new File(filename).exists()) filename else fullPath
    }
  }
}
