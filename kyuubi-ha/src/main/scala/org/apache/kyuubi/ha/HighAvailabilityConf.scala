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

package org.apache.kyuubi.ha

import java.time.Duration
import java.util.Locale

import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.config.{ConfigBuilder, ConfigEntry, KyuubiConf, OptionalConfigEntry}
import org.apache.kyuubi.engine.ProvidePolicy
import org.apache.kyuubi.ha.client.RetryPolicies

object HighAvailabilityConf {

  private def buildConf(key: String): ConfigBuilder = KyuubiConf.buildConf(key)

  val HA_ZK_QUORUM: ConfigEntry[String] = buildConf("ha.zookeeper.quorum")
    .doc("The connection string for the zookeeper ensemble")
    .version("1.0.0")
    .stringConf
    .createWithDefault("")

  val HA_ZK_NAMESPACE: ConfigEntry[String] = buildConf("ha.zookeeper.namespace")
    .doc("The root directory for the service to deploy its instance uri. Additionally, it will" +
      " creates a -[username] suffixed root directory for each application")
    .version("1.0.0")
    .stringConf
    .createWithDefault("kyuubi")

  val HA_ZK_ACL_ENABLED: ConfigEntry[Boolean] =
    buildConf("ha.zookeeper.acl.enabled")
      .doc("Set to true if the zookeeper ensemble is kerberized")
      .version("1.0.0")
      .booleanConf
      .createWithDefault(UserGroupInformation.isSecurityEnabled)

  val HA_ZK_CONN_MAX_RETRIES: ConfigEntry[Int] =
    buildConf("ha.zookeeper.connection.max.retries")
      .doc("Max retry times for connecting to the zookeeper ensemble")
      .version("1.0.0")
      .intConf
      .createWithDefault(3)

  val HA_ZK_CONN_BASE_RETRY_WAIT: ConfigEntry[Int] =
    buildConf("ha.zookeeper.connection.base.retry.wait")
      .doc("Initial amount of time to wait between retries to the zookeeper ensemble")
      .version("1.0.0")
      .intConf
      .createWithDefault(1000)

  val HA_ZK_CONN_MAX_RETRY_WAIT: ConfigEntry[Int] =
    buildConf("ha.zookeeper.connection.max.retry.wait")
      .doc(s"Max amount of time to wait between retries for" +
        s" ${RetryPolicies.BOUNDED_EXPONENTIAL_BACKOFF} policy can reach, or max time until" +
        s" elapsed for ${RetryPolicies.UNTIL_ELAPSED} policy to connect the zookeeper ensemble")
      .version("1.0.0")
      .intConf
      .createWithDefault(30 * 1000)

  val HA_ZK_CONN_TIMEOUT: ConfigEntry[Int] = buildConf("ha.zookeeper.connection.timeout")
    .doc("The timeout(ms) of creating the connection to the zookeeper ensemble")
    .version("1.0.0")
    .intConf
    .createWithDefault(15 * 1000)

  val HA_ZK_SESSION_TIMEOUT: ConfigEntry[Int] = buildConf("ha.zookeeper.session.timeout")
    .doc("The timeout(ms) of a connected session to be idled")
    .version("1.0.0")
    .intConf
    .createWithDefault(60 * 1000)

  val HA_ZK_CONN_RETRY_POLICY: ConfigEntry[String] =
    buildConf("ha.zookeeper.connection.retry.policy")
    .doc("The retry policy for connecting to the zookeeper ensemble, all candidates are:" +
      s" ${RetryPolicies.values.mkString("<ul><li>", "</li><li> ", "</li></ul>")}")
    .version("1.0.0")
    .stringConf
    .checkValues(RetryPolicies.values.map(_.toString))
    .createWithDefault(RetryPolicies.EXPONENTIAL_BACKOFF.toString)

  val HA_ZK_NODE_TIMEOUT: ConfigEntry[Long] =
    buildConf("ha.zookeeper.node.creation.timeout")
    .doc("Timeout for creating zookeeper node")
    .version("1.2.0")
    .timeConf
    .checkValue(_ > 0, "Must be positive")
    .createWithDefault(Duration.ofSeconds(120).toMillis)

  val HA_ZK_ENGINE_SESSION_ID: OptionalConfigEntry[String] =
    buildConf("ha.engine.session.id")
    .doc("The sessionId will be attached to zookeeper node when engine started, " +
      "and the kyuubi server will check it cyclically.")
    .internal
    .version("1.4.0")
    .stringConf
    .createOptional

  val HA_ZK_ENGINE_POOL_SIZE: ConfigEntry[Int] =
    buildConf("ha.engine.pool.size")
      .doc("Maximum number of engines when using USER share level.")
      .version("1.4.0")
      .intConf
      .checkValue(s => s > 0 && s < 50, "Invalid Engine Pool Size, should be between 0 and 50")
      .createWithDefault(1)

  val HA_ZK_ENGINE_PROVIDE_POLICY: ConfigEntry[String] =
    buildConf("ha.engine.provide.policy")
      .doc("Multiple engines can be exposed under same space when using USER share level, " +
        "choose the appropriate strategy according to different scenarios.")
      .version("1.4.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .createWithDefault(ProvidePolicy.RANDOM.toString)
}
