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

package org.apache.kyuubi.service.authentication.ldap

import javax.net.SocketFactory

import com.unboundid.ldap.sdk._

import org.apache.kyuubi.config.KyuubiConf

/**
 * Shared construction helpers for [[LDAPConnectionOptions]] and [[ServerSet]] so the pool
 * path ([[UnboundIdConnectionPool]]) and the ephemeral path ([[UnboundIdDirSearchFactory]])
 * cannot drift apart on timeouts, SSL handling, or failover topology.
 *
 * The only intentional difference between the two paths is `autoReconnect`: pooled
 * connections benefit from automatic reconnect on detected closures, while ephemeral
 * connections are short-lived and discarded after a single bind, so reconnect is wasted work.
 */
private[ldap] object LdapServerSets {

  /**
   * Build an [[LDAPConnectionOptions]] from Kyuubi conf.
   * @param autoReconnect true for pooled connections, false for ephemeral.
   */
  def buildConnectionOptions(conf: KyuubiConf, autoReconnect: Boolean): LDAPConnectionOptions = {
    val opts = new LDAPConnectionOptions()
    // The config is a Long of milliseconds; setConnectTimeoutMillis takes an int. Clamp to
    // Int.MaxValue (~24 days) so an out-of-range value cannot silently overflow into a
    // negative/garbage timeout. Time configs are non-negative, so no lower-bound clamp.
    opts.setConnectTimeoutMillis(
      math.min(
        conf.get(KyuubiConf.AUTHENTICATION_LDAP_POOL_CONNECT_TIMEOUT),
        Int.MaxValue.toLong).toInt)
    opts.setResponseTimeoutMillis(
      conf.get(KyuubiConf.AUTHENTICATION_LDAP_POOL_RESPONSE_TIMEOUT))
    opts.setAutoReconnect(autoReconnect)
    opts
  }

  /**
   * Build a [[ServerSet]] from one or more [[LDAPURL]]s. A single URL produces a
   * [[SingleServerSet]]; multiple URLs produce a [[FailoverServerSet]] that tries each
   * server in order, advancing only on connection-level failures (so that bind errors
   * such as INVALID_CREDENTIALS surface immediately without retrying other servers).
   *
   * @throws IllegalArgumentException if `urls` is empty -- callers must validate config.
   */
  def buildServerSet(
      urls: Seq[LDAPURL],
      options: LDAPConnectionOptions,
      sslSocketFactory: SocketFactory): ServerSet = {
    require(urls.nonEmpty, "LDAP URL list must not be empty")

    def makeSingle(url: LDAPURL): SingleServerSet = {
      if (url.getScheme.equalsIgnoreCase("ldaps")) {
        new SingleServerSet(url.getHost, url.getPort, sslSocketFactory, options)
      } else {
        new SingleServerSet(url.getHost, url.getPort, options)
      }
    }

    urls match {
      case Seq(url) => makeSingle(url)
      case multi => new FailoverServerSet(multi.map(u => makeSingle(u): ServerSet): _*)
    }
  }
}
