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

import javax.naming.NamingException
import javax.net.ssl.SSLContext
import javax.security.sasl.AuthenticationException

import com.unboundid.ldap.sdk._

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf

/**
 * [[DirSearchFactory]] backed by UnboundID SDK.
 *
 * Two connection strategies are applied depending on who is being authenticated:
 *
 *  - Bind user (service account): borrows a pre-authenticated connection from the
 *    [[UnboundIdConnectionPool]]. The pool holds warm, health-checked connections so the
 *    borrow is effectively free -- no TCP handshake, no TLS negotiation, no BIND round-trip.
 *    On [[DirSearch.close]] the connection is returned (healthy) or discarded (defunct).
 *
 *  - End user (credential verification): obtains a short-lived [[LDAPConnection]] from the
 *    [[ServerSet]] and issues a BIND request with the user's DN and password.
 *    [[FailoverServerSet.getConnection]] tries each configured server in order, skipping to
 *    the next only on connection-level failures (DNS, refused, timeout). BIND failure with
 *    INVALID_CREDENTIALS (LDAP error 49) is thrown immediately without failover -- bad
 *    credentials should fail fast and retrying other servers would risk triggering AD lockout.
 *
 * When no bind user is configured (unusual in AD environments but supported), every call
 * uses the end-user path -- the caller's own credentials are used to search LDAP.
 */
private[authentication] class UnboundIdDirSearchFactory(
    conf: KyuubiConf,
    poolOpt: Option[UnboundIdConnectionPool])
  extends DirSearchFactory with Logging {

  private val bindDn: Option[String] = conf.get(KyuubiConf.AUTHENTICATION_LDAP_BIND_USER)

  // Reuse the pool's ServerSet when available -- ephemeral connections share the same
  // server topology and SSL configuration without duplicating the setup. When no pool is
  // configured, build a dedicated ServerSet from the same URL list (but without
  // autoReconnect, which is pool-specific and inappropriate for short-lived connections).
  private val serverSet: ServerSet =
    poolOpt.map(_.serverSet).getOrElse(buildEphemeralServerSet())

  override def getInstance(conf: KyuubiConf, principal: String, password: String): DirSearch = {
    val isBindUser = bindDn.exists(_ == principal) && poolOpt.isDefined
    if (isBindUser) {
      borrowFromPool(poolOpt.get)
    } else {
      ephemeralBind(principal, password)
    }
  }

  private def borrowFromPool(pool: UnboundIdConnectionPool): DirSearch = {
    try {
      val conn = pool.checkoutConnection()
      new UnboundIdDirSearch(conf, conn, poolOpt)
    } catch {
      case e: LDAPException =>
        throw new AuthenticationException(
          s"Failed to borrow connection from LDAP pool: ${e.getMessage}",
          e)
    }
  }

  /**
   * Obtains a short-lived connection from the [[ServerSet]] and issues a BIND.
   *
   * [[FailoverServerSet.getConnection]] handles server-level failover natively: it tries
   * each server in order and only advances to the next on a connection-level failure.
   * INVALID_CREDENTIALS is raised during the BIND step -- after a connection is already
   * established -- so it is never retried across servers, preventing AD lockout events.
   */
  private def ephemeralBind(principal: String, password: String): DirSearch = {
    val conn =
      try {
        serverSet.getConnection()
      } catch {
        case e: LDAPException =>
          val ne = new NamingException(s"LDAP connect failed: ${e.getMessage}")
          ne.initCause(e)
          throw ne
      }
    try {
      // bind() throws LDAPException for any non-SUCCESS result; it never returns a
      // non-SUCCESS BindResult, so no extra check on the result code is needed.
      conn.bind(new SimpleBindRequest(principal, password))
      new UnboundIdDirSearch(conf, conn, poolOpt = None)
    } catch {
      case e: LDAPException if e.getResultCode == ResultCode.INVALID_CREDENTIALS =>
        conn.close()
        throw new AuthenticationException(
          s"Error validating LDAP user: $principal (InvalidCredentials: ${e.getMessage})",
          e)
      case e: LDAPException =>
        conn.close()
        val ne = new NamingException(s"LDAP bind failed: ${e.getMessage}")
        ne.initCause(e)
        throw ne
      case t: Throwable =>
        conn.close()
        throw t
    }
  }

  private def buildEphemeralServerSet(): ServerSet = {
    val urls = conf.get(KyuubiConf.AUTHENTICATION_LDAP_URL)
      .map(LdapUrlParser.parseAll)
      .getOrElse(Seq.empty)

    if (urls.isEmpty) {
      throw new IllegalStateException(
        "kyuubi.authentication.ldap.url must be set for LDAP authentication")
    }

    // autoReconnect=false: ephemeral connections are used for one bind then discarded;
    // reconnect logic is wasted work and would mask transient failures we want to surface.
    val opts = LdapServerSets.buildConnectionOptions(conf, autoReconnect = false)
    LdapServerSets.buildServerSet(urls, opts, SSLContext.getDefault.getSocketFactory)
  }
}
