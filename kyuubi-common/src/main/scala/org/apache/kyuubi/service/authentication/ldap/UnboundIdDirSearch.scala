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

import scala.collection.JavaConverters._

import com.unboundid.ldap.sdk._

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf

/**
 * [[DirSearch]] implementation backed by an UnboundID [[LDAPConnection]].
 *
 * Two modes are supported:
 *  - Pooled: the connection was borrowed from [[UnboundIdConnectionPool]]. On [[close]],
 *    [[LDAPConnectionPool.releaseConnectionAfterException]] is used when an exception was
 *    recorded during the request, letting the pool's health check decide whether the
 *    connection is still usable or should be discarded and replaced.
 *  - Ephemeral: the connection was created for a single credential-verification BIND. On
 *    [[close]], the connection is closed and discarded unconditionally.
 *
 * Unlike the JNDI-based [[LdapSearch]], search failures are propagated as
 * [[NamingException]] rather than silently swallowed. This allows callers to detect
 * mid-query connection drops and surface them as authentication failures instead of
 * returning empty search results that would masquerade as "user not found".
 *
 * Partial results from [[LDAPSearchException]] (e.g. SIZE_LIMIT_EXCEEDED) are returned
 * rather than thrown away, matching the intent of the sizeLimit setting in [[QueryFactory]].
 * Whether the connection is reused or discarded after such an exception is delegated to
 * [[ResultCode.isConnectionUsable]], which returns true for server-side soft limits and
 * false only for result codes that indicate a broken socket or session.
 */
private[ldap] class UnboundIdDirSearch(
    conf: KyuubiConf,
    initialConnection: LDAPConnection,
    poolOpt: Option[UnboundIdConnectionPool])
  extends DirSearch with Logging {

  // The active connection used for searches. Mutable so that the mid-request retry path
  // (see searchWithAttributes) can swap a broken pooled connection for a fresh one without
  // bubbling the failure up to the caller. Only mutated on the pooled path; ephemeral
  // searches do not retry because their connection is bound to specific end-user
  // credentials that we cannot re-establish from inside this class.
  private var connection: LDAPConnection = initialConnection

  private val baseDn = conf.get(KyuubiConf.AUTHENTICATION_LDAP_BASE_DN).orNull
  private val groupBases: Array[String] =
    LdapUtils.patternsToBaseDns(
      LdapUtils.parseDnPatterns(conf, KyuubiConf.AUTHENTICATION_LDAP_GROUP_DN_PATTERN))
  private val userPatterns: Array[String] =
    LdapUtils.parseDnPatterns(conf, KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN)
  private val userBases: Array[String] = LdapUtils.patternsToBaseDns(userPatterns)
  private val queries: QueryFactory = new QueryFactory(conf)

  // Bounded number of swap-and-retry attempts on connection-level failures. One retry is
  // enough to cover a single broken pooled connection (rotated server, dropped socket,
  // idle reset by an intermediary) while keeping the request latency bounded and avoiding
  // retry storms during a real outage.
  // Tunable via kyuubi.authentication.ldap.pool.search.maxRetries.
  private val maxSearchRetries: Int =
    conf.get(KyuubiConf.AUTHENTICATION_LDAP_POOL_SEARCH_MAX_RETRIES)

  // The last LDAPException thrown during this request, if any. Stored so that
  // close() can delegate the reuse-or-discard decision to the pool's health check via
  // releaseConnectionAfterException rather than making that call at the exception site.
  // DirSearch is used within a single request thread, so no volatile needed.
  private var lastException: Option[LDAPException] = None

  // Set true when the retry path has already returned the active connection to the pool
  // via releaseConnectionAfterException AND a replacement checkout failed -- so close()
  // must not release the same connection a second time. Reset to false once a fresh
  // connection is successfully checked out (the new connection is unreleased).
  private var connectionAlreadyReleased: Boolean = false

  override def findUserDn(user: String): String = {
    val allNames =
      if (LdapUtils.isDn(user)) {
        val userBaseDn = LdapUtils.extractBaseDn(user)
        val userRdn = LdapUtils.extractFirstRdn(user)
        search(Array(userBaseDn), queries.findUserDnByRdn(userRdn))
      } else {
        val byPattern = findDnByPattern(userPatterns, user)
        if (byPattern.nonEmpty) byPattern
        else search(userBases, queries.findUserDnByName(user))
      }

    if (allNames.length == 1) allNames.head
    else {
      info(
        s"Expected exactly one user result for $user but got ${allNames.length}. Returning null")
      null
    }
  }

  override def findGroupDn(group: String): String = {
    val results = search(groupBases, queries.findGroupDnById(group))
    if (results.length == 1) results.head
    else throw new NamingException("Single result was expected")
  }

  override def isUserMemberOfGroup(user: String, groupDn: String): Boolean = {
    val userId = LdapUtils.extractUserName(user)
    search(userBases, queries.isUserMemberOfGroup(userId, groupDn)).nonEmpty
  }

  override def findGroupsForUser(userDn: String): Array[String] = {
    val userName = LdapUtils.extractUserName(userDn)
    search(groupBases, queries.findGroupsForUser(userName, userDn))
  }

  override def executeCustomQuery(query: String): Array[String] = {
    val results = searchWithAttributes(Array(baseDn), queries.customQuery(query))
    results.flatMap { entry =>
      Seq(entry.getDN) ++ entry.getAttributes.asScala.flatMap(_.getValues.toSeq)
    }
  }

  private def findDnByPattern(patterns: Seq[String], name: String): Array[String] = {
    for (pattern <- patterns) {
      val patternBaseDn = LdapUtils.extractBaseDn(pattern)
      // Use replace (literal) not replaceAll (regex): usernames containing '$' or '\'
      // are treated as regex back-references / escape sequences in the replacement string
      // by String.replaceAll, causing unexpected rewriting or IndexOutOfBoundsException.
      val rdn = LdapUtils.extractFirstRdn(pattern).replace("%s", name)
      val names = search(Array(patternBaseDn), queries.findDnByPattern(rdn))
      if (names.nonEmpty) return names
    }
    Array.empty
  }

  private def search(baseDns: Array[String], query: Query): Array[String] =
    searchWithAttributes(baseDns, query).map(_.getDN)

  private def searchWithAttributes(
      baseDns: Array[String],
      query: Query): Array[SearchResultEntry] = {
    // query.filter is already a parsed Filter object -- no redundant re-parse inside the SDK.
    // query.attributes / query.sizeLimit replace the JNDI SearchControls bridge.
    //
    // Every query except customQuery reads only entry.getDN (see `search`), so request no
    // attributes via the RFC 4511 "1.1" OID (SearchRequest.NO_ATTRIBUTES) to avoid pulling
    // every attribute over the wire for DN-only lookups. customQuery is the only caller that
    // populates query.attributes (the group-membership attribute) and is honored as-is.
    // This matches both the pre-UnboundID default (empty returningAttributes) and the JNDI
    // path in LdapSearch.
    val requestedAttrs: Array[String] =
      if (query.attributes.isEmpty) Array(SearchRequest.NO_ATTRIBUTES) else query.attributes.toArray

    debug(s"Executing LDAP query '${query.filterString}' on bases [${baseDns.mkString(", ")}]")

    baseDns.toSeq.flatMap { base =>
      if (base == null) Seq.empty[SearchResultEntry]
      else searchBaseWithRetry(base, query, requestedAttrs, maxSearchRetries)
    }.toArray
  }

  /**
   * Executes one base-DN search with bounded retry on connection-level failures.
   *
   * Retry contract:
   *  - Only the pooled path retries. On retry, the broken connection is released to the
   *    pool with [[LDAPConnectionPool.releaseConnectionAfterException]] (which discards it
   *    via the configured health check), and a fresh one is checked out. If the pool's
   *    [[com.unboundid.ldap.sdk.FailoverServerSet]] is configured with multiple URLs, the
   *    fresh connection will land on a healthy server, closing the gap where JNDI fails
   *    over only at initial context creation.
   *  - The ephemeral path does not retry: its connection was bound with end-user
   *    credentials that this class does not retain.
   *  - Retry is restricted to result codes where [[ResultCode.isConnectionUsable]] returns
   *    false (SERVER_DOWN, CONNECT_ERROR, LOCAL_ERROR, etc.). Protocol-level errors that
   *    leave the connection usable (e.g. INVALID_DN_SYNTAX) are propagated without retry --
   *    retrying them on a different connection would not change the outcome.
   *  - Soft-limit codes (SIZE_LIMIT_EXCEEDED etc.) are not retried because partial results
   *    are intentionally returned per the [[QueryFactory]] sizeLimit contract.
   */
  private def searchBaseWithRetry(
      base: String,
      query: Query,
      requestedAttrs: Array[String],
      attemptsRemaining: Int): Seq[SearchResultEntry] = {
    try {
      val request =
        new SearchRequest(base, SearchScope.SUB, query.filter, requestedAttrs: _*)
      if (query.sizeLimit > 0) request.setSizeLimit(query.sizeLimit)
      connection.search(request).getSearchEntries.asScala.toSeq
    } catch {
      case e: LDAPSearchException
          if e.getResultCode == ResultCode.NO_SUCH_OBJECT ||
            e.getResultCode == ResultCode.NO_SUCH_ATTRIBUTE =>
        // Base DN does not exist or attribute absent -- not an error, move to next base.
        Seq.empty[SearchResultEntry]

      case e: LDAPSearchException if e.getResultCode.isConnectionUsable =>
        // Server responded but truncated results (e.g. SIZE_LIMIT_EXCEEDED, TIME_LIMIT_EXCEEDED,
        // ADMIN_LIMIT_EXCEEDED). Return whatever entries arrived before the limit; the
        // connection is still alive for soft-limit codes -- isConnectionUsable separates the
        // "server told us to stop" outcomes (handled here) from the "connection died" ones
        // (which fall through to the retry/propagate cases below).
        //
        // Auth-safety note: returning a truncated result set cannot create false positives.
        // LDAP only includes entries that already matched the search filter, so truncation
        // can shrink the matching set but never invent matches. The worst behaviour change
        // is that an ambiguous result (multiple matches expected) appears unique -- callers
        // that enforce uniqueness (e.g. findUserDn requires length == 1) handle that case
        // explicitly. Configure sizeLimit >= 2 in QueryFactory so duplicates remain
        // observable when truncation occurs.
        debug(s"LDAP search truncated (${e.getResultCode}) on base '$base': ${e.getMessage}")
        Option(e.getSearchEntries).fold(Seq.empty[SearchResultEntry])(_.asScala.toSeq)

      case e: LDAPException
          if attemptsRemaining > 0 &&
            !e.getResultCode.isConnectionUsable &&
            poolOpt.isDefined =>
        // Connection-level failure on the pooled path. Discard the broken connection,
        // check out a fresh one, and retry. This is what closes the JNDI failover gap:
        // a server failing mid-search (not just at initial connect) is recovered
        // transparently to the caller as long as a healthy peer is available.
        val pool = poolOpt.get
        warn(
          s"LDAP search failed mid-request on ${connection.getConnectedAddress}:" +
            s"${connection.getConnectedPort} (${e.getResultCode}): ${e.getMessage}. " +
            s"Retrying with a fresh connection (attempts left: ${attemptsRemaining - 1}).")
        // Mark BEFORE releasing. Either ordering has an edge case:
        //   - Set before: if release throws *before* the pool accepts the connection,
        //     close() will skip releasing and the connection may leak.
        //   - Set after: if release throws *after* the pool accepts the connection,
        //     close() will release it a second time and corrupt pool accounting.
        // Picking "before" because LDAPConnectionPool.releaseConnectionAfterException
        // delegates to releaseDefunctConnection / releaseConnection internally and both
        // are no-throw on a non-null conn against a live pool (the two preconditions
        // this code already satisfies by construction). The double-release case, by
        // contrast, is genuinely reachable when every peer in the FailoverServerSet is
        // down on the replacement checkout. Revisit if a future SDK upgrade widens the
        // throw contract of releaseConnectionAfterException.
        connectionAlreadyReleased = true
        pool.releaseConnectionAfterException(connection, e)
        try {
          connection = pool.checkoutConnection()
          // Replacement is healthy and now owned by this DirSearch -- subsequent close()
          // must release this new connection normally.
          connectionAlreadyReleased = false
        } catch {
          case ce: LDAPException =>
            // Could not get a replacement (pool exhausted, all servers down). Leave
            // connectionAlreadyReleased=true so close() does NOT release the previous
            // (already-released) connection a second time. Surface the ORIGINAL search
            // failure for diagnostic clarity; the checkout error chains under it.
            lastException = Some(e)
            val ne = toNamingException(e)
            ne.addSuppressed(ce)
            throw ne
        }
        searchBaseWithRetry(base, query, requestedAttrs, attemptsRemaining - 1)

      case e: LDAPException =>
        // Either non-recoverable, no retries left, or ephemeral path. Record and propagate.
        lastException = Some(e)
        throw toNamingException(e)
    }
  }

  /**
   * Returns the connection to the pool or closes it, depending on whether an exception
   * was recorded during the request.
   *
   * When an exception was recorded, [[UnboundIdConnectionPool.releaseConnectionAfterException]]
   * is called instead of manually choosing between [[releaseConnection]] and
   * [[releaseDefunctConnection]]. This delegates the reuse-or-discard decision to
   * [[LDAPConnectionPool.releaseConnectionAfterException]], which consults the pool's
   * configured health check -- the same check already in use for background validation.
   */
  override def close(): Unit = poolOpt match {
    case Some(pool) =>
      if (connectionAlreadyReleased) {
        // Retry path already returned this connection to the pool (with the failure
        // attached) and could not obtain a replacement. Releasing again would either
        // double-return a discarded connection or hand the pool a re-released
        // healthy one -- both corrupt the pool's accounting. Nothing to do.
        debug("LDAP DirSearch close(): connection already released during retry path")
      } else {
        lastException match {
          case Some(e) => pool.releaseConnectionAfterException(connection, e)
          case None => pool.releaseConnection(connection)
        }
      }
    case None =>
      connection.close()
  }

  private def toNamingException(e: LDAPException): NamingException = {
    val ne = new NamingException(
      s"LDAP operation failed (${e.getResultCode}): ${e.getMessage}")
    ne.initCause(e)
    ne
  }
}
