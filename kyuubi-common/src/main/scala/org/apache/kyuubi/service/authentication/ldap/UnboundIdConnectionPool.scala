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

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import javax.net.ssl.SSLContext

import com.unboundid.ldap.sdk._

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf

/**
 * Owns a persistent [[LDAPConnectionPool]] backed by an UnboundID [[FailoverServerSet]].
 *
 * One instance is created at server startup and shared across all authentication requests.
 * The pool holds pre-authenticated connections for the configured bind user so that each
 * authentication request borrows a warm connection rather than performing a full TCP + TLS
 * + BIND round-trip. A background health-check thread validates idle connections against the
 * LDAP root DSE and replaces any that are broken or stale.
 *
 * Only instantiated when both binddn and bindpw are configured -- if no bind user is set the
 * factory falls back to ephemeral per-request connections.
 */
private[authentication] class UnboundIdConnectionPool(conf: KyuubiConf)
  extends Closeable with Logging {

  private val urls: Seq[LDAPURL] =
    conf.get(KyuubiConf.AUTHENTICATION_LDAP_URL)
      .map(LdapUrlParser.parseAll)
      .getOrElse(Seq.empty)

  private val bindDn = conf.get(KyuubiConf.AUTHENTICATION_LDAP_BIND_USER).get
  private val bindPassword = conf.get(KyuubiConf.AUTHENTICATION_LDAP_BIND_PASSWORD).get

  private val minConns = conf.get(KyuubiConf.AUTHENTICATION_LDAP_POOL_MIN_CONNECTIONS)
  private val maxConns = conf.get(KyuubiConf.AUTHENTICATION_LDAP_POOL_MAX_CONNECTIONS)
  private val healthCheckIntervalMs =
    conf.get(KyuubiConf.AUTHENTICATION_LDAP_POOL_HEALTH_CHECK_INTERVAL)
  private val checkoutTimeoutMs =
    conf.get(KyuubiConf.AUTHENTICATION_LDAP_POOL_CHECKOUT_TIMEOUT)

  // autoReconnect=true: the pool detects closed sockets between requests and reconnects
  // transparently, so a server bounce does not drain the pool to zero on the next checkout.
  private val connectionOptions: LDAPConnectionOptions =
    LdapServerSets.buildConnectionOptions(conf, autoReconnect = true)

  private val sslSocketFactory = SSLContext.getDefault.getSocketFactory

  val serverSet: ServerSet = {
    if (urls.isEmpty) {
      throw new IllegalStateException(
        "kyuubi.authentication.ldap.url must be set when pool is enabled")
    }
    // FailoverServerSet tries servers in order, advancing only on connection-level failure.
    // Ordered failover (rather than round-robin) is intentional: it prefers the first
    // configured URL and only moves to the next when a connection cannot be established,
    // which suits deployments where each URL fronts its own load-balanced LDAP endpoint.
    LdapServerSets.buildServerSet(urls, connectionOptions, sslSocketFactory)
  }

  // Primary server is the first URL in the ordered list. A checkout that lands on a
  // different host means FailoverServerSet has advanced past a failed primary -- we
  // surface that as a warn so operators know the primary is unhealthy without having to
  // cross-reference pool defunct-connection counters in Prometheus.
  private val primaryHost: String = urls.head.getHost
  private val primaryPort: Int = urls.head.getPort

  // Tracks whether checkouts are currently landing on a secondary server, so the failover
  // warning fires only on state transitions rather than once per checkout. Logging per
  // checkout would flood the auth hot path during a sustained primary outage.
  private val onFailover = new AtomicBoolean(false)

  private val pool: LDAPConnectionPool = buildPool()

  private def buildPool(): LDAPConnectionPool = {
    val bindRequest = new SimpleBindRequest(bindDn, bindPassword)
    val pool = new LDAPConnectionPool(serverSet, bindRequest, minConns, maxConns)
    pool.setHealthCheckIntervalMillis(healthCheckIntervalMs)
    // The SDK default for setMaxWaitTimeMillis is 0: skip the wait queue, attempt to grow
    // the pool, and fail fast if not (typically CONNECT_ERROR when already at maxConnections,
    // or the result code from the underlying connection attempt if creation was tried).
    // A positive value smooths brief contention spikes so they do not surface as auth
    // failures during normal bursts.
    pool.setMaxWaitTimeMillis(checkoutTimeoutMs)
    // Validate idle connections by fetching the root DSE (empty base DN, always readable).
    // invokeOnCheckout=false avoids an extra round-trip on every borrow; the background
    // thread and release validation keep the pool healthy without adding per-request latency.
    pool.setHealthCheck(new LoggingHealthCheck(new GetEntryLDAPConnectionPoolHealthCheck(
      "", // root DSE -- readable by the bind user on any LDAP server
      0L, // max response time: use connection-level response timeout
      false, // invokeOnCreate: connection just authenticated, skip redundant check
      true, // invokeAfterAuthentication: verify bind succeeded
      false, // invokeOnCheckout: skip to avoid per-request latency
      true, // invokeOnRelease: detect connections broken during use
      true // invokeForBackgroundChecks: evict stale idle connections
    )))
    val urlStr = urls
      .map(u => s"${u.getScheme}://${u.getHost}:${u.getPort}")
      .mkString(", ")
    info(
      s"LDAP connection pool initialised: [$urlStr]" +
        s" min=$minConns max=$maxConns" +
        s" healthCheck=${healthCheckIntervalMs}ms checkoutTimeout=${checkoutTimeoutMs}ms")
    pool
  }

  /**
   * Returns a connection borrowed from the pool. Caller must call [[releaseConnection]] or
   * [[releaseConnectionAfterException]] when done.
   *
   * Detects when a borrowed connection is on a server other than the primary (first
   * configured URL), which means [[FailoverServerSet]] has advanced past the primary due to
   * a connection-level failure. [[FailoverServerSet]] itself is silent about which server it
   * chose, so this is the only signal of a live failover.
   *
   * Logging is edge-triggered to keep the auth hot path quiet during a sustained outage:
   * a single WARN on the primary -> secondary transition, a single INFO on recovery, and the
   * per-connection detail at DEBUG. A per-checkout WARN would otherwise emit one line per
   * authentication request for the entire duration of an outage.
   */
  def checkoutConnection(): LDAPConnection = {
    val conn = pool.getConnection()
    val connHost = conn.getConnectedAddress
    val connPort = conn.getConnectedPort
    val onSecondary = connHost != primaryHost || connPort != primaryPort
    if (onSecondary) {
      if (onFailover.compareAndSet(false, true)) {
        warn(
          s"LDAP pool failover active: connections now landing on secondary server" +
            s" $connHost:$connPort (primary is $primaryHost:$primaryPort). The primary LDAP" +
            s" server may be down. Per-checkout detail is logged at DEBUG until recovery.")
      } else {
        debug(s"LDAP pool checkout on secondary server $connHost:$connPort")
      }
    } else if (onFailover.compareAndSet(true, false)) {
      info(s"LDAP pool failover recovered: connections back on primary $primaryHost:$primaryPort")
    }
    conn
  }

  /**
   * Returns a healthy connection to the pool.
   * Call [[releaseConnectionAfterException]] instead if the connection experienced an
   * [[LDAPException]] so the pool can decide whether to reuse or replace it.
   */
  def releaseConnection(conn: LDAPConnection): Unit = pool.releaseConnection(conn)

  /**
   * Releases a connection after an [[LDAPException]] was thrown during its use.
   *
   * Delegates to [[LDAPConnectionPool.releaseConnectionAfterException]], which is the
   * SDK-recommended approach: it inspects the exception's result code and the pool's
   * configured health check to decide automatically whether to return the connection for
   * reuse or discard it. This is preferable to manually branching on a boolean `defunct`
   * flag because the pool's health check has more context than our call site does.
   */
  def releaseConnectionAfterException(conn: LDAPConnection, e: LDAPException): Unit =
    pool.releaseConnectionAfterException(conn, e)

  /**
   * Live gauge accessors for [[org.apache.kyuubi.metrics.MetricsSystem]]. Each call reads the
   * current value from [[LDAPConnectionPoolStatistics]] held by the SDK pool -- no copying or
   * caching, so the values reflect the pool state at the moment the metric is scraped.
   *
   * Cumulative counters (failed/successful checkouts, defunct/expired closures) are exposed
   * as [[Long]] gauges; dashboards compute deltas from successive scrapes.
   */
  def numAvailableConnections: Long = pool.getConnectionPoolStatistics.getNumAvailableConnections
  def numFailedCheckouts: Long = pool.getConnectionPoolStatistics.getNumFailedCheckouts
  def numSuccessfulCheckouts: Long = pool.getConnectionPoolStatistics.getNumSuccessfulCheckouts
  def numConnectionsClosedDefunct: Long =
    pool.getConnectionPoolStatistics.getNumConnectionsClosedDefunct
  def numConnectionsClosedExpired: Long =
    pool.getConnectionPoolStatistics.getNumConnectionsClosedExpired

  override def close(): Unit = {
    pool.close()
    info("LDAP connection pool closed")
  }
}

/**
 * Decorates [[LDAPConnectionPoolHealthCheck]] to emit a [[Logging.warn]] whenever the
 * delegate decides a connection must be discarded.
 *
 * Only the two discard paths are instrumented:
 *  - [[ensureConnectionValidForContinuedUse]]: called on release and by background checks.
 *  - [[ensureConnectionValidAfterException]]: called via [[releaseConnectionAfterException]].
 *
 * [[ensureNewConnectionValid]] is delegated silently -- a healthy replacement connection
 * is expected behaviour and not worth logging at warn.
 */
final private[ldap] class LoggingHealthCheck(delegate: LDAPConnectionPoolHealthCheck)
  extends LDAPConnectionPoolHealthCheck with Logging {

  override def ensureConnectionValidForContinuedUse(conn: LDAPConnection): Unit =
    try delegate.ensureConnectionValidForContinuedUse(conn)
    catch {
      case e: LDAPException =>
        warn(
          s"LDAP pool discarding connection to " +
            s"${conn.getConnectedAddress}:${conn.getConnectedPort}" +
            s" (${e.getResultCode}): ${e.getMessage}")
        throw e
    }

  override def ensureConnectionValidAfterException(
      conn: LDAPConnection,
      e: LDAPException): Unit =
    try delegate.ensureConnectionValidAfterException(conn, e)
    catch {
      case ex: LDAPException =>
        // Log both the original exception that triggered the health check (e -- the "why
        // did the operation fail") and the health check's own rejection (ex -- the "why is
        // the connection being discarded"). Previously only `e` was logged, which meant
        // operators saw the triggering failure but not the health check's discard reason.
        warn(
          s"LDAP pool discarding connection to " +
            s"${conn.getConnectedAddress}:${conn.getConnectedPort}" +
            s" after exception (${e.getResultCode}): ${e.getMessage}" +
            s"; health check rejected with (${ex.getResultCode}): ${ex.getMessage}")
        throw ex
    }

  override def ensureNewConnectionValid(conn: LDAPConnection): Unit =
    delegate.ensureNewConnectionValid(conn)
}

/**
 * Singleton holder for the [[UnboundIdConnectionPool]].
 *
 * Initialised on first use (double-checked locking) so the pool is created once and shared
 * for the lifetime of the Kyuubi server process. [[close]] is called on server shutdown via
 * [[org.apache.kyuubi.service.authentication.AuthenticationProviderFactory.close]].
 */
private[authentication] object UnboundIdConnectionPool extends Logging {

  @volatile private var instance: Option[UnboundIdConnectionPool] = None
  private val lock = new Object()

  /**
   * Returns the live pool instance without forcing creation. Used by metric gauges that
   * read pool statistics: gauges registered before the pool exists return their default
   * value until the first authentication request triggers [[getOrCreate]].
   */
  def currentInstance: Option[UnboundIdConnectionPool] = instance

  def getOrCreate(conf: KyuubiConf): Option[UnboundIdConnectionPool] = {
    if (instance.isEmpty) {
      lock.synchronized {
        if (instance.isEmpty) {
          val hasBindUser =
            conf.get(KyuubiConf.AUTHENTICATION_LDAP_BIND_USER).isDefined &&
              conf.get(KyuubiConf.AUTHENTICATION_LDAP_BIND_PASSWORD).isDefined
          instance =
            if (hasBindUser) Some(new UnboundIdConnectionPool(conf))
            else {
              info(
                "LDAP bind user not configured: pool disabled, ephemeral connections will be used")
              None
            }
        }
      }
    }
    instance
  }

  def close(): Unit = lock.synchronized {
    instance.foreach(_.close())
    instance = None
  }
}
