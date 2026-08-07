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

package org.apache.kyuubi.service.authentication

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import javax.security.sasl.AuthenticationException

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.AuthMethods.AuthMethod
import org.apache.kyuubi.service.authentication.ldap.{CachingLdapAuthenticationProvider, LdapAuthCache, LdapAuthFailureClassifier, LdapSearchFactory, UnboundIdConnectionPool, UnboundIdDirSearchFactory}
import org.apache.kyuubi.util.ClassUtils

/**
 * This class helps select a [[PasswdAuthenticationProvider]] for a given [[AuthMethods]]
 */
object AuthenticationProviderFactory extends Logging {

  // Singletons keyed by auth scope ("server" or "http"). The LDAP stack must be a singleton
  // so the connection pool and auth cache are shared across all authentication requests -- not
  // recreated per request. JDBC providers are stateless, so per-call creation is fine there.
  private val ldapProviders = new ConcurrentHashMap[String, PasswdAuthenticationProvider]()

  @throws[AuthenticationException]
  def getAuthenticationProvider(
      method: AuthMethod,
      conf: KyuubiConf,
      isServer: Boolean = true): PasswdAuthenticationProvider = {
    if (isServer) {
      getAuthenticationProviderForServer(method, conf)
    } else {
      getAuthenticationProviderForEngine(conf)
    }
  }

  private def getAuthenticationProviderForServer(
      method: AuthMethod,
      conf: KyuubiConf): PasswdAuthenticationProvider = method match {
    case AuthMethods.NONE => new AnonymousAuthenticationProviderImpl
    case AuthMethods.LDAP => ldapProviders.computeIfAbsent("server", _ => buildLdapProvider(conf))
    case AuthMethods.JDBC => new JdbcAuthenticationProviderImpl(conf)
    case AuthMethods.CUSTOM =>
      val className = conf.get(KyuubiConf.AUTHENTICATION_CUSTOM_CLASS)
      require(
        className.nonEmpty,
        "kyuubi.authentication.custom.class must be set when auth method was CUSTOM.")
      ClassUtils.createInstance(className.get, classOf[PasswdAuthenticationProvider], conf)
    case _ => throw new AuthenticationException("Not a valid authentication method")
  }

  private def getAuthenticationProviderForEngine(conf: KyuubiConf): PasswdAuthenticationProvider = {
    if (conf.get(KyuubiConf.INTERNAL_SECURITY_ENABLED)) {
      new EngineSecureAuthenticationProviderImpl
    } else {
      new AnonymousAuthenticationProviderImpl
    }
  }

  def getHttpBasicAuthenticationProvider(
      method: AuthMethod,
      conf: KyuubiConf): PasswdAuthenticationProvider = method match {
    case AuthMethods.NONE => new AnonymousAuthenticationProviderImpl
    case AuthMethods.LDAP => ldapProviders.computeIfAbsent("http", _ => buildLdapProvider(conf))
    case AuthMethods.JDBC => new JdbcAuthenticationProviderImpl(conf)
    case AuthMethods.CUSTOM =>
      val className = conf.get(KyuubiConf.AUTHENTICATION_CUSTOM_BASIC_CLASS)
      require(
        className.nonEmpty,
        "kyuubi.authentication.custom.basic.class must be set for http basic authentication.")
      ClassUtils.createInstance(className.get, classOf[PasswdAuthenticationProvider], conf)
    case _ => throw new AuthenticationException("Not a valid authentication method")
  }

  def getHttpBearerAuthenticationProvider(
      providerClass: String,
      conf: KyuubiConf): TokenAuthenticationProvider = {
    require(
      !StringUtils.isBlank(providerClass),
      "kyuubi.authentication.custom.bearer.class must be set for http bearer authentication.")
    ClassUtils.createInstance(providerClass, classOf[TokenAuthenticationProvider], conf)
  }

  /**
   * Builds a [[PasswdAuthenticationProvider]] for LDAP.
   *
   * When pool is enabled, the stack is:
   *   CachingLdapAuthenticationProvider
   *     -> LdapAuthenticationProviderImpl(UnboundIdDirSearchFactory + connection pool)
   *
   * When pool is disabled (the default), uses the legacy JNDI path without caching. Enabling
   * the pool opts into warm, health-checked, failover-aware connections; the legacy path is
   * retained for environments where per-request connections are required (e.g. firewall
   * rules that disallow long-lived connections).
   */
  private def buildLdapProvider(conf: KyuubiConf): PasswdAuthenticationProvider = {
    if (conf.get(KyuubiConf.AUTHENTICATION_LDAP_POOL_ENABLED)) {
      val poolOpt = UnboundIdConnectionPool.getOrCreate(conf)
      val factory = new UnboundIdDirSearchFactory(conf, poolOpt)
      val core = new LdapAuthenticationProviderImpl(conf, factory)
      if (conf.get(KyuubiConf.AUTHENTICATION_LDAP_CACHE_ENABLED)) {
        new CachingLdapAuthenticationProvider(core, conf)
      } else {
        core
      }
    } else {
      // The auth cache is only wired into the UnboundID/pool path; warn loudly when an
      // operator turns the pool off but leaves caching enabled, so they're not silently
      // running without the cache they thought they had configured.
      if (conf.get(KyuubiConf.AUTHENTICATION_LDAP_CACHE_ENABLED)) {
        warn(
          s"${KyuubiConf.AUTHENTICATION_LDAP_CACHE_ENABLED.key} has no effect when " +
            s"${KyuubiConf.AUTHENTICATION_LDAP_POOL_ENABLED.key}=false: " +
            "the legacy JNDI auth path does not use the LDAP auth cache.")
      }
      new LdapAuthenticationProviderImpl(conf, new LdapSearchFactory)
    }
  }

  /** Called on server shutdown to release the LDAP connection pool. */
  def close(): Unit = {
    UnboundIdConnectionPool.close()
    LdapAuthCache.resetCache()
    ldapProviders.clear()
    // Reset auth counters so a re-init after close() (e.g. between test cases) starts
    // from a clean slate. In production close() runs once at JVM shutdown, so the reset
    // is a no-op there.
    ldapAuthSuccessCounter.set(0L)
    ldapAuthFailureInvalidCredentialsCounter.set(0L)
    ldapAuthFailureInvalidInputCounter.set(0L)
    ldapAuthFailureInfrastructureCounter.set(0L)
    ldapAuthCacheHitCounter.set(0L)
    ldapAuthCacheMissCounter.set(0L)
    // Drop any installed metrics recorder so subsequent server starts in the same JVM
    // (notably tests) do not see events emitted to a stale destination.
    ldapAuthMetricsRecorder = LdapAuthMetricsRecorder.NoOp
  }

  /**
   * Live gauge accessors for the LDAP connection pool. Each method returns 0 when the pool
   * has not yet been created (e.g. before the first authentication request, or when LDAP
   * pooling is disabled). Designed to be wired into [[org.apache.kyuubi.metrics.MetricsSystem]]
   * gauges from `kyuubi-server`, which depends on `kyuubi-metrics`; `kyuubi-common` does not.
   */
  def ldapPoolNumAvailableConnections: Long = ldapPoolStat(_.numAvailableConnections)
  def ldapPoolNumFailedCheckouts: Long = ldapPoolStat(_.numFailedCheckouts)
  def ldapPoolNumSuccessfulCheckouts: Long = ldapPoolStat(_.numSuccessfulCheckouts)
  def ldapPoolNumConnectionsClosedDefunct: Long = ldapPoolStat(_.numConnectionsClosedDefunct)
  def ldapPoolNumConnectionsClosedExpired: Long = ldapPoolStat(_.numConnectionsClosedExpired)

  private def ldapPoolStat(read: UnboundIdConnectionPool => Long): Long =
    UnboundIdConnectionPool.currentInstance.map(read).getOrElse(0L)

  // ---------------------------------------------------------------------------------------
  // LDAP authentication request counters.
  //
  // Held as AtomicLong here so they can be incremented from kyuubi-common code paths
  // (CachingLdapAuthenticationProvider, LdapAuthenticationProviderImpl) without taking a
  // dependency on kyuubi-metrics. KyuubiServer reads them via the Long getters below and
  // registers them as MetricsSystem gauges, which DropwizardExports surfaces 1:1 in
  // Prometheus. Counters are reset on close() so test isolation is automatic.
  //
  // Semantics:
  //   ldapAuthSuccess              = ldapAuthCacheHit + (cache-miss attempts that succeeded)
  //   sum(ldapAuthFailure_*)       = cache-miss attempts that failed
  //   ldapAuthCacheMiss            = (success - cache-hit) + sum(failure_*)   [caching ON]
  //   ldapAuthCacheHit / cache-miss = both 0 when caching is disabled
  // ---------------------------------------------------------------------------------------

  private val ldapAuthSuccessCounter = new AtomicLong(0L)
  private val ldapAuthFailureInvalidCredentialsCounter = new AtomicLong(0L)
  private val ldapAuthFailureInvalidInputCounter = new AtomicLong(0L)
  private val ldapAuthFailureInfrastructureCounter = new AtomicLong(0L)
  private val ldapAuthCacheHitCounter = new AtomicLong(0L)
  private val ldapAuthCacheMissCounter = new AtomicLong(0L)

  // Installable callback for surfacing auth events to a metrics system. See
  // [[LdapAuthMetricsRecorder]] for why this is a hook rather than a direct call -- short
  // version: cumulative counters must surface in Prometheus as Counter (not Gauge) so
  // rate() / increase() handle process restarts correctly, and kyuubi-common does not
  // depend on kyuubi-metrics. KyuubiServer installs a real implementation at startup.
  @volatile private var ldapAuthMetricsRecorder: LdapAuthMetricsRecorder =
    LdapAuthMetricsRecorder.NoOp

  /**
   * Installs the recorder that surfaces LDAP auth events to the metrics subsystem.
   * Passing `null` resets to the no-op default. Idempotent across calls.
   */
  def setLdapAuthMetricsRecorder(recorder: LdapAuthMetricsRecorder): Unit = {
    ldapAuthMetricsRecorder = if (recorder == null) LdapAuthMetricsRecorder.NoOp else recorder
  }

  /** Records a successful LDAP authentication (cache hit OR cache-miss success). */
  private[authentication] def recordLdapAuthSuccess(): Unit = {
    ldapAuthSuccessCounter.incrementAndGet()
    ldapAuthMetricsRecorder.recordSuccess()
  }

  /**
   * Records a failed LDAP authentication, classified by `reason`. Unknown reasons fall
   * into the `infrastructure` bucket so a typo at a call site does not silently drop the
   * event.
   */
  private[authentication] def recordLdapAuthFailure(reason: String): Unit = {
    val normalised = reason match {
      case LdapAuthFailureClassifier.INVALID_CREDENTIALS =>
        ldapAuthFailureInvalidCredentialsCounter.incrementAndGet()
        LdapAuthFailureClassifier.INVALID_CREDENTIALS
      case LdapAuthFailureClassifier.INVALID_INPUT =>
        ldapAuthFailureInvalidInputCounter.incrementAndGet()
        LdapAuthFailureClassifier.INVALID_INPUT
      case _ =>
        ldapAuthFailureInfrastructureCounter.incrementAndGet()
        LdapAuthFailureClassifier.INFRASTRUCTURE
    }
    ldapAuthMetricsRecorder.recordFailure(normalised)
  }

  /** Records a cache hit at the LDAP authentication caching layer. */
  private[authentication] def recordLdapAuthCacheHit(): Unit = {
    ldapAuthCacheHitCounter.incrementAndGet()
    ldapAuthMetricsRecorder.recordCacheHit()
  }

  /** Records a cache miss at the LDAP authentication caching layer. */
  private[authentication] def recordLdapAuthCacheMiss(): Unit = {
    ldapAuthCacheMissCounter.incrementAndGet()
    ldapAuthMetricsRecorder.recordCacheMiss()
  }

  // In-process counter accessors retained for test inspection. Tightened to
  // `private[authentication]` because production metric exposure now flows through the
  // recorder above -- no caller outside this package needs these.
  private[authentication] def ldapAuthSuccess: Long = ldapAuthSuccessCounter.get()
  private[authentication] def ldapAuthFailureInvalidCredentials: Long =
    ldapAuthFailureInvalidCredentialsCounter.get()
  private[authentication] def ldapAuthFailureInvalidInput: Long =
    ldapAuthFailureInvalidInputCounter.get()
  private[authentication] def ldapAuthFailureInfrastructure: Long =
    ldapAuthFailureInfrastructureCounter.get()
  private[authentication] def ldapAuthCacheHit: Long = ldapAuthCacheHitCounter.get()
  private[authentication] def ldapAuthCacheMiss: Long = ldapAuthCacheMissCounter.get()
}
