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

import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.util.concurrent.{Callable, ExecutionException, TimeUnit}
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import javax.security.sasl.AuthenticationException

import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.util.concurrent.UncheckedExecutionException

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.{AuthenticationProviderFactory, PasswdAuthenticationProvider}

/**
 * [[PasswdAuthenticationProvider]] decorator that short-circuits repeated LDAP round-trips
 * for the same (username, password) pair within a configurable TTL.
 *
 * Cache key: HMAC-SHA256(username + NUL + password) encoded as a hex string. Using HMAC
 * rather than a plain hash prevents offline dictionary attacks against the in-memory cache;
 * the key material (secret) is generated once at JVM startup and never leaves the process.
 * Passwords themselves are not stored anywhere in the cache.
 *
 * Only successful authentications are cached. Failed attempts always reach LDAP, which
 * allows clients to correct credentials without waiting for a TTL to expire, and avoids
 * the security risk of caching a "bad credentials" result that would block a subsequent
 * correct password.
 *
 * The backing Guava [[Cache]] lives on the [[LdapAuthCache]] companion object so that the
 * "server" and "http" auth scopes (which AuthenticationProviderFactory builds independently
 * via computeIfAbsent) share a single cache. Both scopes hit the same LDAP backend with
 * the same user identity, so cross-protocol cache reuse is correct and removes the second
 * LDAP round-trip a user pays when they connect over both Thrift and HTTP.
 */
private[authentication] class CachingLdapAuthenticationProvider(
    delegate: PasswdAuthenticationProvider,
    conf: KyuubiConf)
  extends PasswdAuthenticationProvider with Logging {

  // The cache itself is shared across all CachingLdapAuthenticationProvider instances --
  // see the class doc for why. Conf-derived TTL is also kept here only for the debug log
  // formatting; the cache's actual TTL is set by getOrCreateCache from the first conf.
  private val cache: Cache[String, java.lang.Boolean] = LdapAuthCache.getOrCreateCache(conf)
  private val ttlMs = conf.get(KyuubiConf.AUTHENTICATION_LDAP_CACHE_TTL)

  override def authenticate(user: String, password: String): Unit = {
    // Null inputs: bypass cacheKey entirely and delegate directly. String concatenation
    // converts null to the literal "null", so cacheKey(null, p) would hash "null<NUL>p"
    // -- producing a semantically wrong key that could collide with a real username
    // "null". The delegate validates blank/null inputs and throws AuthenticationException
    // with proper invalid_input classification; no caching is needed for that path.
    if (user == null || password == null) {
      AuthenticationProviderFactory.recordLdapAuthCacheMiss()
      delegate.authenticate(user, password)
      return
    }
    val key = LdapAuthCache.cacheKey(user, password)

    // Fast path: avoid Callable allocation when the entry is already present. A cache hit
    // is itself a successful authentication from the user's perspective, so it increments
    // the success counter alongside the cache.hit counter -- the delegate is NOT called,
    // so the impl-layer success counter is intentionally not bumped here.
    if (cache.getIfPresent(key) != null) {
      AuthenticationProviderFactory.recordLdapAuthCacheHit()
      AuthenticationProviderFactory.recordLdapAuthSuccess()
      debug(s"LDAP auth cache hit for user $user")
      return
    }

    // Cache miss. cache.get(key, Callable) coalesces concurrent identical requests so a
    // burst of authentications for the same (user, password) results in one LDAP
    // round-trip rather than N: exactly one thread (the "leader") invokes the callable
    // while the others ("waiters") block until the leader's result is available.
    //
    // Per-request accounting under coalescing: every request observed an empty cache when
    // it arrived, so every request bumps cache.miss (this matches the documented semantics
    // of cache.miss as "this request did not find a cached entry"). For success/failure
    // counters, however, the impl inside the callable only records once -- for the leader.
    // To keep success/failure counts per-request (so 8 successful concurrent auths show as
    // 8 successes, not 1), waiters must record their own outcome on the wrapper side. The
    // `wasLeader` flag below distinguishes the leader (whose impl already recorded) from
    // waiters (who need wrapper-side recording).
    //
    // Failure path symmetric: Guava propagates the leader's exception to all waiters via
    // the same ExecutionException, and the impl recorded the leader's failure once -- but
    // every request observed a failed authentication, so waiters classify and record their
    // own failure. Classification uses the cause chain when present, falling back to the
    // ExecutionException itself.
    //
    // Guava propagates the callable's exception WITHOUT populating the cache, so failed
    // authentications are never cached -- preserving the no-negative-caching contract.
    AuthenticationProviderFactory.recordLdapAuthCacheMiss()
    var wasLeader = false
    try {
      cache.get(
        key,
        new Callable[java.lang.Boolean] {
          override def call(): java.lang.Boolean = {
            wasLeader = true
            delegate.authenticate(user, password)
            java.lang.Boolean.TRUE
          }
        })
      if (!wasLeader) {
        // Waited on the leader's successful authentication; record my own success.
        AuthenticationProviderFactory.recordLdapAuthSuccess()
      }
      debug(s"LDAP auth cache populated for user $user (ttl=${ttlMs}ms)")
    } catch {
      case e: ExecutionException =>
        if (!wasLeader) recordCoalescedFailure(e)
        CachingLdapAuthenticationProvider.rethrowAsAuth(e)
      case e: UncheckedExecutionException =>
        if (!wasLeader) recordCoalescedFailure(e)
        CachingLdapAuthenticationProvider.rethrowAsAuth(e)
    }
  }

  /**
   * Classifies and records a failure observed by a coalesced waiter. The leader's impl
   * has already recorded its own failure with the classification it derived from the
   * underlying exception; here we classify from whatever Guava handed us so that the
   * waiter's failure bucket matches the leader's as closely as possible.
   */
  private def recordCoalescedFailure(wrapped: Exception): Unit = {
    val cause = Option(wrapped.getCause).getOrElse(wrapped)
    AuthenticationProviderFactory.recordLdapAuthFailure(
      LdapAuthFailureClassifier.classify(cause))
  }

  // Test-only helpers: scoped to the ldap package so unit tests can drive the cache, but
  // hidden from production call sites that should never need to peek at the cache state.
  private[ldap] def invalidate(user: String, password: String): Unit =
    cache.invalidate(LdapAuthCache.cacheKey(user, password))

  private[ldap] def cacheSize(): Long = cache.size()
}

private[authentication] object CachingLdapAuthenticationProvider {

  /**
   * Unwraps a Guava cache-loader wrapper exception and rethrows it as an
   * [[AuthenticationException]]. The original cause is preserved either by rethrowing it
   * directly (if it already is an [[AuthenticationException]]) or by chaining it inside a
   * fresh [[AuthenticationException]] -- so callers see the same exception type they would
   * have seen without the cache wrapper.
   */
  private def rethrowAsAuth(wrapped: Exception): Nothing = wrapped.getCause match {
    case ae: AuthenticationException => throw ae
    case other =>
      val cause = if (other != null) other else wrapped
      val ae = new AuthenticationException(cause.getMessage)
      ae.initCause(cause)
      throw ae
  }
}

// Visibility is `private[authentication]` rather than `private[ldap]` because
// `AuthenticationProviderFactory.close()` needs to reset the shared cache between server
// lifecycles (and between test cases) -- the factory lives one package up.
private[authentication] object LdapAuthCache extends Logging {

  // Per-process secret generated once at class load time. HMAC key is 32 random bytes.
  private val hmacSecret: Array[Byte] = {
    val bytes = new Array[Byte](32)
    new SecureRandom().nextBytes(bytes)
    bytes
  }

  // One initialised Mac per thread -- amortises JCA provider lookup and SecretKeySpec
  // allocation across requests. Mac.doFinal() resets state per JCA spec, so the instance
  // is safe to reuse after each call without an explicit reset().
  private val threadLocalMac: ThreadLocal[Mac] = ThreadLocal.withInitial[Mac](() => {
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(hmacSecret, "HmacSHA256"))
    mac
  })

  // NUL (\u0000) separator between user and password makes the HMAC input unambiguous --
  // without it, ("a", "bc") and ("ab", "c") would hash identically. The Unicode escape is
  // used in the source (rather than a literal 0x00 byte) so the file stays plain ASCII
  // and tools like git diff, GitHub, scalafmt, and grep handle it correctly.
  private val UserPasswordSeparator: String = "\u0000"

  def cacheKey(user: String, password: String): String = {
    val raw = threadLocalMac
      .get()
      .doFinal((user + UserPasswordSeparator + password).getBytes(StandardCharsets.UTF_8))
    toHex(raw)
  }

  // Shared cache backing every CachingLdapAuthenticationProvider in the process. Lazy init
  // is gated by getOrCreateCache below; resetCache nulls it out for test isolation. The
  // ttl / max-size snapshot of the first conf is kept alongside so subsequent callers with
  // divergent conf get a visible warning rather than a silent reuse.
  @volatile private var sharedCache: Cache[String, java.lang.Boolean] = _
  @volatile private var initialTtlMs: Long = 0L
  @volatile private var initialMaxSize: Int = 0
  private val cacheInitLock = new Object()

  /**
   * Returns the singleton auth-success cache. The first caller's conf determines the cache
   * size and TTL; subsequent callers reuse the same instance regardless of their conf.
   * That assumption is safe in production because AuthenticationProviderFactory always
   * builds providers from the same KyuubiConf within a process lifetime, but it can drift
   * in embedded contexts or tests that build multiple providers from different confs.
   * To make that drift observable we log on init and warn on every divergent-conf reuse.
   */
  private[ldap] def getOrCreateCache(conf: KyuubiConf): Cache[String, java.lang.Boolean] = {
    if (sharedCache == null) {
      cacheInitLock.synchronized {
        if (sharedCache == null) {
          val ttlMs = conf.get(KyuubiConf.AUTHENTICATION_LDAP_CACHE_TTL)
          val maxSize = conf.get(KyuubiConf.AUTHENTICATION_LDAP_CACHE_MAX_SIZE)
          sharedCache = CacheBuilder.newBuilder()
            .maximumSize(maxSize)
            .expireAfterWrite(ttlMs, TimeUnit.MILLISECONDS)
            .build()
          initialTtlMs = ttlMs
          initialMaxSize = maxSize
          info(s"LDAP auth cache initialised: ttlMs=$ttlMs maxSize=$maxSize")
        }
      }
    } else {
      // Already initialised; the existing cache is not reconfigured. Warn the caller if
      // their conf disagrees so the silent first-caller-wins behaviour is observable.
      val ttlMs = conf.get(KyuubiConf.AUTHENTICATION_LDAP_CACHE_TTL)
      val maxSize = conf.get(KyuubiConf.AUTHENTICATION_LDAP_CACHE_MAX_SIZE)
      if (ttlMs != initialTtlMs || maxSize != initialMaxSize) {
        warn(
          s"LDAP auth cache reuse with mismatched config: initial " +
            s"(ttlMs=$initialTtlMs maxSize=$initialMaxSize) vs requested " +
            s"(ttlMs=$ttlMs maxSize=$maxSize). The existing cache is unchanged. " +
            s"Restart Kyuubi to apply the new values.")
      }
    }
    sharedCache
  }

  /**
   * Discards the shared cache so the next [[getOrCreateCache]] call rebuilds it from fresh
   * conf. Invoked from [[AuthenticationProviderFactory.close]]; effectively a no-op in
   * production (close runs once at JVM shutdown) and used between tests for isolation.
   */
  private[authentication] def resetCache(): Unit = cacheInitLock.synchronized {
    sharedCache = null
    initialTtlMs = 0L
    initialMaxSize = 0
  }

  private def toHex(bytes: Array[Byte]): String = {
    val sb = new StringBuilder(bytes.length * 2)
    bytes.foreach(b => sb.append(f"${b & 0xFF}%02x"))
    sb.toString()
  }
}
