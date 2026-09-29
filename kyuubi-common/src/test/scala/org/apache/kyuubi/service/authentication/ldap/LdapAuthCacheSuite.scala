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

import java.util.concurrent.{Callable, CountDownLatch, CyclicBarrier, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import javax.security.sasl.AuthenticationException

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.{AuthenticationProviderFactory, PasswdAuthenticationProvider}

class LdapAuthCacheSuite extends KyuubiFunSuite {

  // Counter state lives on the AuthenticationProviderFactory singleton; reset between
  // tests so cache.hit / cache.miss / success assertions are independent of execution
  // order. The singleton only resets through close(), which is safe to call here because
  // no LDAP pool has been initialised in this suite.
  override def afterEach(): Unit = {
    try AuthenticationProviderFactory.close()
    finally super.afterEach()
  }

  private def makeDelegate(onAuthenticate: (String, String) => Unit): PasswdAuthenticationProvider =
    (user: String, password: String) => onAuthenticate(user, password)

  test("cacheKey is deterministic for same inputs") {
    val k1 = LdapAuthCache.cacheKey("alice", "secret")
    val k2 = LdapAuthCache.cacheKey("alice", "secret")
    assert(k1 === k2)
  }

  test("cacheKey differs for different passwords") {
    val k1 = LdapAuthCache.cacheKey("alice", "secret1")
    val k2 = LdapAuthCache.cacheKey("alice", "secret2")
    assert(k1 !== k2)
  }

  test("cacheKey differs for different users") {
    val k1 = LdapAuthCache.cacheKey("alice", "secret")
    val k2 = LdapAuthCache.cacheKey("bob", "secret")
    assert(k1 !== k2)
  }

  test("successful authentication is cached and skips delegate on repeat") {
    val callCount = new AtomicInteger(0)
    val delegate = makeDelegate((_, _) => { callCount.incrementAndGet(); () })
    val conf = new KyuubiConf()
    val provider = new CachingLdapAuthenticationProvider(delegate, conf)

    provider.authenticate("alice", "secret")
    provider.authenticate("alice", "secret")
    provider.authenticate("alice", "secret")

    assert(callCount.get() === 1)
    assert(provider.cacheSize() === 1)
  }

  test("failed authentication is not cached") {
    val callCount = new AtomicInteger(0)
    val delegate = makeDelegate { (_, _) =>
      callCount.incrementAndGet()
      throw new AuthenticationException("bad credentials")
    }
    val conf = new KyuubiConf()
    val provider = new CachingLdapAuthenticationProvider(delegate, conf)

    intercept[AuthenticationException](provider.authenticate("alice", "wrong"))
    intercept[AuthenticationException](provider.authenticate("alice", "wrong"))

    assert(callCount.get() === 2)
    assert(provider.cacheSize() === 0)
  }

  test("different users are cached independently") {
    val callCount = new AtomicInteger(0)
    val delegate = makeDelegate((_, _) => { callCount.incrementAndGet(); () })
    val conf = new KyuubiConf()
    val provider = new CachingLdapAuthenticationProvider(delegate, conf)

    provider.authenticate("alice", "pw1")
    provider.authenticate("bob", "pw2")
    provider.authenticate("alice", "pw1")
    provider.authenticate("bob", "pw2")

    assert(callCount.get() === 2)
    assert(provider.cacheSize() === 2)
  }

  test("password change after cache hit still allows re-auth after invalidation") {
    var storedPassword = "initial"
    val delegate = makeDelegate { (_, password) =>
      if (password != storedPassword) {
        throw new AuthenticationException("wrong password")
      }
    }
    val conf = new KyuubiConf()
    val provider = new CachingLdapAuthenticationProvider(delegate, conf)

    provider.authenticate("alice", "initial")
    // Cache hit -- old password still works from cache
    provider.authenticate("alice", "initial")

    // After invalidation, new password must be used
    provider.invalidate("alice", "initial")
    storedPassword = "newpassword"

    intercept[AuthenticationException](provider.authenticate("alice", "initial"))
    provider.authenticate("alice", "newpassword")
  }

  test("concurrent identical authentications collapse to a single delegate call") {
    // Setup: a delegate that blocks inside authenticate until released, so we can be sure
    // multiple threads are simultaneously waiting on the same cache key. Without Guava's
    // per-key coalescing in cache.get(key, Callable) all threads would each invoke the
    // delegate; with coalescing only one does, and the rest receive the cached result.
    val callCount = new AtomicInteger(0)
    val release = new CountDownLatch(1)
    val firstCallEntered = new CountDownLatch(1)
    val delegate = makeDelegate { (_, _) =>
      callCount.incrementAndGet()
      firstCallEntered.countDown()
      // Hold this delegate invocation open so other threads pile up at cache.get.
      assert(release.await(5, TimeUnit.SECONDS), "test gate never released")
    }
    val provider = new CachingLdapAuthenticationProvider(delegate, new KyuubiConf())

    val threads = 8
    val pool = Executors.newFixedThreadPool(threads)
    try {
      // submit() returns immediately; invokeAll() would block here forever because the
      // first task is parked inside the delegate awaiting `release`.
      val futures = (1 to threads).map { _ =>
        pool.submit(new Callable[Unit] {
          override def call(): Unit = provider.authenticate("alice", "secret")
        })
      }

      // Wait until at least one thread is inside the delegate, then let it finish.
      assert(
        firstCallEntered.await(5, TimeUnit.SECONDS),
        "no thread reached the delegate")
      release.countDown()

      // All futures must complete without timing out.
      futures.foreach(_.get(5, TimeUnit.SECONDS))
    } finally {
      pool.shutdownNow()
    }

    // Coalescing means exactly one LDAP round-trip for the burst.
    assert(callCount.get() === 1)
    assert(provider.cacheSize() === 1)
  }

  test("max size config is respected") {
    val conf = new KyuubiConf()
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_CACHE_MAX_SIZE, 2)
    val delegate = makeDelegate((_, _) => ())
    val provider = new CachingLdapAuthenticationProvider(delegate, conf)

    provider.authenticate("user1", "pw")
    provider.authenticate("user2", "pw")
    provider.authenticate("user3", "pw")

    // Cache size should not exceed max
    assert(provider.cacheSize() <= 2)
  }

  test("cache hit increments cache.hit and success counters") {
    val delegate = makeDelegate((_, _) => ())
    val provider = new CachingLdapAuthenticationProvider(delegate, new KyuubiConf())

    // First call -> miss (delegate invoked, success recorded by delegate is bypassed
    // because the delegate here is a stub, not the impl). The wrapper records the miss.
    provider.authenticate("alice", "secret")
    assert(AuthenticationProviderFactory.ldapAuthCacheMiss === 1L)
    assert(AuthenticationProviderFactory.ldapAuthCacheHit === 0L)
    assert(AuthenticationProviderFactory.ldapAuthSuccess === 0L)

    // Second + third calls -> cache hits, both bump cache.hit and success.
    provider.authenticate("alice", "secret")
    provider.authenticate("alice", "secret")
    assert(AuthenticationProviderFactory.ldapAuthCacheMiss === 1L)
    assert(AuthenticationProviderFactory.ldapAuthCacheHit === 2L)
    assert(AuthenticationProviderFactory.ldapAuthSuccess === 2L)
  }

  test("cache miss increments cache.miss but not success on its own") {
    // Stub delegate that succeeds; the wrapper records cache.miss only. The delegate
    // here does NOT record success (production delegate, LdapAuthenticationProviderImpl,
    // does that itself; this assertion verifies the wrapper does not double-count).
    val delegate = makeDelegate((_, _) => ())
    val provider = new CachingLdapAuthenticationProvider(delegate, new KyuubiConf())

    provider.authenticate("alice", "secret")
    assert(AuthenticationProviderFactory.ldapAuthCacheMiss === 1L)
    assert(AuthenticationProviderFactory.ldapAuthCacheHit === 0L)
    assert(
      AuthenticationProviderFactory.ldapAuthSuccess === 0L,
      "wrapper must not record success on cache miss; delegate is responsible")
  }

  test("metrics recorder receives the same events the in-process counters do") {
    // Install a recorder that captures every call; verify it sees exactly the same set
    // of events the in-process AtomicLong counters observe. KyuubiServer installs an
    // equivalent recorder in production that delegates to MetricsSystem.incCount, which
    // surfaces as Prometheus type `counter`.
    val recorder = new CapturingLdapAuthRecorder
    AuthenticationProviderFactory.setLdapAuthMetricsRecorder(recorder)

    val delegate = makeDelegate((_, _) => ())
    val provider = new CachingLdapAuthenticationProvider(delegate, new KyuubiConf())

    provider.authenticate("alice", "secret") // miss
    provider.authenticate("alice", "secret") // hit (cache.hit + success)
    provider.authenticate("alice", "secret") // hit (cache.hit + success)

    // Recorder calls match the in-process counters exactly.
    assert(recorder.cacheMisses.get === 1)
    assert(recorder.cacheHits.get === 2)
    assert(recorder.successes.get === 2)
    assert(AuthenticationProviderFactory.ldapAuthCacheMiss === 1L)
    assert(AuthenticationProviderFactory.ldapAuthCacheHit === 2L)
    assert(AuthenticationProviderFactory.ldapAuthSuccess === 2L)
  }

  test("concurrent identical authentications: success counter matches request count" +
    " under coalescing") {
    // Pins the per-request invariant: when N concurrent identical requests are coalesced
    // by Guava (one delegate call, N-1 waiters), the success counter must report N, not 1.
    // The stub delegate mimics LdapAuthenticationProviderImpl's contract of recording its
    // own success when its callable actually runs; the wrapper records success for the
    // waiters via its `wasLeader` logic.
    val callCount = new AtomicInteger(0)
    val release = new CountDownLatch(1)
    val firstCallEntered = new CountDownLatch(1)
    val delegate = makeDelegate { (_, _) =>
      callCount.incrementAndGet()
      firstCallEntered.countDown()
      assert(release.await(5, TimeUnit.SECONDS), "test gate never released")
      // Match the impl's recording behaviour: success on actual delegate execution.
      AuthenticationProviderFactory.recordLdapAuthSuccess()
    }
    val provider = new CachingLdapAuthenticationProvider(delegate, new KyuubiConf())

    val threads = 8
    val pool = Executors.newFixedThreadPool(threads)
    // Pre-warm so all 8 worker threads exist before the real tasks queue; otherwise the
    // last-arriving worker can be late enough to find the cache already populated and
    // observe a hit instead of becoming a coalescing waiter.
    val warmup = (1 to threads).map { _ =>
      pool.submit(new Callable[Unit] { override def call(): Unit = () })
    }
    warmup.foreach(_.get(5, TimeUnit.SECONDS))

    // CyclicBarrier releases all task threads into authenticate at the same instant so
    // they race into cache.get together rather than serially.
    val barrier = new CyclicBarrier(threads)
    try {
      val futures = (1 to threads).map { _ =>
        pool.submit(new Callable[Unit] {
          override def call(): Unit = {
            barrier.await(5, TimeUnit.SECONDS)
            provider.authenticate("alice", "secret")
          }
        })
      }
      assert(
        firstCallEntered.await(5, TimeUnit.SECONDS),
        "no thread reached the delegate")
      // Brief pause so the other 7 threads have time to enter cache.get and block as
      // waiters before the leader's callable releases. Without this, the leader can
      // complete the callable before slow-arriving waiters reach cache.get, leaving
      // them to observe a populated cache and record a hit instead of a miss.
      Thread.sleep(200)
      release.countDown()
      futures.foreach(_.get(5, TimeUnit.SECONDS))
    } finally {
      pool.shutdownNow()
    }

    assert(callCount.get() === 1, "coalescing must collapse to exactly one delegate call")
    assert(
      AuthenticationProviderFactory.ldapAuthCacheMiss === 8L,
      "every concurrent request observed an empty cache when it arrived")
    assert(AuthenticationProviderFactory.ldapAuthCacheHit === 0L)
    assert(
      AuthenticationProviderFactory.ldapAuthSuccess === 8L,
      s"every request must contribute one success increment; got " +
        s"${AuthenticationProviderFactory.ldapAuthSuccess} for 8 requests with 1 delegate call")
  }

  test("concurrent identical authentications: failure counter matches request count" +
    " under coalescing") {
    // Symmetric to the success test. When the leader's delegate throws, Guava propagates
    // the exception to every waiter via the same ExecutionException. The impl recorded
    // the leader's failure once; waiters classify and record their own failure on the
    // wrapper side, so the failure bucket totals equal the request count.
    //
    // Note: Guava does NOT cache failures, so a thread that arrives after the leader's
    // callable has failed will trigger a fresh delegate invocation. The pool warm-up +
    // CyclicBarrier + post-firstCallEntered sleep ensures all 8 waiters are inside
    // cache.get before the leader's failure releases, so all 8 are coalesced under the
    // leader's single callable invocation.
    val callCount = new AtomicInteger(0)
    val release = new CountDownLatch(1)
    val firstCallEntered = new CountDownLatch(1)
    val delegate = makeDelegate { (_, _) =>
      callCount.incrementAndGet()
      firstCallEntered.countDown()
      assert(release.await(5, TimeUnit.SECONDS), "test gate never released")
      // Match the impl's recording behaviour: failure with classification on delegate
      // execution. The wrapper records its own failure for waiters via its own classifier.
      AuthenticationProviderFactory.recordLdapAuthFailure(
        LdapAuthFailureClassifier.INVALID_CREDENTIALS)
      throw new AuthenticationException("simulated invalid credentials")
    }
    val provider = new CachingLdapAuthenticationProvider(delegate, new KyuubiConf())

    val threads = 8
    val pool = Executors.newFixedThreadPool(threads)
    val warmup = (1 to threads).map { _ =>
      pool.submit(new Callable[Unit] { override def call(): Unit = () })
    }
    warmup.foreach(_.get(5, TimeUnit.SECONDS))

    val barrier = new CyclicBarrier(threads)
    try {
      val futures = (1 to threads).map { _ =>
        pool.submit(new Callable[Unit] {
          override def call(): Unit = {
            barrier.await(5, TimeUnit.SECONDS)
            provider.authenticate("alice", "wrong")
          }
        })
      }
      assert(
        firstCallEntered.await(5, TimeUnit.SECONDS),
        "no thread reached the delegate")
      Thread.sleep(200)
      release.countDown()
      // Each future fails: Future.get wraps the AuthenticationException in
      // java.util.concurrent.ExecutionException, regardless of whether this thread was
      // leader or waiter.
      futures.foreach { f =>
        val ex = intercept[java.util.concurrent.ExecutionException](f.get(5, TimeUnit.SECONDS))
        assert(ex.getCause.isInstanceOf[AuthenticationException])
      }
    } finally {
      pool.shutdownNow()
    }

    assert(callCount.get() === 1, "coalescing must collapse to exactly one delegate call")
    assert(AuthenticationProviderFactory.ldapAuthCacheMiss === 8L)
    assert(AuthenticationProviderFactory.ldapAuthCacheHit === 0L)
    assert(AuthenticationProviderFactory.ldapAuthSuccess === 0L)

    // Total failures across all classification buckets must be 8 -- one per request.
    // Bucket attribution is implementation-defined: the leader's stub recorded one
    // invalid_credentials directly, and the waiter-side classifier maps the unwrapped
    // AuthenticationException (no cause chain attached) to whatever its message-based
    // fallback dictates. We pin the total, not the split, because the test exists to
    // catch undercount regressions, not to overspecify classifier behaviour.
    val totalFailures =
      AuthenticationProviderFactory.ldapAuthFailureInvalidCredentials +
        AuthenticationProviderFactory.ldapAuthFailureInvalidInput +
        AuthenticationProviderFactory.ldapAuthFailureInfrastructure
    val invCreds = AuthenticationProviderFactory.ldapAuthFailureInvalidCredentials
    val invInput = AuthenticationProviderFactory.ldapAuthFailureInvalidInput
    val infra = AuthenticationProviderFactory.ldapAuthFailureInfrastructure
    assert(
      totalFailures === 8L,
      s"every request must contribute one failure increment; got total=$totalFailures " +
        s"(invalid_credentials=$invCreds, invalid_input=$invInput, infrastructure=$infra)")
  }

  test("server and http LDAP scopes share a single cache instance") {
    // AuthenticationProviderFactory builds independent CachingLdapAuthenticationProvider
    // instances for the "server" and "http" auth scopes. Both must consult the same
    // backing cache, otherwise a user who authenticates over Thrift then HTTP re-pays
    // the LDAP round-trip within the TTL window.
    val callCount = new AtomicInteger(0)
    val delegate1 = makeDelegate { (_, _) => callCount.incrementAndGet() }
    val delegate2 = makeDelegate { (_, _) => callCount.incrementAndGet() }

    val conf = new KyuubiConf()
    val serverScope = new CachingLdapAuthenticationProvider(delegate1, conf)
    val httpScope = new CachingLdapAuthenticationProvider(delegate2, conf)

    // First auth via "server" scope populates the shared cache.
    serverScope.authenticate("alice", "secret")
    assert(callCount.get() === 1, "first authentication must hit the delegate")

    // Same credentials via "http" scope must hit the cache and skip the delegate.
    httpScope.authenticate("alice", "secret")
    assert(
      callCount.get() === 1,
      "http scope must reuse the cache entry populated by the server scope")

    // The shared cache has exactly one entry, regardless of which wrapper queries.
    assert(serverScope.cacheSize() === 1)
    assert(httpScope.cacheSize() === 1)
  }

  test("cache miss with delegate failure: cache.miss increments, no success or failure") {
    // The wrapper records cache.miss and rethrows. The delegate (stub here, impl in
    // production) is responsible for recording success/failure -- the wrapper does not.
    val delegate = makeDelegate { (_, _) => throw new AuthenticationException("nope") }
    val provider = new CachingLdapAuthenticationProvider(delegate, new KyuubiConf())

    intercept[AuthenticationException](provider.authenticate("alice", "secret"))
    assert(AuthenticationProviderFactory.ldapAuthCacheMiss === 1L)
    assert(AuthenticationProviderFactory.ldapAuthCacheHit === 0L)
    assert(AuthenticationProviderFactory.ldapAuthSuccess === 0L)
    // Failure counters are owned by LdapAuthenticationProviderImpl, not the wrapper.
    assert(AuthenticationProviderFactory.ldapAuthFailureInvalidCredentials === 0L)
    assert(AuthenticationProviderFactory.ldapAuthFailureInvalidInput === 0L)
    assert(AuthenticationProviderFactory.ldapAuthFailureInfrastructure === 0L)
  }
}
