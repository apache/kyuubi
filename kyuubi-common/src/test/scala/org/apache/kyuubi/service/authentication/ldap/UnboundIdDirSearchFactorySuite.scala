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

import java.net.ServerSocket
import javax.security.sasl.AuthenticationException

import com.unboundid.ldap.listener.{InMemoryDirectoryServer, InMemoryDirectoryServerConfig}
import com.unboundid.ldap.sdk.{Attribute, Entry}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.{AuthenticationProviderFactory, LdapAuthenticationProviderImpl, PasswdAuthenticationProvider, WithLdapServer}

/**
 * Integration tests for [[UnboundIdDirSearchFactory]] against an in-process LDAP server.
 *
 * Tests the two code paths:
 *  - Bind-user path: borrows connection from pool
 *  - End-user path: ephemeral BIND with failover across multiple URLs
 */
class UnboundIdDirSearchFactorySuite extends KyuubiFunSuite with WithLdapServer {

  private var conf: KyuubiConf = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    conf = new KyuubiConf()
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_URL, ldapUrl)
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_BASE_DN, "ou=users")
  }

  override protected def afterEach(): Unit = {
    // Reset the AuthenticationProviderFactory singleton state (auth counters, installed
    // metrics recorder, cached LDAP providers, owned pool) so tests that drive the
    // recorder cannot leak observations into subsequent tests.
    try AuthenticationProviderFactory.close()
    finally super.afterEach()
  }

  test("getInstance returns DirSearch for valid end-user credentials") {
    val factory = new UnboundIdDirSearchFactory(conf, poolOpt = None)
    val bindDn = s"uid=$ldapUser,ou=users"
    val search = factory.getInstance(conf, bindDn, ldapUserPasswd)
    try {
      assert(search != null)
    } finally {
      search.close()
    }
  }

  test("getInstance throws AuthenticationException for invalid credentials (LDAP 49)") {
    val factory = new UnboundIdDirSearchFactory(conf, poolOpt = None)
    val bindDn = s"uid=$ldapUser,ou=users"
    intercept[AuthenticationException] {
      factory.getInstance(conf, bindDn, "wrong-password")
    }
  }

  test("invalid credentials do not failover to second server") {
    // Start a second in-memory server
    val config2 = new InMemoryDirectoryServerConfig("ou=users")
    config2.setSchema(null)
    config2.addAdditionalBindCredentials(s"uid=$ldapUser,ou=users", ldapUserPasswd)
    val server2 = new InMemoryDirectoryServer(config2)
    server2.startListening()
    try {
      val twoServerUrl = s"$ldapUrl ldap://localhost:${server2.getListenPort}"
      conf.set(KyuubiConf.AUTHENTICATION_LDAP_URL, twoServerUrl)
      val factory = new UnboundIdDirSearchFactory(conf, poolOpt = None)

      // With wrong password, must fail immediately without trying server2
      val ex = intercept[AuthenticationException] {
        factory.getInstance(conf, s"uid=$ldapUser,ou=users", "wrong-password")
      }
      assert(ex.getMessage.contains("InvalidCredentials"))
    } finally {
      server2.close()
    }
  }

  test("connection failure on first server falls over to second server") {
    // Start a second in-memory server; point first URL at a port that will refuse connections
    val config2 = new InMemoryDirectoryServerConfig("ou=users")
    config2.setSchema(null)
    config2.addAdditionalBindCredentials(s"uid=$ldapUser,ou=users", ldapUserPasswd)
    val server2 = new InMemoryDirectoryServer(config2)
    server2.startListening()
    try {
      // Allocate-and-close a port to get one that's guaranteed-refused on this host.
      // Using a hardcoded port like 19999 produces a misleading green if a developer has
      // an unrelated service bound to it; ServerSocket(0)+close is deterministic.
      val refusedPort = {
        val s = new ServerSocket(0)
        try s.getLocalPort
        finally s.close()
      }
      val twoServerUrl =
        s"ldap://localhost:$refusedPort ldap://localhost:${server2.getListenPort}"
      conf.set(KyuubiConf.AUTHENTICATION_LDAP_URL, twoServerUrl)
      val factory = new UnboundIdDirSearchFactory(conf, poolOpt = None)
      val search = factory.getInstance(conf, s"uid=$ldapUser,ou=users", ldapUserPasswd)
      try {
        assert(search != null)
      } finally {
        search.close()
      }
    } finally {
      server2.close()
    }
  }

  test("bind-user path borrows from pool") {
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_USER, s"uid=$ldapUser,ou=users")
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_PASSWORD, ldapUserPasswd)
    val pool = new UnboundIdConnectionPool(conf)
    try {
      val factory = new UnboundIdDirSearchFactory(conf, poolOpt = Some(pool))
      val search = factory.getInstance(conf, s"uid=$ldapUser,ou=users", ldapUserPasswd)
      try {
        assert(search != null)
      } finally {
        search.close()
      }
    } finally {
      pool.close()
    }
  }

  test("counter attribution across cache wrapper + impl: miss then hit") {
    // Joint integration of CachingLdapAuthenticationProvider + LdapAuthenticationProviderImpl
    // against the in-memory directory. Locks in the per-layer responsibilities:
    //   - first call (cache miss): wrapper bumps cache.miss; impl runs and bumps success
    //   - second call (cache hit): wrapper bumps cache.hit AND success; impl is not invoked
    // Total over the two calls: cache.miss=1, cache.hit=1, success=2, failures=0.
    // A future refactor that double-counts success (e.g. wrapper also bumps it on miss)
    // would push success to 3 and fail this test.
    val recorder = new CapturingLdapAuthRecorder
    AuthenticationProviderFactory.setLdapAuthMetricsRecorder(recorder)

    val factory = new UnboundIdDirSearchFactory(conf, poolOpt = None)
    val impl = new LdapAuthenticationProviderImpl(conf, factory)
    val cached: PasswdAuthenticationProvider =
      new CachingLdapAuthenticationProvider(impl, conf)

    val bindDn = s"uid=$ldapUser,ou=users"

    cached.authenticate(bindDn, ldapUserPasswd) // miss -> impl runs -> impl records success
    assert(recorder.cacheMisses.get === 1)
    assert(recorder.cacheHits.get === 0)
    assert(recorder.successes.get === 1, "impl must record success on cache-miss success")

    cached.authenticate(bindDn, ldapUserPasswd) // hit -> wrapper records success; impl skipped
    assert(recorder.cacheMisses.get === 1, "second call must not bump cache.miss")
    assert(recorder.cacheHits.get === 1)
    assert(
      recorder.successes.get === 2,
      "wrapper must bump success on cache hit; total success = miss-success + hit-success")

    // Failure buckets must remain at zero -- this path is two clean successes.
    assert(recorder.invalidCreds.get === 0)
    assert(recorder.invalidInput.get === 0)
    assert(recorder.infrastructure.get === 0)
  }

  test("end-to-end failure metrics: bad credentials classify as invalid_credentials") {
    // Drives the full production stack -- LdapAuthenticationProviderImpl ->
    // UnboundIdDirSearchFactory -> ephemeralBind -> LdapAuthFailureClassifier -> recorder
    // -- against the in-memory directory, with a wrong password, and asserts the
    // recorder observed exactly one `invalid_credentials` event. Guards against future
    // regressions in the classifier's cause-chain dispatch path (e.g. the
    // INVALID_CREDENTIALS LDAPException cause check) that the cache-suite tests, which
    // use a stub delegate, would not catch.
    val recorder = new CapturingLdapAuthRecorder
    AuthenticationProviderFactory.setLdapAuthMetricsRecorder(recorder)

    val factory = new UnboundIdDirSearchFactory(conf, poolOpt = None)
    val provider = new LdapAuthenticationProviderImpl(conf, factory)

    intercept[AuthenticationException] {
      provider.authenticate(ldapUser, "wrong-password")
    }

    // The classifier should map the chained LDAPException(INVALID_CREDENTIALS) cause to
    // `invalid_credentials`. Asserting zero on the other buckets pins the dispatch to
    // exactly one bucket -- a regression that maps everything to `infrastructure` would
    // increment infrastructure and leave invalidCreds at 0.
    assert(
      recorder.invalidCreds.get === 1,
      s"expected exactly one invalid_credentials event, got " +
        s"invalidCreds=${recorder.invalidCreds.get}, " +
        s"infrastructure=${recorder.infrastructure.get}, " +
        s"invalidInput=${recorder.invalidInput.get}")
    assert(recorder.successes.get === 0)
    assert(recorder.infrastructure.get === 0)
    assert(recorder.invalidInput.get === 0)
  }

  test("executeCustomQuery output parity: JNDI vs UnboundID against same server") {
    // F8 from the review: confirm that the new UnboundID-backed executeCustomQuery
    // produces the same set of strings the legacy JNDI implementation does for the same
    // query against the same data. Both flatten the search results into a single array
    // of [DN, attribute_value, attribute_value, ..., DN, ...]; LDAP gives no guarantee on
    // attribute iteration order, so this asserts SET equality rather than exact sequence.
    // The in-memory server's base DN exists in its config but is not auto-populated as
    // an entry, so we add it (idempotently, since other test invocations may have done
    // so) before any child entry.
    if (ldapServer.getEntry("ou=users") == null) {
      ldapServer.add(new Entry(
        "ou=users",
        new Attribute("objectClass", "top", "organizationalUnit"),
        new Attribute("ou", "users")))
    }
    val parityDn = "uid=parityuser,ou=users"
    val parityEntry = new Entry(
      parityDn,
      new Attribute("objectClass", "top", "person", "inetOrgPerson"),
      new Attribute("uid", "parityuser"),
      new Attribute("cn", "Parity User"),
      new Attribute("sn", "User"),
      // QueryFactory.customQuery requests only the `groupMembershipAttr` (default
      // "member"). Multi-valued member here exercises the inner flatMap over attribute
      // values, and matches how executeCustomQuery is actually used in production
      // (custom-query group membership lookups).
      new Attribute("member", "cn=group1,ou=groups", "cn=group2,ou=groups"))
    ldapServer.add(parityEntry)

    val query = "(uid=parityuser)"
    val bindDn = s"uid=$ldapUser,ou=users"

    val jndiFactory = new LdapSearchFactory
    val jndiSearch = jndiFactory.getInstance(conf, bindDn, ldapUserPasswd)
    val jndiResult =
      try jndiSearch.executeCustomQuery(query)
      finally jndiSearch.close()

    val unboundFactory = new UnboundIdDirSearchFactory(conf, poolOpt = None)
    val unboundSearch = unboundFactory.getInstance(conf, bindDn, ldapUserPasswd)
    val unboundResult =
      try unboundSearch.executeCustomQuery(query)
      finally unboundSearch.close()

    // Both must observe the matching DN and both values of the multi-valued member attribute.
    assert(jndiResult.contains(parityDn))
    assert(unboundResult.contains(parityDn))
    assert(jndiResult.contains("cn=group1,ou=groups"))
    assert(unboundResult.contains("cn=group1,ou=groups"))
    assert(jndiResult.contains("cn=group2,ou=groups"))
    assert(unboundResult.contains("cn=group2,ou=groups"))

    // Set equality is the meaningful invariant -- the JNDI and UnboundID iteration
    // orders over attributes are implementation-defined and need not match. A divergence
    // in the SET would indicate one path is dropping or duplicating data.
    val jndiSet = jndiResult.toSet
    val unboundSet = unboundResult.toSet
    assert(
      jndiSet === unboundSet,
      s"executeCustomQuery diverged.\nOnly in JNDI: ${jndiSet -- unboundSet}\n" +
        s"Only in UnboundID: ${unboundSet -- jndiSet}")

    // Length parity guards against one path silently de-duplicating.
    assert(
      jndiResult.length === unboundResult.length,
      s"length mismatch -- JNDI=${jndiResult.length} (${jndiResult.toSeq}), " +
        s"UnboundID=${unboundResult.length} (${unboundResult.toSeq})")
  }

  test("pool initialises with first URL refused and second URL healthy") {
    // Coverage gap: UnboundIdDirSearchFactorySuite already exercised ephemeral-path
    // failover (line 97), but the pooled path -- which is the main new code -- had no
    // failover-on-init test. Verifies that UnboundIdConnectionPool boots cleanly when the
    // first configured server refuses connections, because FailoverServerSet skips ahead
    // to the next URL during pool warmup rather than failing the whole pool construction.
    val refusedPort = {
      val s = new ServerSocket(0)
      try s.getLocalPort
      finally s.close()
    }
    val twoServerUrl = s"ldap://localhost:$refusedPort $ldapUrl"
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_URL, twoServerUrl)
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_USER, s"uid=$ldapUser,ou=users")
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_PASSWORD, ldapUserPasswd)
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_POOL_MIN_CONNECTIONS, 1)
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_POOL_MAX_CONNECTIONS, 2)

    val pool = new UnboundIdConnectionPool(conf)
    try {
      val factory = new UnboundIdDirSearchFactory(conf, poolOpt = Some(pool))
      val search = factory.getInstance(conf, s"uid=$ldapUser,ou=users", ldapUserPasswd)
      try assert(search != null)
      finally search.close()
      // Pool must have at least one healthy connection -- failover succeeded.
      assert(
        pool.numAvailableConnections >= 1,
        s"expected >=1 healthy connection after pool boot with refused first URL, got " +
          s"${pool.numAvailableConnections}")
    } finally {
      pool.close()
    }
  }

  /**
   * Ensures the shared in-memory directory has a real searchable entry for `ldapUser`.
   * WithLdapServer only registers the credentials via addAdditionalBindCredentials, which
   * lets BIND succeed but leaves no entry that findUserDn / search can return. Tests that
   * drive the full LdapAuthenticationProviderImpl bind-user flow (impl.authenticate ->
   * findUserDn -> userSearch) need the entry present. Idempotent so multiple tests can call it.
   */
  private def ensureUserEntry(server: InMemoryDirectoryServer = ldapServer): Unit = {
    if (server.getEntry("ou=users") == null) {
      server.add(new Entry(
        "ou=users",
        new Attribute("objectClass", "top", "organizationalUnit"),
        new Attribute("ou", "users")))
    }
    val userDn = s"uid=$ldapUser,ou=users"
    if (server.getEntry(userDn) == null) {
      server.add(new Entry(
        userDn,
        new Attribute("objectClass", "top", "person", "inetOrgPerson"),
        new Attribute("uid", ldapUser),
        new Attribute("cn", ldapUser),
        new Attribute("sn", ldapUser),
        new Attribute("userPassword", ldapUserPasswd)))
    }
  }

  test("pool retries mid-request when active connection breaks server-side") {
    // Verifies the bounded retry path added to UnboundIdDirSearch.searchBaseWithRetry:
    // when a pooled connection dies mid-SEARCH (server kill, dropped socket), the in-flight
    // request is not surfaced as a failure -- the broken connection is discarded and a
    // fresh one (from a healthy peer in FailoverServerSet) is used to complete the search.
    // This closes the JNDI gap where failover applied only at initial-context creation.
    //
    // Strategy: two in-memory servers; pool configured with [server1, server2] in failover
    // order. Warm the pool against server1, kill server1, then drive an auth. The first
    // checkout returns a dead-to-server1 connection (we did not enable invokeOnCheckout
    // health checks because they add per-request latency), so the SEARCH fails, the retry
    // discards it, and FailoverServerSet advances the next checkout to server2.
    val server1Config = new InMemoryDirectoryServerConfig("ou=users")
    server1Config.setSchema(null)
    server1Config.addAdditionalBindCredentials(s"uid=$ldapUser,ou=users", ldapUserPasswd)
    val server1 = new InMemoryDirectoryServer(server1Config)
    server1.startListening()
    ensureUserEntry(server1)
    val server2Config = new InMemoryDirectoryServerConfig("ou=users")
    server2Config.setSchema(null)
    server2Config.addAdditionalBindCredentials(s"uid=$ldapUser,ou=users", ldapUserPasswd)
    val server2 = new InMemoryDirectoryServer(server2Config)
    server2.startListening()
    ensureUserEntry(server2)
    try {
      val twoServerUrl =
        s"ldap://localhost:${server1.getListenPort} ldap://localhost:${server2.getListenPort}"
      conf.set(KyuubiConf.AUTHENTICATION_LDAP_URL, twoServerUrl)
      conf.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_USER, s"uid=$ldapUser,ou=users")
      conf.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_PASSWORD, ldapUserPasswd)
      conf.set(KyuubiConf.AUTHENTICATION_LDAP_POOL_MIN_CONNECTIONS, 1)
      conf.set(KyuubiConf.AUTHENTICATION_LDAP_POOL_MAX_CONNECTIONS, 2)

      val pool = new UnboundIdConnectionPool(conf)
      try {
        val factory = new UnboundIdDirSearchFactory(conf, poolOpt = Some(pool))

        // Warm: borrow + release one connection so the pool holds an idle (now-doomed)
        // connection to server1.
        val warm = factory.getInstance(conf, s"uid=$ldapUser,ou=users", ldapUserPasswd)
        warm.close()

        // Kill server1. All idle pool connections are now broken at the socket level.
        server1.shutDown(true)

        // Drive an auth that triggers a SEARCH (findUserDn). The first SearchRequest fails
        // with a connection-level result code; the retry path must discard, re-checkout,
        // fail over to server2, and complete successfully.
        val search = factory.getInstance(conf, s"uid=$ldapUser,ou=users", ldapUserPasswd)
        try {
          val dn = search.findUserDn(ldapUser)
          assert(
            dn != null,
            "mid-request failover did not recover the search -- expected findUserDn to " +
              "succeed against server2 after server1 was killed")
        } finally {
          search.close()
        }
      } finally {
        pool.close()
      }
    } finally {
      try server1.close()
      catch { case _: Throwable => () }
      server2.close()
    }
  }

  test("bind-user flow closes every DirSearch it opens (no end-user verification leak)") {
    // Regression guard for the second-DirSearch leak previously present in
    // LdapAuthenticationProviderImpl.doAuthenticate: createDirSearch(userDn, password)
    // returned a DirSearch wrapping an ephemeral LDAPConnection whose close() was the only
    // path that freed the socket -- and the returned value was discarded, not closed.
    //
    // Strategy: wrap the real factory with one that hands out tracking decorators counting
    // open() and close() calls. If the fix is in place, opens == closes for any number of
    // auths. The decorator is local to this test, so the assertion is exact -- no flakiness
    // from external FD accounting or directory-server stats.
    ensureUserEntry()
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_USER, s"uid=$ldapUser,ou=users")
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_PASSWORD, ldapUserPasswd)
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_POOL_MIN_CONNECTIONS, 1)
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_POOL_MAX_CONNECTIONS, 2)

    val pool = new UnboundIdConnectionPool(conf)
    try {
      val opens = new java.util.concurrent.atomic.AtomicInteger(0)
      val closes = new java.util.concurrent.atomic.AtomicInteger(0)
      val realFactory = new UnboundIdDirSearchFactory(conf, poolOpt = Some(pool))
      val countingFactory: DirSearchFactory = new DirSearchFactory {
        override def getInstance(
            conf: KyuubiConf,
            principal: String,
            password: String): DirSearch = {
          val inner = realFactory.getInstance(conf, principal, password)
          opens.incrementAndGet()
          new DirSearch {
            override def findUserDn(user: String): String = inner.findUserDn(user)
            override def findGroupDn(group: String): String = inner.findGroupDn(group)
            override def isUserMemberOfGroup(user: String, groupDn: String): Boolean =
              inner.isUserMemberOfGroup(user, groupDn)
            override def findGroupsForUser(userDn: String): Array[String] =
              inner.findGroupsForUser(userDn)
            override def executeCustomQuery(query: String): Array[String] =
              inner.executeCustomQuery(query)
            override def close(): Unit = {
              try inner.close()
              finally { closes.incrementAndGet(); () }
            }
          }
        }
      }
      val impl = new LdapAuthenticationProviderImpl(conf, countingFactory)

      val nAuths = 20
      (1 to nAuths).foreach { _ => impl.authenticate(ldapUser, ldapUserPasswd) }

      // The bind-user flow opens TWO DirSearches per auth: one for the bind-user pool
      // checkout, one for the end-user credential-verification ephemeral bind. Both must
      // be closed. opens/closes are an exact pairing -- not a bound.
      assert(
        opens.get === 2 * nAuths,
        s"expected ${2 * nAuths} DirSearch opens for $nAuths bind-user auths, " +
          s"got ${opens.get}")
      assert(
        closes.get === opens.get,
        s"DirSearch leak detected: ${opens.get} opened, ${closes.get} closed " +
          s"(diff ${opens.get - closes.get}). The end-user credential-verification " +
          s"DirSearch is the historical leak site.")
    } finally {
      pool.close()
    }
  }

  test("pool reuse: burst of N auths opens <= max.connections pool sockets") {
    // Asserts the central pool-reuse claim. With a bind user configured and pool capped at
    // poolMax, a burst of nAuths (>> poolMax) must NOT open nAuths pool connections -- the
    // pool must reuse warm connections across requests. Each iteration also opens exactly
    // one ephemeral end-user verification connection that is closed inside the auth path
    // (verified separately by the leak-detection test above).
    ensureUserEntry()
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_USER, s"uid=$ldapUser,ou=users")
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_PASSWORD, ldapUserPasswd)
    val poolMin = 1
    val poolMax = 3
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_POOL_MIN_CONNECTIONS, poolMin)
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_POOL_MAX_CONNECTIONS, poolMax)

    val pool = new UnboundIdConnectionPool(conf)
    try {
      val factory = new UnboundIdDirSearchFactory(conf, poolOpt = Some(pool))
      val impl = new LdapAuthenticationProviderImpl(conf, factory)
      val nAuths = 50
      (1 to nAuths).foreach { _ => impl.authenticate(ldapUser, ldapUserPasswd) }

      // numSuccessfulCheckouts grows with each pool borrow; the count of distinct
      // connections actually created is bounded by poolMax. Health-check replacements can
      // add a small slack but not a per-request multiplier.
      assert(
        pool.numAvailableConnections <= poolMax,
        s"pool exceeded max -- numAvailable=${pool.numAvailableConnections} > $poolMax")
      assert(
        pool.numSuccessfulCheckouts >= nAuths,
        s"expected >=$nAuths successful checkouts, got ${pool.numSuccessfulCheckouts}")
    } finally {
      pool.close()
    }
  }

  test("retry does not double-release pool connection when replacement checkout fails") {
    // Regression guard for the retry edge case: when searchBaseWithRetry releases the
    // broken connection via releaseConnectionAfterException and the immediately-following
    // pool.checkoutConnection() also fails (e.g. pool exhausted, every server down), the
    // DirSearch.close() that fires as the exception propagates must NOT release the same
    // connection a second time. Tracked via UnboundIdDirSearch.connectionAlreadyReleased.
    //
    // Strategy: use a counting pool subclass. Borrow a connection while the server is up,
    // kill the server (so a search on that connection will fail), arm the pool to refuse
    // the replacement checkout, then drive a search. We assert that exactly ONE
    // releaseConnectionAfterException call lands at the pool -- the retry path's release.
    // Without the fix, close() would release the same connection a second time and the
    // counter would tick to two.
    import java.util.concurrent.atomic.AtomicInteger
    import com.unboundid.ldap.sdk.{LDAPConnection, LDAPException => SdkLdapEx, ResultCode}

    val standaloneConfig = new InMemoryDirectoryServerConfig("ou=users")
    standaloneConfig.setSchema(null)
    standaloneConfig.addAdditionalBindCredentials(s"uid=$ldapUser,ou=users", ldapUserPasswd)
    val standalone = new InMemoryDirectoryServer(standaloneConfig)
    standalone.startListening()
    ensureUserEntry(standalone)
    try {
      val isolatedConf = new KyuubiConf()
      isolatedConf.set(
        KyuubiConf.AUTHENTICATION_LDAP_URL,
        s"ldap://localhost:${standalone.getListenPort}")
      isolatedConf.set(KyuubiConf.AUTHENTICATION_LDAP_BASE_DN, "ou=users")
      isolatedConf.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_USER, s"uid=$ldapUser,ou=users")
      isolatedConf.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_PASSWORD, ldapUserPasswd)
      isolatedConf.set(KyuubiConf.AUTHENTICATION_LDAP_POOL_MIN_CONNECTIONS, 1)
      isolatedConf.set(KyuubiConf.AUTHENTICATION_LDAP_POOL_MAX_CONNECTIONS, 1)

      val releaseAfterExceptionCount = new AtomicInteger(0)
      val releaseHealthyCount = new AtomicInteger(0)
      val refuseCheckout = new AtomicInteger(0)
      val countingPool = new UnboundIdConnectionPool(isolatedConf) {
        override def checkoutConnection(): LDAPConnection = {
          if (refuseCheckout.get() > 0) {
            // Simulate "no replacement available" (pool exhausted, all servers down).
            throw new SdkLdapEx(ResultCode.CONNECT_ERROR, "test-induced checkout failure")
          }
          super.checkoutConnection()
        }
        override def releaseConnection(conn: LDAPConnection): Unit = {
          releaseHealthyCount.incrementAndGet()
          super.releaseConnection(conn)
        }
        override def releaseConnectionAfterException(
            conn: LDAPConnection,
            e: SdkLdapEx): Unit = {
          releaseAfterExceptionCount.incrementAndGet()
          super.releaseConnectionAfterException(conn, e)
        }
      }
      try {
        // Borrow a connection while the server is still up so the search request actually
        // travels onto a real (now-doomed) socket.
        val borrowed = countingPool.checkoutConnection()
        // Kill the server. The borrowed connection is now broken at the socket level;
        // its next operation will throw an LDAPException with !isConnectionUsable.
        standalone.shutDown(true)
        // Arm the pool so the retry path's replacement checkout fails -- forcing the
        // exception-propagation branch of searchBaseWithRetry to fire.
        refuseCheckout.incrementAndGet()

        val search = new UnboundIdDirSearch(isolatedConf, borrowed, Some(countingPool))
        intercept[javax.naming.NamingException] {
          search.findUserDn(ldapUser)
        }
        // close() runs after the failed retry path. The fix means it must observe
        // connectionAlreadyReleased=true and skip the release.
        search.close()

        // Exactly ONE release-with-exception: the one from inside the retry path.
        // Two would indicate close() did a duplicate release on an already-returned
        // connection.
        assert(
          releaseAfterExceptionCount.get === 1,
          s"expected exactly 1 releaseConnectionAfterException call, got " +
            s"${releaseAfterExceptionCount.get}. >1 implies the retry path released " +
            s"and then close() released the same connection again.")
        // Healthy-release path must not be touched: the connection was never healthy at
        // close() time.
        assert(
          releaseHealthyCount.get === 0,
          s"healthy releaseConnection must not be called when the failed retry leaves " +
            s"the connection already released; got ${releaseHealthyCount.get}.")
      } finally {
        countingPool.close()
      }
    } finally {
      try standalone.close()
      catch { case _: Throwable => () }
    }
  }

  test("checkoutConnection emits warn when FailoverServerSet lands on secondary server") {
    // Verifies the failover-visibility warning added to UnboundIdConnectionPool.checkoutConnection:
    // when the primary server is unreachable and FailoverServerSet quietly advances to a secondary,
    // the checkout must emit a WARN log naming both servers so operators can detect live failover
    // without having to correlate Prometheus defunct-connection counters.
    //
    // Setup: primary URL points at a refused port; secondary URL is the real in-memory server.
    // All pool connections land on the secondary, so every checkout fires the warn.
    import org.apache.logging.log4j.Level
    val refusedPort = {
      val s = new ServerSocket(0)
      try s.getLocalPort
      finally s.close()
    }
    // primary=refused, secondary=real; minConns=1 so the pool warms during construction
    val twoServerUrl = s"ldap://localhost:$refusedPort $ldapUrl"
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_URL, twoServerUrl)
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_USER, s"uid=$ldapUser,ou=users")
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_PASSWORD, ldapUserPasswd)
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_POOL_MIN_CONNECTIONS, 1)
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_POOL_MAX_CONNECTIONS, 2)

    val logAppender = new LogAppender("checkoutConnection failover warn")
    logAppender.setThreshold(Level.WARN)
    withLogAppender(logAppender, loggerNames = Seq(classOf[UnboundIdConnectionPool].getName)) {
      val pool = new UnboundIdConnectionPool(conf)
      try {
        val conn = pool.checkoutConnection()
        pool.releaseConnection(conn)
      } finally {
        pool.close()
      }
    }
    val warnMessages = logAppender.loggingEvents
      .filter(_.getLevel == Level.WARN)
      .map(_.getMessage.getFormattedMessage)
    assert(
      warnMessages.exists(_.contains("failover active")),
      s"expected a WARN containing 'failover active' when all connections land on secondary; " +
        s"got: ${warnMessages.mkString(", ")}")
    assert(
      warnMessages.exists(_.contains(s"localhost:$refusedPort")),
      s"warn message should name the primary (refused) server localhost:$refusedPort; " +
        s"got: ${warnMessages.mkString(", ")}")
  }

  test("malformed username with filter metacharacters classifies as invalid_input") {
    // Regression guard for the Query.QueryBuilder.build IllegalStateException bypass: a
    // username containing LDAP filter metacharacters renders to an unparseable RFC 4515
    // filter (e.g. "(uid=*)(uid=*)"), build() throws ISE, and -- before the fix -- that
    // ISE escaped the auth contract uncaught, skipping both the auth-failure conversion
    // and the metric recorder. The catch in LdapAuthenticationProviderImpl.doAuthenticate
    // now wraps it as AuthenticationException with a prefix the classifier maps to
    // invalid_input.
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_USER, s"uid=$ldapUser,ou=users")
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_PASSWORD, ldapUserPasswd)

    val recorder = new CapturingLdapAuthRecorder
    AuthenticationProviderFactory.setLdapAuthMetricsRecorder(recorder)

    val factory = new UnboundIdDirSearchFactory(conf, poolOpt = None)
    val provider = new LdapAuthenticationProviderImpl(conf, factory)

    val badUser = "*)(uid=*"
    val ex = intercept[AuthenticationException] {
      provider.authenticate(badUser, ldapUserPasswd)
    }
    // The wrapped message must start with the invalid_input prefix recognised by the
    // classifier so the metric lands in the right bucket.
    assert(
      ex.getMessage.startsWith("Error validating LDAP user, invalid filter"),
      s"unexpected exception message: ${ex.getMessage}")
    assert(
      recorder.invalidInput.get === 1,
      s"expected one invalid_input event, got invalidInput=${recorder.invalidInput.get}, " +
        s"infrastructure=${recorder.infrastructure.get}, " +
        s"invalidCreds=${recorder.invalidCreds.get}")
    assert(recorder.invalidCreds.get === 0)
    assert(recorder.infrastructure.get === 0)
    assert(recorder.successes.get === 0)
  }
}
