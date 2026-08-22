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

import java.net.ServerSocket
import javax.security.sasl.AuthenticationException

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf

class AuthenticationProviderFactorySuite extends KyuubiFunSuite {

  import AuthenticationProviderFactory._

  // Reset the singleton ldapProviders map and the underlying UnboundID pool between tests
  // so any LDAP provider built in one test cannot be observed by the next (the singleton
  // is keyed by auth scope, not by KyuubiConf, so different conf in successive tests would
  // otherwise resolve to the cached provider from the first run).
  override def afterEach(): Unit = {
    try AuthenticationProviderFactory.close()
    finally super.afterEach()
  }

  /**
   * Allocates a TCP port, immediately closes the listener, and returns the port number.
   * Subsequent connections to `127.0.0.1:port` are deterministically refused for the
   * lifetime of the test -- no DNS, no timing dependence, no hostname-resolution variance.
   */
  private def refusedLocalPort(): Int = {
    val s = new ServerSocket(0)
    try s.getLocalPort
    finally s.close()
  }

  test("get auth provider") {
    val conf = KyuubiConf()
      // UnboundIdDirSearchFactory checks at construction that kyuubi.authentication.ldap.url
      // is non-empty and parseable as an LDAPURL -- it does NOT open a TCP connection. Using
      // a freshly-allocated-then-closed loopback port gives us a guaranteed-refused TCP
      // target so the failure path is deterministic.
      .set(KyuubiConf.AUTHENTICATION_LDAP_URL, s"ldap://127.0.0.1:${refusedLocalPort()}")
    val p1 = getAuthenticationProvider(AuthMethods.withName("NONE"), conf)
    p1.authenticate(Utils.currentUser, "")
    val p2 = getAuthenticationProvider(AuthMethods.withName("LDAP"), conf)
    val e1 = intercept[AuthenticationException](p2.authenticate("test", "test"))
    // With the pool disabled (the default), the factory builds the legacy JNDI-backed
    // provider. A refused connection surfaces from LdapSearchFactory.getInstance, which
    // wraps the NamingException as an AuthenticationException with this prefix.
    assert(
      e1.getMessage.contains("Error validating LDAP user:"),
      s"unexpected AuthenticationException message: ${e1.getMessage}")
    val e2 = intercept[AuthenticationException](
      AuthenticationProviderFactory.getAuthenticationProvider(null, conf))
    assert(e2.getMessage === "Not a valid authentication method")
  }

  test("close() releases LDAP singletons so subsequent calls rebuild fresh providers") {
    val conf = KyuubiConf()
      // Construction-time URL check parses the value as an LDAPURL; it does not connect.
      // A refused loopback port works fine -- this test exercises provider caching and
      // close(), never calling authenticate().
      .set(KyuubiConf.AUTHENTICATION_LDAP_URL, s"ldap://127.0.0.1:${refusedLocalPort()}")

    // Force the LDAP singleton to be created and cached under the "server" key.
    val p1 = getAuthenticationProvider(AuthMethods.withName("LDAP"), conf)
    val p2 = getAuthenticationProvider(AuthMethods.withName("LDAP"), conf)
    assert(p1 eq p2, "LDAP provider must be cached as a singleton between calls")

    // close() must drop the cached provider and any owned LDAP pool. The next call
    // returns a fresh instance built from the (potentially updated) conf.
    AuthenticationProviderFactory.close()
    val p3 = getAuthenticationProvider(AuthMethods.withName("LDAP"), conf)
    assert(p3 ne p1, "close() should evict the cached LDAP provider")
    // afterEach() runs close() again as belt-and-braces for the next test.
  }

}
