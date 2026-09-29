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

import com.unboundid.ldap.sdk.{Attribute, Entry}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.WithLdapServer

/**
 * Integration tests for [[UnboundIdDirSearch]] against an in-process LDAP server.
 *
 * Each test gets its own fresh connection to avoid state leakage between test cases.
 */
class UnboundIdDirSearchSuite extends KyuubiFunSuite with WithLdapServer {

  private var conf: KyuubiConf = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    conf = new KyuubiConf()
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_BASE_DN, "ou=users")
  }

  private def openSearch(): UnboundIdDirSearch = {
    val conn = ldapServer.getConnection
    new UnboundIdDirSearch(conf, conn, poolOpt = None)
  }

  /**
   * Ensures the shared in-memory directory has a real searchable entry for [[ldapUser]].
   * [[WithLdapServer]] only registers bind credentials via addAdditionalBindCredentials,
   * which lets BIND succeed but does not create any entry that search can return.
   * Tests that drive findUserDn / findDnByPattern need the entry present. Idempotent.
   */
  private def ensureUserEntry(): Unit = {
    if (ldapServer.getEntry("ou=users") == null) {
      ldapServer.add(new Entry(
        "ou=users",
        new Attribute("objectClass", "top", "organizationalUnit"),
        new Attribute("ou", "users")))
    }
    val userDn = s"uid=$ldapUser,ou=users"
    if (ldapServer.getEntry(userDn) == null) {
      ldapServer.add(new Entry(
        userDn,
        new Attribute("objectClass", "top", "person", "inetOrgPerson"),
        new Attribute("uid", ldapUser),
        new Attribute("cn", ldapUser),
        new Attribute("sn", ldapUser),
        new Attribute("userPassword", ldapUserPasswd)))
    }
  }

  test("findUserDn returns null when user not found") {
    val search = openSearch()
    try {
      assert(search.findUserDn("nosuchuser") === null)
    } finally {
      search.close()
    }
  }

  test("findUserDn with bind credentials finds configured user") {
    // Requires a real directory entry: WithLdapServer.beforeAll only registers bind
    // credentials which are not searchable. findDnByPattern runs a filter search so
    // it needs an actual entry under ou=users to return a non-empty result.
    ensureUserEntry()
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN, s"uid=%s,ou=users")
    val search = openSearch()
    try {
      val dn = search.findUserDn(ldapUser)
      assert(dn === s"uid=$ldapUser,ou=users")
    } finally {
      search.close()
    }
  }

  test("executeCustomQuery returns only the configured attribute, not all attributes") {
    // Regression for the attribute over-fetch: searches must request only the attributes the
    // caller needs. customQuery requests the group-membership attribute; previously the empty
    // attribute list was sent to the server as "*" (all attributes), so executeCustomQuery
    // returned every attribute value (e.g. sn) instead of just the configured one.
    ensureUserEntry()
    val attrUserDn = "uid=attrtest,ou=users"
    if (ldapServer.getEntry(attrUserDn) == null) {
      ldapServer.add(new Entry(
        attrUserDn,
        new Attribute("objectClass", "top", "person", "inetOrgPerson"),
        new Attribute("uid", "attrtest"),
        new Attribute("cn", "AttrTest Common Name"),
        new Attribute("sn", "AttrTestSurname")))
    }
    // customQuery returns the group-membership attribute; point it at cn for this entry.
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_GROUP_MEMBERSHIP_KEY, "cn")
    val search = openSearch()
    try {
      val results = search.executeCustomQuery("(uid=attrtest)").toSeq
      assert(results.contains(attrUserDn), s"DN should be returned; got: $results")
      assert(results.contains("AttrTest Common Name"), s"cn should be returned; got: $results")
      assert(
        !results.contains("AttrTestSurname"),
        s"sn was not requested and must not be returned; got: $results")
    } finally {
      search.close()
    }
  }

  test("close on ephemeral connection (poolOpt = None) closes the connection") {
    val conn = ldapServer.getConnection
    val search = new UnboundIdDirSearch(conf, conn, poolOpt = None)
    search.close()
    assert(!conn.isConnected)
  }

  test("close on pooled connection releases back to pool") {
    val conf2 = new KyuubiConf()
    conf2.set(KyuubiConf.AUTHENTICATION_LDAP_URL, ldapUrl)
    conf2.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_USER, s"uid=$ldapUser,ou=users")
    conf2.set(KyuubiConf.AUTHENTICATION_LDAP_BIND_PASSWORD, ldapUserPasswd)
    val pool = new UnboundIdConnectionPool(conf2)
    try {
      val conn = pool.checkoutConnection()
      val search = new UnboundIdDirSearch(conf, conn, poolOpt = Some(pool))
      // Should release healthy connection back to pool, not close it
      search.close()
      // Pool should still be functional -- checkout a second connection
      val conn2 = pool.checkoutConnection()
      assert(conn2.isConnected)
      pool.releaseConnection(conn2)
    } finally {
      pool.close()
    }
  }
}
