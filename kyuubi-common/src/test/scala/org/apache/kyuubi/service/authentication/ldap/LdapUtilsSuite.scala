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

import javax.naming.ldap.{LdapName, Rdn}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

class LdapUtilsSuite extends KyuubiFunSuite {

  /**
   * Parses a distinguished name and returns the leftmost RDN. Asserting on the parsed value
   * rather than on the escaped text keeps these tests independent of the escape notation.
   */
  private def leftmostRdn(dnString: String): Rdn = {
    val dn = new LdapName(dnString)
    dn.getRdn(dn.size() - 1)
  }

  test("CreateCandidatePrincipalsForUserDn") {
    val conf = new KyuubiConf()
    val userDn = "cn=user1,ou=CORP,dc=mycompany,dc=com"
    val expected = Array(userDn)
    val actual = LdapUtils.createCandidatePrincipals(conf, userDn)
    assert(actual === expected)
  }

  test("CreateCandidatePrincipalsForUserWithDomain") {
    val conf = new KyuubiConf()
    val userWithDomain: String = "user1@mycompany.com"
    val expected = Array(userWithDomain)
    val actual = LdapUtils.createCandidatePrincipals(conf, userWithDomain)
    assert(actual === expected)
  }

  test("CreateCandidatePrincipalsLdapDomain") {
    val conf = new KyuubiConf()
      .set(KyuubiConf.AUTHENTICATION_LDAP_DOMAIN, "mycompany.com")
    val expected = Array("user1@mycompany.com")
    val actual = LdapUtils.createCandidatePrincipals(conf, "user1")
    assert(actual === expected)
  }

  test("CreateCandidatePrincipalsUserPatternsDefaultBaseDn") {
    val conf = new KyuubiConf()
      .set(KyuubiConf.AUTHENTICATION_LDAP_GUID_KEY, "sAMAccountName")
      .set(KyuubiConf.AUTHENTICATION_LDAP_BASE_DN, "dc=mycompany,dc=com")
    val expected = Array("sAMAccountName=user1,dc=mycompany,dc=com")
    val actual = LdapUtils.createCandidatePrincipals(conf, "user1")
    assert(actual === expected)
  }

  test("CreateCandidatePrincipals") {
    val conf = new KyuubiConf()
      .set(KyuubiConf.AUTHENTICATION_LDAP_BASE_DN, "dc=mycompany,dc=com")
      .set(
        KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN,
        "cn=%s,ou=CORP1,dc=mycompany,dc=com:cn=%s,ou=CORP2,dc=mycompany,dc=com")
    val expected = Array(
      "cn=user1,ou=CORP1,dc=mycompany,dc=com",
      "cn=user1,ou=CORP2,dc=mycompany,dc=com")
    val actual = LdapUtils.createCandidatePrincipals(conf, "user1")
    assert(actual.sorted === expected.sorted)
  }

  test("CreateCandidatePrincipalsSubstitutesValueLiterally") {
    val conf = new KyuubiConf()
      .set(KyuubiConf.AUTHENTICATION_LDAP_BASE_DN, "dc=mycompany,dc=com")
      .set(KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN, "cn=%s,ou=CORP,dc=mycompany,dc=com")
    // The name has to survive as one literal cn value: DN metacharacters must not add RDN
    // components, and '$' and '\' must not be taken as regex replacement metacharacters.
    // Names containing '=' take the isDn early return and never reach the pattern.
    Seq("a,b", "a+b", "a\"b", "a\\b", "#a", "a$b", "a$0b", "a\\", "\\").foreach { user =>
      val candidates = LdapUtils.createCandidatePrincipals(conf, user)
      assert(candidates.length === 1)
      assert(leftmostRdn(candidates.head).getValue === user)
    }
  }

  test("ExtractFirstRdn") {
    val dn = "cn=user1,ou=CORP1,dc=mycompany,dc=com"
    val expected = "cn=user1"
    val actual = LdapUtils.extractFirstRdn(dn)
    assert(actual === expected)
  }

  test("ExtractFirstRdnWithoutComma") {
    assert(LdapUtils.extractFirstRdn("cn=user1") === "cn=user1")
    assert(LdapUtils.extractFirstRdn("user1") === "user1")
  }

  test("ExtractBaseDn") {
    val dn: String = "cn=user1,ou=CORP1,dc=mycompany,dc=com"
    val expected = "ou=CORP1,dc=mycompany,dc=com"
    val actual = LdapUtils.extractBaseDn(dn)
    assert(actual === expected)
  }

  test("ExtractBaseDnNegative") {
    val dn: String = "cn=user1"
    assert(LdapUtils.extractBaseDn(dn) === null)
  }

  test("ExtractUserNameNegative") {
    // a DN with no comma, or with a comma before the first '=', must not throw
    assert(LdapUtils.extractUserName("cn=user1") === "user1")
    assert(LdapUtils.extractUserName("a,b=c") === "c")
  }

  test("EscapeLDAPSearchFilter") {
    assert(LdapUtils.escapeLDAPSearchFilter("") === "")
    assert(LdapUtils.escapeLDAPSearchFilter("user1") === "user1")
    assert(LdapUtils.escapeLDAPSearchFilter("use*r") === "use\\2ar")
    assert(LdapUtils.escapeLDAPSearchFilter("a(b)c") === "a\\28b\\29c")
    assert(LdapUtils.escapeLDAPSearchFilter("a\\b") === "a\\5cb")
    assert(LdapUtils.escapeLDAPSearchFilter("a\u0000b") === "a\\00b")
    assert(LdapUtils.escapeLDAPSearchFilter("*)(objectClass=*") ===
      "\\2a\\29\\28objectClass=\\2a")
  }

  test("EscapeLdapDnValue") {
    assert(LdapUtils.escapeLdapDnValue("") === "")
    assert(LdapUtils.escapeLdapDnValue("user1") === "user1")
    assert(LdapUtils.escapeLdapDnValue("a$b") === "a$b")
    // each of these would otherwise act as a DN metacharacter
    Seq("a,b", "a+b", "a\"b", "a\\b", "a<b>c", "a;b", "#a", " a", "a=b").foreach { value =>
      assert(leftmostRdn("cn=" + LdapUtils.escapeLdapDnValue(value) + ",dc=example,dc=com")
        .getValue === value)
    }
  }

  test("EscapeLdapDnValueEscapesNul") {
    // Rdn.escapeValue leaves NUL untouched, but RFC 4514 requires it to be escaped and DN
    // parsers that treat the name as a C string would truncate there
    assert(LdapUtils.escapeLdapDnValue("a\u0000b") === "a\\00b")
    assert(leftmostRdn("cn=" + LdapUtils.escapeLdapDnValue("a\u0000b") + ",dc=example,dc=com")
      .getValue === "a\u0000b")
  }
}
