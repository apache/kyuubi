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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

class LdapUtilsSuite extends KyuubiFunSuite {
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

  test("CreateCandidatePrincipalsPreservesRegexSpecialCharsInUserName") {
    // Regression: substitution into the DN pattern must be literal. A user name containing
    // '$' (or '\') is interpreted as a regex replacement back-reference/escape by
    // String.replaceAll -- throwing IndexOutOfBoundsException or corrupting the value --
    // whereas String.replace substitutes it verbatim.
    val conf = new KyuubiConf()
      .set(KyuubiConf.AUTHENTICATION_LDAP_BASE_DN, "dc=mycompany,dc=com")
      .set(
        KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN,
        "cn=%s,ou=CORP,dc=mycompany,dc=com")
    val expected = Array("cn=user$1,ou=CORP,dc=mycompany,dc=com")
    val actual = LdapUtils.createCandidatePrincipals(conf, "user$1")
    assert(actual === expected)
  }

  test("ExtractFirstRdn") {
    val dn = "cn=user1,ou=CORP1,dc=mycompany,dc=com"
    val expected = "cn=user1"
    val actual = LdapUtils.extractFirstRdn(dn)
    assert(actual === expected)
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

  test("extractBaseDn handles DN with escaped comma in value") {
    // "Smith, John" contains a comma that must be escaped in a DN; indexOf would split here
    // incorrectly but DN.getParent() handles it correctly.
    val dn = "cn=Smith\\, John,ou=People,dc=example,dc=com"
    assert(LdapUtils.extractBaseDn(dn) === "ou=People,dc=example,dc=com")
  }

  test("extractFirstRdn handles DN with escaped comma in value") {
    val dn = "cn=Smith\\, John,ou=People,dc=example,dc=com"
    assert(LdapUtils.extractFirstRdn(dn) === "cn=Smith\\, John")
  }

  test("extractBaseDn falls back for pattern strings containing %s") {
    val pattern = "cn=%s,ou=CORP,dc=mycompany,dc=com"
    assert(LdapUtils.extractBaseDn(pattern) === "ou=CORP,dc=mycompany,dc=com")
  }

  test("extractFirstRdn falls back for pattern strings containing %s") {
    val pattern = "cn=%s,ou=CORP,dc=mycompany,dc=com"
    assert(LdapUtils.extractFirstRdn(pattern) === "cn=%s")
  }

  test("isDn returns true for valid distinguished name") {
    assert(LdapUtils.isDn("cn=user1,ou=CORP,dc=mycompany,dc=com"))
  }

  test("isDn returns false for plain username") {
    assert(!LdapUtils.isDn("user1"))
  }

  test("isDn returns false for null without throwing") {
    assert(!LdapUtils.isDn(null))
  }

  // Behaviour-change guards against regressions to the legacy `contains("=")` heuristic
  // that pre-dated the switch to UnboundID DN.isValidDN. Strings that contain '=' but are
  // malformed (invalid RDN, missing attribute name, etc.) must NOT be treated as DNs --
  // they should fall through to the candidate-principal pattern matcher instead. This is
  // the documented behavior change in F6 of the review.

  test("isDn returns false for malformed DN with invalid second RDN") {
    // Legacy heuristic returned true (contains '='); new strict check correctly returns
    // false because the second component "bad" has no attribute=value form.
    assert(!LdapUtils.isDn("cn=alice,bad"))
  }

  test("isDn returns false for malformed DN with empty attribute name") {
    assert(!LdapUtils.isDn("=foo"))
  }

  test("isDn returns false for email-style usernames") {
    assert(!LdapUtils.isDn("alice@example.com"))
  }

  test("isDn returns false for empty string") {
    // UnboundID DN.isValidDN("") returns true (the empty / null DN per RFC 4514), but our
    // isDn explicitly rejects it -- an empty string is never a meaningful DN login.
    assert(!LdapUtils.isDn(""))
  }

  test("isDn returns true for valid single-RDN DN") {
    assert(LdapUtils.isDn("uid=alice"))
  }

  test("isDn returns true for valid DN with escaped commas in attribute values") {
    assert(LdapUtils.isDn("cn=Doe\\, John,ou=people,dc=example,dc=com"))
  }

}
