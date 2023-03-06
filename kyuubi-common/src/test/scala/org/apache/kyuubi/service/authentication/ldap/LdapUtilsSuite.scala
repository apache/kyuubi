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
}
