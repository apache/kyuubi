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

import com.unboundid.ldap.sdk.Entry

import org.apache.kyuubi.service.authentication.ldap.{LdapAuthenticationTestCase, User}

class LdapAtnProviderSuite extends WithLdapServer {

  override protected val ldapBaseDn: Array[String] = Array(
    "dc=example,dc=com",
    "cn=microsoft,ou=schema")

  private val GROUP1_NAME = "group1"
  private val GROUP2_NAME = "group2"
  private val GROUP3_NAME = "group3"
  private val GROUP4_NAME = "group4"

  private val GROUP_ADMINS_NAME = "admins"
  private val GROUP_TEAM1_NAME = "team1"
  private val GROUP_TEAM2_NAME = "team2"
  private val GROUP_RESOURCE1_NAME = "resource1"
  private val GROUP_RESOURCE2_NAME = "resource2"

  private val USER1 =
    User.useIdForPassword(id = "user1", dn = "uid=user1,ou=People,dc=example,dc=com")

  private val USER2 =
    User.useIdForPassword(id = "user2", dn = "uid=user2,ou=People,dc=example,dc=com")

  private val USER3 =
    User.useIdForPassword(id = "user3", dn = "cn=user3,ou=People,dc=example,dc=com")

  private val USER4 =
    User.useIdForPassword(id = "user4", dn = "cn=user4,ou=People,dc=example,dc=com")

  private val ENGINEER_1 = User(
    id = "engineer1",
    dn = "sAMAccountName=engineer1,ou=Engineering,dc=ad,dc=example,dc=com",
    password = "engineer1-password")

  private val ENGINEER_2 = User(
    id = "engineer2",
    dn = "sAMAccountName=engineer2,ou=Engineering,dc=ad,dc=example,dc=com",
    password = "engineer2-password")

  private val MANAGER_1 = User(
    id = "manager1",
    dn = "sAMAccountName=manager1,ou=Management,dc=ad,dc=example,dc=com",
    password = "manager1-password")

  private val MANAGER_2 = User(
    id = "manager2",
    dn = "sAMAccountName=manager2,ou=Management,dc=ad,dc=example,dc=com",
    password = "manager2-password")

  private val ADMIN_1 = User(
    id = "admin1",
    dn = "sAMAccountName=admin1,ou=Administration,dc=ad,dc=example,dc=com",
    password = "admin1-password")

  private var testCase: LdapAuthenticationTestCase = _

  private def defaultBuilder = LdapAuthenticationTestCase.builder.ldapUrl(ldapUrl)

  override def beforeAll(): Unit = {
    super.beforeAll()
    ldapServer.add(new Entry(
      "dn: dc=example,dc=com",
      "dc: example",
      "objectClass: top",
      "objectClass: domain"))

    applyLDIF("ldap/example.com.ldif")
    applyLDIF("ldap/microsoft.schema.ldif")
    applyLDIF("ldap/ad.example.com.ldif")
  }

  test("In-Memory LDAP server is started") {
    assert(ldapServer.getListenPort > 0)
  }

  test("UserBindPositiveWithShortname") {
    testCase = defaultBuilder
      .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
      .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
      .build
    testCase.assertAuthenticatePasses(USER1.credentialsWithId)
    testCase.assertAuthenticatePasses(USER2.credentialsWithId)
  }

  test("UserBindPositiveWithShortnameOldConfig") {
    testCase = defaultBuilder
      .baseDN("ou=People,dc=example,dc=com")
      .build
    testCase.assertAuthenticatePasses(USER1.credentialsWithId)
    testCase.assertAuthenticatePasses(USER2.credentialsWithId)
  }

  test("UserBindNegativeWithShortname") {
    testCase = defaultBuilder
      .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
      .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
      .build
    testCase.assertAuthenticateFailsUsingWrongPassword(USER1.credentialsWithId)
    testCase.assertAuthenticateFailsUsingWrongPassword(USER2.credentialsWithId)
  }

  test("UserBindNegativeWithShortnameOldConfig") {
    testCase = defaultBuilder
      .baseDN("ou=People,dc=example,dc=com")
      .build
    testCase.assertAuthenticateFailsUsingWrongPassword(USER1.credentialsWithId)
    testCase.assertAuthenticateFails(USER1.dn, USER2.password)
    testCase.assertAuthenticateFailsUsingWrongPassword(USER2.credentialsWithId)
  }

  test("UserBindPositiveWithDN") {
    testCase = defaultBuilder
      .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
      .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
      .build
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn)
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn)
  }

  test("UserBindPositiveWithDNOldConfig") {
    testCase = defaultBuilder
      .baseDN("ou=People,dc=example,dc=com")
      .build
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn)
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn)
  }

  test("UserBindPositiveWithDNWrongOldConfig") {
    testCase = defaultBuilder
      .baseDN("ou=DummyPeople,dc=example,dc=com")
      .build
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn)
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn)
  }

  test("UserBindPositiveWithDNWrongConfig") {
    testCase = defaultBuilder
      .userDNPatterns("uid=%s,ou=DummyPeople,dc=example,dc=com")
      .groupDNPatterns("uid=%s,ou=DummyGroups,dc=example,dc=com")
      .build
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn)
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn)
  }

  test("UserBindPositiveWithDNBlankConfig") {
    testCase = defaultBuilder
      .userDNPatterns(" ")
      .groupDNPatterns(" ")
      .build
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn)
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn)
  }

  test("UserBindPositiveWithDNBlankOldConfig") {
    testCase = defaultBuilder.baseDN("").build
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn)
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn)
  }

  test("UserBindNegativeWithDN") {
    testCase = defaultBuilder
      .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
      .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
      .build
    testCase.assertAuthenticateFailsUsingWrongPassword(USER1.credentialsWithDn)
    testCase.assertAuthenticateFails(USER1.dn, USER2.password)
    testCase.assertAuthenticateFailsUsingWrongPassword(USER2.credentialsWithDn)
  }

  test("UserBindNegativeWithDNOldConfig") {
    testCase = defaultBuilder
      .baseDN("ou=People,dc=example,dc=com")
      .build
    testCase.assertAuthenticateFailsUsingWrongPassword(USER1.credentialsWithDn)
    testCase.assertAuthenticateFails(USER1.dn, USER2.password)
    testCase.assertAuthenticateFailsUsingWrongPassword(USER2.credentialsWithDn)
  }

  test("UserFilterPositive") {
    testCase = defaultBuilder
      .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
      .userFilters(USER1.id)
      .build
    testCase.assertAuthenticatePasses(USER1.credentialsWithId)
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn)
    testCase = defaultBuilder
      .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
      .userFilters(USER2.id)
      .build
    testCase.assertAuthenticatePasses(USER2.credentialsWithId)
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn)
    testCase = defaultBuilder
      .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
      .userFilters(USER1.id, USER2.id)
      .build
    testCase.assertAuthenticatePasses(USER1.credentialsWithId)
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn)
    testCase.assertAuthenticatePasses(USER2.credentialsWithId)
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn)
  }

  test("UserFilterNegative") {
    testCase = defaultBuilder
      .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
      .userFilters(USER2.id)
      .build
    testCase.assertAuthenticateFails(USER1.credentialsWithId)
    testCase.assertAuthenticateFails(USER1.credentialsWithDn)
    testCase = defaultBuilder
      .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
      .userFilters(USER1.id)
      .build
    testCase.assertAuthenticateFails(USER2.credentialsWithId)
    testCase.assertAuthenticateFails(USER2.credentialsWithDn)
    testCase = defaultBuilder
      .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
      .userFilters(USER3.id)
      .build
    testCase.assertAuthenticateFails(USER1.credentialsWithId)
    testCase.assertAuthenticateFails(USER2.credentialsWithId)
  }

  test("GroupFilterPositive") {
    testCase = defaultBuilder
      .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
      .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
      .groupFilters(GROUP1_NAME, GROUP2_NAME)
      .build
    testCase.assertAuthenticatePasses(USER1.credentialsWithId)
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn)
    testCase.assertAuthenticatePasses(USER2.credentialsWithId)
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn)
    testCase = defaultBuilder
      .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
      .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
      .groupFilters(GROUP2_NAME)
      .build
    testCase.assertAuthenticatePasses(USER2.credentialsWithId)
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn)
  }

  test("GroupFilterNegative") {
    testCase = defaultBuilder
      .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
      .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
      .groupFilters(GROUP2_NAME)
      .build
    testCase.assertAuthenticateFails(USER1.credentialsWithId)
    testCase.assertAuthenticateFails(USER1.credentialsWithDn)
    testCase = defaultBuilder
      .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
      .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
      .groupFilters(GROUP1_NAME)
      .build
    testCase.assertAuthenticateFails(USER2.credentialsWithId)
    testCase.assertAuthenticateFails(USER2.credentialsWithDn)
  }

  test("UserAndGroupFilterPositive") {
    testCase = defaultBuilder
      .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
      .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
      .userFilters(USER1.id, USER2.id)
      .groupFilters(GROUP1_NAME, GROUP2_NAME)
      .build
    testCase.assertAuthenticatePasses(USER1.credentialsWithId)
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn)
    testCase.assertAuthenticatePasses(USER2.credentialsWithId)
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn)
  }

  test("UserAndGroupFilterNegative") {
    testCase = defaultBuilder
      .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
      .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
      .userFilters(USER1.id, USER2.id)
      .groupFilters(GROUP3_NAME, GROUP3_NAME)
      .build
    testCase.assertAuthenticateFails(USER2.credentialsWithDn)
    testCase.assertAuthenticateFails(USER2.credentialsWithId)
    testCase.assertAuthenticateFails(USER3.credentialsWithDn)
    testCase.assertAuthenticateFails(USER3.credentialsWithId)
  }

  test("CustomQueryPositive") {
    testCase = defaultBuilder
      .baseDN("ou=People,dc=example,dc=com")
      .userDNPatterns("cn=%s,ou=People,dc=example,dc=com", "uid=%s,ou=People,dc=example,dc=com")
      .groupDNPatterns("cn=%s,ou=People,dc=example,dc=com")
      .customQuery(String.format("(&(objectClass=person)(|(uid=%s)(uid=%s)))", USER1.id, USER4.id))
      .build
    testCase.assertAuthenticatePasses(USER1.credentialsWithId)
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn)
    testCase.assertAuthenticatePasses(USER4.credentialsWithId)
    testCase.assertAuthenticatePasses(USER4.credentialsWithDn)
    testCase = defaultBuilder
      .baseDN("ou=People,dc=example,dc=com")
      .customQuery("(&(objectClass=person)(uid=%s))")
      .build
    testCase.assertAuthenticatePasses(USER1.credentialsWithId)
    testCase.assertAuthenticatePasses(USER2.credentialsWithId)
  }

  test("CustomQueryNegative") {
    testCase = defaultBuilder
      .baseDN("ou=People,dc=example,dc=com")
      .customQuery("(&(objectClass=person)(cn=%s))")
      .build
    testCase.assertAuthenticateFails(USER2.credentialsWithDn)
    testCase.assertAuthenticateFails(USER2.credentialsWithId)
  }

  /**
   * Test to test the LDAP Atn to use a custom LDAP query that returns
   * a) A set of group DNs
   * b) A combination of group(s) DN and user DN
   * LDAP atn is expected to extract the members of the group using the attribute value for
   * `kyuubi.authentication.ldap.userMembershipKey`
   */
  test("CustomQueryWithGroupsPositive") {
    testCase = defaultBuilder
      .baseDN("dc=example,dc=com")
      .userDNPatterns("cn=%s,ou=People,dc=example,dc=com", "uid=%s,ou=People,dc=example,dc=com")
      .customQuery(s"(&(objectClass=groupOfNames)(|(cn=$GROUP1_NAME)(cn=$GROUP2_NAME)))")
      .build
    testCase.assertAuthenticatePasses(USER1.credentialsWithId)
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn)
    testCase.assertAuthenticatePasses(USER2.credentialsWithId)
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn)
    // the following test uses a query that returns a group and a user entry.
    // the ldap atn should use the groupMembershipKey to identify the users for the returned group
    // and the authentication should succeed for the users of that group as well as the lone user4
    // in this case
    testCase = defaultBuilder
      .baseDN("dc=example,dc=com")
      .userDNPatterns("cn=%s,ou=People,dc=example,dc=com", "uid=%s,ou=People,dc=example,dc=com")
      .customQuery(
        s"(|(&(objectClass=groupOfNames)(cn=$GROUP1_NAME))(&(objectClass=person)(sn=${USER4.id})))")
      .build
    testCase.assertAuthenticatePasses(USER1.credentialsWithId)
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn)
    testCase.assertAuthenticatePasses(USER4.credentialsWithId)
    testCase.assertAuthenticatePasses(USER4.credentialsWithDn)
    testCase = defaultBuilder
      .baseDN("dc=example,dc=com")
      .userDNPatterns("cn=%s,ou=People,dc=example,dc=com", "uid=%s,ou=People,dc=example,dc=com")
      .groupMembershipKey("uniqueMember")
      .customQuery(s"(&(objectClass=groupOfUniqueNames)(cn=$GROUP4_NAME))")
      .build
    testCase.assertAuthenticatePasses(USER4.credentialsWithId)
    testCase.assertAuthenticatePasses(USER4.credentialsWithDn)
  }

  test("CustomQueryWithGroupsNegative") {
    testCase = defaultBuilder
      .baseDN("dc=example,dc=com")
      .userDNPatterns("cn=%s,ou=People,dc=example,dc=com", "uid=%s,ou=People,dc=example,dc=com")
      .customQuery(s"(&(objectClass=groupOfNames)(|(cn=$GROUP1_NAME)(cn=$GROUP2_NAME)))")
      .build
    testCase.assertAuthenticateFails(USER3.credentialsWithDn)
    testCase.assertAuthenticateFails(USER3.credentialsWithId)
  }

  test("GroupFilterPositiveWithCustomGUID") {
    testCase = defaultBuilder
      .userDNPatterns("cn=%s,ou=People,dc=example,dc=com")
      .groupDNPatterns("cn=%s,ou=Groups,dc=example,dc=com")
      .groupFilters(GROUP3_NAME)
      .guidKey("cn")
      .build
    testCase.assertAuthenticatePasses(USER3.credentialsWithId)
    testCase.assertAuthenticatePasses(USER3.credentialsWithDn)
  }

  test("GroupFilterPositiveWithCustomAttributes") {
    testCase = defaultBuilder
      .userDNPatterns("cn=%s,ou=People,dc=example,dc=com")
      .groupDNPatterns("cn=%s,ou=Groups,dc=example,dc=com")
      .groupFilters(GROUP4_NAME)
      .guidKey("cn")
      .groupMembershipKey("uniqueMember")
      .groupClassKey("groupOfUniqueNames")
      .build
    testCase.assertAuthenticatePasses(USER4.credentialsWithId)
    testCase.assertAuthenticatePasses(USER4.credentialsWithDn)
  }

  test("DirectUserMembershipGroupFilterPositive") {
    testCase = defaultBuilder
      .userDNPatterns(
        "sAMAccountName=%s,ou=Engineering,dc=ad,dc=example,dc=com",
        "sAMAccountName=%s,ou=Management,dc=ad,dc=example,dc=com")
      .groupDNPatterns(
        "sAMAccountName=%s,ou=Teams,dc=ad,dc=example,dc=com",
        "sAMAccountName=%s,ou=Resources,dc=ad,dc=example,dc=com")
      .groupFilters(GROUP_TEAM1_NAME, GROUP_TEAM2_NAME, GROUP_RESOURCE1_NAME, GROUP_RESOURCE2_NAME)
      .guidKey("sAMAccountName")
      .userMembershipKey("memberOf")
      .build
    testCase.assertAuthenticatePasses(ENGINEER_1.credentialsWithId)
    testCase.assertAuthenticatePasses(ENGINEER_2.credentialsWithId)
    testCase.assertAuthenticatePasses(MANAGER_1.credentialsWithId)
    testCase.assertAuthenticatePasses(MANAGER_2.credentialsWithId)
  }

  test("DirectUserMembershipGroupFilterNegative") {
    testCase = defaultBuilder
      .userDNPatterns(
        "sAMAccountName=%s,ou=Engineering,dc=ad,dc=example,dc=com",
        "sAMAccountName=%s,ou=Management,dc=ad,dc=example,dc=com")
      .groupDNPatterns("cn=%s,ou=Teams,dc=ad,dc=example,dc=com")
      .groupFilters(GROUP_TEAM1_NAME)
      .guidKey("sAMAccountName")
      .userMembershipKey("memberOf")
      .build
    testCase.assertAuthenticateFails(ENGINEER_2.credentialsWithId)
    testCase.assertAuthenticateFails(MANAGER_2.credentialsWithId)
  }

  test("DirectUserMembershipGroupFilterNegativeWithoutUserBases") {
    testCase = defaultBuilder
      .groupDNPatterns("cn=%s,ou=Teams,dc=ad,dc=example,dc=com")
      .groupFilters(GROUP_TEAM1_NAME)
      .guidKey("sAMAccountName")
      .userMembershipKey("memberOf")
      .build
    testCase.assertAuthenticateFails(ENGINEER_1.credentialsWithId)
    testCase.assertAuthenticateFails(ENGINEER_2.credentialsWithId)
    testCase.assertAuthenticateFails(MANAGER_1.credentialsWithId)
    testCase.assertAuthenticateFails(MANAGER_2.credentialsWithId)
  }

  test("DirectUserMembershipGroupFilterWithDNCredentials") {
    testCase = defaultBuilder
      .userDNPatterns("sAMAccountName=%s,ou=Engineering,dc=ad,dc=example,dc=com")
      .groupDNPatterns("cn=%s,ou=Teams,dc=ad,dc=example,dc=com")
      .groupFilters(GROUP_TEAM1_NAME)
      .guidKey("sAMAccountName")
      .userMembershipKey("memberOf")
      .build
    testCase.assertAuthenticatePasses(ENGINEER_1.credentialsWithDn)
    testCase.assertAuthenticateFails(MANAGER_1.credentialsWithDn)
  }

  test("DirectUserMembershipGroupFilterWithDifferentGroupClassKey") {
    testCase = defaultBuilder
      .userDNPatterns("sAMAccountName=%s,ou=Administration,dc=ad,dc=example,dc=com")
      .groupDNPatterns("cn=%s,ou=Administration,dc=ad,dc=example,dc=com")
      .groupFilters(GROUP_ADMINS_NAME).guidKey("sAMAccountName")
      .userMembershipKey("memberOf")
      .groupClassKey("groupOfUniqueNames")
      .build
    testCase.assertAuthenticatePasses(ADMIN_1.credentialsWithId)
    testCase.assertAuthenticateFails(ENGINEER_1.credentialsWithId)
    testCase.assertAuthenticateFails(MANAGER_1.credentialsWithDn)
  }

  test("DirectUserMembershipGroupFilterNegativeWithWrongGroupClassKey") {
    testCase = defaultBuilder
      .userDNPatterns("sAMAccountName=%s,ou=Administration,dc=ad,dc=example,dc=com")
      .groupDNPatterns("cn=%s,ou=Administration,dc=ad,dc=example,dc=com")
      .groupFilters(GROUP_ADMINS_NAME).guidKey("sAMAccountName")
      .userMembershipKey("memberOf")
      .groupClassKey("wrongClass")
      .build
    testCase.assertAuthenticateFails(ADMIN_1.credentialsWithId)
    testCase.assertAuthenticateFails(ENGINEER_1.credentialsWithId)
    testCase.assertAuthenticateFails(MANAGER_1.credentialsWithDn)
  }
}
