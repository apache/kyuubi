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

import javax.naming.NamingException
import javax.security.sasl.AuthenticationException

import org.mockito.ArgumentMatchers.{any, anyString, eq => mockEq, isA}
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.service.authentication.ldap.{DirSearch, DirSearchFactory, LdapSearchFactory}
import org.apache.kyuubi.service.authentication.ldap.LdapUtils.getUserName

class LdapAuthenticationProviderImplSuite extends WithLdapServer {

  private var conf: KyuubiConf = _
  private var factory: DirSearchFactory = _
  private var search: DirSearch = _
  private var auth: LdapAuthenticationProviderImpl = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    conf = new KyuubiConf()
    conf.set(AUTHENTICATION_LDAP_URL, ldapUrl)
    factory = mock[DirSearchFactory]
    search = mock[DirSearch]
    when(factory.getInstance(any(classOf[KyuubiConf]), anyString, anyString))
      .thenReturn(search)
  }

  test("authenticateGivenBlankOrNullPassword") {
    Seq("", "\u0000", null).foreach { pwd =>
      auth = new LdapAuthenticationProviderImpl(conf, new LdapSearchFactory)
      val thrown = intercept[AuthenticationException] {
        auth.authenticate("user", pwd)
      }
      assert(thrown.getMessage.contains("is null or contains blank space"))
    }
  }

  test("AuthenticateNoUserOrGroupFilter") {
    conf.set(
      AUTHENTICATION_LDAP_USER_DN_PATTERN,
      "cn=%s,ou=Users,dc=mycorp,dc=com:cn=%s,ou=PowerUsers,dc=mycorp,dc=com")
    val factory = mock[DirSearchFactory]
    lenient
      .when(search.findUserDn("user1"))
      .thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com")
    when(factory.getInstance(conf, "cn=user1,ou=PowerUsers,dc=mycorp,dc=com", "Blah"))
      .thenReturn(search)
    when(factory.getInstance(conf, "cn=user1,ou=Users,dc=mycorp,dc=com", "Blah"))
      .thenThrow(classOf[AuthenticationException])

    auth = new LdapAuthenticationProviderImpl(conf, factory)
    auth.authenticate("user1", "Blah")

    verify(factory, times(2)).getInstance(isA(classOf[KyuubiConf]), anyString, mockEq("Blah"))
    verify(search, atLeastOnce).close()
  }

  test("AuthenticateWhenUserFilterPasses") {
    conf.set(AUTHENTICATION_LDAP_USER_FILTER.key, "user1,user2")

    when(search.findUserDn("user1")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com")
    when(search.findUserDn("user2")).thenReturn("cn=user2,ou=PowerUsers,dc=mycorp,dc=com")

    authenticateUserAndCheckSearchIsClosed("user1")
    authenticateUserAndCheckSearchIsClosed("user2")
  }

  test("AuthenticateWhenLoginWithDomainAndUserFilterPasses") {
    conf.set(AUTHENTICATION_LDAP_USER_FILTER.key, "user1")

    when(search.findUserDn("user1")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com")

    authenticateUserAndCheckSearchIsClosed("user1@mydomain.com")
  }

  test("AuthenticateWhenLoginWithDnAndUserFilterPasses") {
    conf.set(AUTHENTICATION_LDAP_USER_FILTER.key, "user1")

    when(search.findUserDn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com"))
      .thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com")

    authenticateUserAndCheckSearchIsClosed("cn=user1,ou=PowerUsers,dc=mycorp,dc=com")
  }

  test("AuthenticateWhenUserSearchFails") {
    conf.set(AUTHENTICATION_LDAP_USER_FILTER.key, "user1,user2")
    intercept[AuthenticationException] {
      when(search.findUserDn("user1")).thenReturn(null)
      authenticateUserAndCheckSearchIsClosed("user1")
    }
  }

  test("AuthenticateWhenUserFilterFails") {
    conf.set(AUTHENTICATION_LDAP_USER_FILTER.key, "user1,user2")
    intercept[AuthenticationException] {
      when(search.findUserDn("user3")).thenReturn("cn=user3,ou=PowerUsers,dc=mycorp,dc=com")
      authenticateUserAndCheckSearchIsClosed("user3")
    }
  }

  test("AuthenticateWhenGroupMembershipKeyFilterPasses") {
    conf.set(AUTHENTICATION_LDAP_GROUP_FILTER.key, "group1,group2")

    when(search.findUserDn("user1")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com")
    when(search.findUserDn("user2")).thenReturn("cn=user2,ou=PowerUsers,dc=mycorp,dc=com")

    when(search.findGroupsForUser("cn=user1,ou=PowerUsers,dc=mycorp,dc=com"))
      .thenReturn(Array(
        "cn=testGroup,ou=Groups,dc=mycorp,dc=com",
        "cn=group1,ou=Groups,dc=mycorp,dc=com"))
    when(search.findGroupsForUser("cn=user2,ou=PowerUsers,dc=mycorp,dc=com"))
      .thenReturn(Array(
        "cn=testGroup,ou=Groups,dc=mycorp,dc=com",
        "cn=group2,ou=Groups,dc=mycorp,dc=com"))

    authenticateUserAndCheckSearchIsClosed("user1")
    authenticateUserAndCheckSearchIsClosed("user2")
  }

  test("AuthenticateWhenUserAndGroupMembershipKeyFiltersPass") {
    conf.set(AUTHENTICATION_LDAP_GROUP_FILTER.key, "group1,group2")
    conf.set(AUTHENTICATION_LDAP_USER_FILTER.key, "user1,user2")

    when(search.findUserDn("user1")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com")
    when(search.findUserDn("user2")).thenReturn("cn=user2,ou=PowerUsers,dc=mycorp,dc=com")

    when(search.findGroupsForUser("cn=user1,ou=PowerUsers,dc=mycorp,dc=com"))
      .thenReturn(Array(
        "cn=testGroup,ou=Groups,dc=mycorp,dc=com",
        "cn=group1,ou=Groups,dc=mycorp,dc=com"))
    when(search.findGroupsForUser("cn=user2,ou=PowerUsers,dc=mycorp,dc=com"))
      .thenReturn(Array(
        "cn=testGroup,ou=Groups,dc=mycorp,dc=com",
        "cn=group2,ou=Groups,dc=mycorp,dc=com"))

    authenticateUserAndCheckSearchIsClosed("user1")
    authenticateUserAndCheckSearchIsClosed("user2")
  }

  test("AuthenticateWhenUserFilterPassesAndGroupMembershipKeyFilterFails") {
    conf.set(AUTHENTICATION_LDAP_GROUP_FILTER.key, "group1,group2")
    conf.set(AUTHENTICATION_LDAP_USER_FILTER.key, "user1,user2")
    intercept[AuthenticationException] {
      when(search.findUserDn("user1")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com")
      when(search.findGroupsForUser("cn=user1,ou=PowerUsers,dc=mycorp,dc=com"))
        .thenReturn(Array(
          "cn=testGroup,ou=Groups,dc=mycorp,dc=com",
          "cn=OtherGroup,ou=Groups,dc=mycorp,dc=com"))
      authenticateUserAndCheckSearchIsClosed("user1")
    }
  }

  test("AuthenticateWhenUserFilterFailsAndGroupMembershipKeyFilterPasses") {
    conf.set(AUTHENTICATION_LDAP_GROUP_FILTER.key, "group3")
    conf.set(AUTHENTICATION_LDAP_USER_FILTER.key, "user1,user2")
    intercept[AuthenticationException] {
      when(search.findUserDn("user3")).thenReturn("cn=user3,ou=PowerUsers,dc=mycorp,dc=com")
      lenient.when(search.findGroupsForUser("cn=user3,ou=PowerUsers,dc=mycorp,dc=com"))
        .thenReturn(Array(
          "cn=testGroup,ou=Groups,dc=mycorp,dc=com",
          "cn=group3,ou=Groups,dc=mycorp,dc=com"))
      authenticateUserAndCheckSearchIsClosed("user3")
    }
  }

  test("AuthenticateWhenCustomQueryFilterPasses") {
    conf.set(AUTHENTICATION_LDAP_BASE_DN, "dc=mycorp,dc=com")
    conf.set(
      AUTHENTICATION_LDAP_CUSTOM_LDAP_QUERY,
      "(&(objectClass=person)(|(memberOf=CN=Domain Admins,CN=Users,DC=apache,DC=org)" +
        "(memberOf=CN=Administrators,CN=Builtin,DC=apache,DC=org)))")

    when(search.executeCustomQuery(anyString))
      .thenReturn(Array(
        "cn=user1,ou=PowerUsers,dc=mycorp,dc=com",
        "cn=user2,ou=PowerUsers,dc=mycorp,dc=com"))

    authenticateUserAndCheckSearchIsClosed("user1")
  }

  test("AuthenticateWhenCustomQueryFilterFailsAndUserFilterPasses") {
    conf.set(AUTHENTICATION_LDAP_BASE_DN, "dc=mycorp,dc=com")
    conf.set(
      AUTHENTICATION_LDAP_CUSTOM_LDAP_QUERY,
      "(&(objectClass=person)(|(memberOf=CN=Domain Admins,CN=Users,DC=apache,DC=org)" +
        "(memberOf=CN=Administrators,CN=Builtin,DC=apache,DC=org)))")
    conf.set(AUTHENTICATION_LDAP_USER_FILTER.key, "user3")
    intercept[AuthenticationException] {
      lenient.when(search.findUserDn("user3")).thenReturn("cn=user3,ou=PowerUsers,dc=mycorp,dc=com")
      when(search.executeCustomQuery(anyString))
        .thenReturn(Array(
          "cn=user1,ou=PowerUsers,dc=mycorp,dc=com",
          "cn=user2,ou=PowerUsers,dc=mycorp,dc=com"))
      authenticateUserAndCheckSearchIsClosed("user3")
    }
  }

  test("AuthenticateWhenUserMembershipKeyFilterPasses") {
    conf.set(AUTHENTICATION_LDAP_BASE_DN, "dc=mycorp,dc=com")
    conf.set(AUTHENTICATION_LDAP_GROUP_FILTER.key, "HIVE-USERS")
    conf.set(AUTHENTICATION_LDAP_USER_MEMBERSHIP_KEY, "memberOf")

    when(search.findUserDn("user1")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com")

    val groupDn = "cn=HIVE-USERS,ou=Groups,dc=mycorp,dc=com"
    when(search.findGroupDn("HIVE-USERS")).thenReturn(groupDn)
    when(search.isUserMemberOfGroup("user1", groupDn)).thenReturn(true)

    auth = new LdapAuthenticationProviderImpl(conf, factory)
    auth.authenticate("user1", "Blah")

    verify(factory, times(1)).getInstance(isA(classOf[KyuubiConf]), anyString, mockEq("Blah"))
    verify(search, times(1)).findGroupDn(anyString)
    verify(search, times(1)).isUserMemberOfGroup(anyString, anyString)
    verify(search, atLeastOnce).close()
  }

  test("AuthenticateWhenUserMembershipKeyFilterFails") {
    conf.set(AUTHENTICATION_LDAP_BASE_DN, "dc=mycorp,dc=com")
    conf.set(AUTHENTICATION_LDAP_GROUP_FILTER.key, "HIVE-USERS")
    conf.set(AUTHENTICATION_LDAP_USER_MEMBERSHIP_KEY, "memberOf")
    intercept[AuthenticationException] {
      when(search.findUserDn("user1")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com")

      val groupDn = "cn=HIVE-USERS,ou=Groups,dc=mycorp,dc=com"
      when(search.findGroupDn("HIVE-USERS")).thenReturn(groupDn)
      when(search.isUserMemberOfGroup("user1", groupDn)).thenReturn(false)

      auth = new LdapAuthenticationProviderImpl(conf, factory)
      auth.authenticate("user1", "Blah")
    }
  }

  test("AuthenticateWhenUserMembershipKeyFilter2x2PatternsPasses") {
    conf.set(AUTHENTICATION_LDAP_GROUP_FILTER.key, "HIVE-USERS1,HIVE-USERS2")
    conf.set(AUTHENTICATION_LDAP_GROUP_DN_PATTERN, "cn=%s,ou=Groups,ou=branch1,dc=mycorp,dc=com")
    conf.set(AUTHENTICATION_LDAP_USER_DN_PATTERN, "cn=%s,ou=Userss,ou=branch1,dc=mycorp,dc=com")
    conf.set(AUTHENTICATION_LDAP_USER_MEMBERSHIP_KEY, "memberOf")

    when(search.findUserDn("user1")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com")

    when(search.findGroupDn("HIVE-USERS1"))
      .thenReturn("cn=HIVE-USERS1,ou=Groups,ou=branch1,dc=mycorp,dc=com")
    when(search.findGroupDn("HIVE-USERS2"))
      .thenReturn("cn=HIVE-USERS2,ou=Groups,ou=branch1,dc=mycorp,dc=com")

    when(search.isUserMemberOfGroup(
      "user1",
      "cn=HIVE-USERS1,ou=Groups,ou=branch1,dc=mycorp,dc=com"))
      .thenThrow(classOf[NamingException])
    when(search.isUserMemberOfGroup(
      "user1",
      "cn=HIVE-USERS2,ou=Groups,ou=branch1,dc=mycorp,dc=com"))
      .thenReturn(true)

    auth = new LdapAuthenticationProviderImpl(conf, factory)
    auth.authenticate("user1", "Blah")

    verify(factory, times(1)).getInstance(isA(classOf[KyuubiConf]), anyString, mockEq("Blah"))
    verify(search, times(2)).findGroupDn(anyString)
    verify(search, times(2)).isUserMemberOfGroup(anyString, anyString)
    verify(search, atLeastOnce).close()
  }

  // Kyuubi does not implement it
  // test("AuthenticateWithBindInCredentialFilePasses")
  // test("testAuthenticateWithBindInMissingCredentialFilePasses")

  test("AuthenticateWithBindUserPasses") {
    val bindUser = "cn=BindUser,ou=Users,ou=branch1,dc=mycorp,dc=com"
    val bindPass = "Blah"
    val authFullUser = "cn=user1,ou=Users,ou=branch1,dc=mycorp,dc=com"
    val authUser = "user1"
    val authPass = "Blah2"
    conf.set(AUTHENTICATION_LDAP_BIND_USER, bindUser)
    conf.set(AUTHENTICATION_LDAP_BIND_PASSWORD, bindPass)

    when(search.findUserDn(mockEq(authUser))).thenReturn(authFullUser)

    auth = new LdapAuthenticationProviderImpl(conf, factory)
    auth.authenticate(authUser, authPass)

    verify(factory, times(1)).getInstance(
      isA(classOf[KyuubiConf]),
      mockEq(bindUser),
      mockEq(bindPass))
    verify(factory, times(1)).getInstance(
      isA(classOf[KyuubiConf]),
      mockEq(authFullUser),
      mockEq(authPass))
    verify(search, times(1)).findUserDn(mockEq(authUser))
  }

  test("AuthenticateWithBindDomainUserPasses") {
    val bindUser = "cn=BindUser,ou=Users,ou=branch1,dc=mycorp,dc=com"
    val bindPass = "Blah"
    val authFullUser = "cn=user1,ou=Users,ou=branch1,dc=mycorp,dc=com"
    val authUser = "user1@mydomain.com"
    val authPass = "Blah2"
    conf.set(AUTHENTICATION_LDAP_BIND_USER, bindUser)
    conf.set(AUTHENTICATION_LDAP_BIND_PASSWORD, bindPass)

    val username = getUserName(authUser)
    when(search.findUserDn(mockEq(username))).thenReturn(authFullUser)

    auth = new LdapAuthenticationProviderImpl(conf, factory)
    auth.authenticate(authUser, authPass)

    verify(factory, times(1)).getInstance(
      isA(classOf[KyuubiConf]),
      mockEq(bindUser),
      mockEq(bindPass))
    verify(factory, times(1)).getInstance(
      isA(classOf[KyuubiConf]),
      mockEq(authFullUser),
      mockEq(authPass))
    verify(search, times(1)).findUserDn(mockEq(username))
  }

  test("AuthenticateWithBindUserFailsOnAuthentication") {
    val bindUser = "cn=BindUser,ou=Users,ou=branch1,dc=mycorp,dc=com"
    val bindPass = "Blah"
    val authFullUser = "cn=user1,ou=Users,ou=branch1,dc=mycorp,dc=com"
    val authUser = "user1"
    val authPass = "Blah2"
    conf.set(AUTHENTICATION_LDAP_BIND_USER, bindUser)
    conf.set(AUTHENTICATION_LDAP_BIND_PASSWORD, bindPass)

    intercept[AuthenticationException] {
      when(
        factory.getInstance(
          any(classOf[KyuubiConf]),
          mockEq(authFullUser),
          mockEq(authPass))).thenThrow(classOf[AuthenticationException])
      when(search.findUserDn(mockEq(authUser))).thenReturn(authFullUser)

      auth = new LdapAuthenticationProviderImpl(conf, factory)
      auth.authenticate(authUser, authPass)
    }
  }

  test("AuthenticateWithBindUserFailsOnGettingDn") {
    val bindUser = "cn=BindUser,ou=Users,ou=branch1,dc=mycorp,dc=com"
    val bindPass = "Blah"
    val authUser = "user1"
    val authPass = "Blah2"
    conf.set(AUTHENTICATION_LDAP_BIND_USER, bindUser)
    conf.set(AUTHENTICATION_LDAP_BIND_PASSWORD, bindPass)

    intercept[AuthenticationException] {
      when(search.findUserDn(mockEq(authUser))).thenThrow(classOf[NamingException])
      auth = new LdapAuthenticationProviderImpl(conf, factory)
      auth.authenticate(authUser, authPass)
    }
  }

  test("AuthenticateWithBindUserFailsOnBinding") {
    val bindUser = "cn=BindUser,ou=Users,ou=branch1,dc=mycorp,dc=com"
    val bindPass = "Blah"
    val authUser = "user1"
    val authPass = "Blah2"
    conf.set(AUTHENTICATION_LDAP_BIND_USER, bindUser)
    conf.set(AUTHENTICATION_LDAP_BIND_PASSWORD, bindPass)

    intercept[AuthenticationException] {
      when(factory.getInstance(any(classOf[KyuubiConf]), mockEq(bindUser), mockEq(bindPass)))
        .thenThrow(classOf[AuthenticationException])

      auth = new LdapAuthenticationProviderImpl(conf, factory)
      auth.authenticate(authUser, authPass)
    }
  }

  private def authenticateUserAndCheckSearchIsClosed(user: String): Unit = {
    auth = new LdapAuthenticationProviderImpl(conf, factory)
    try auth.authenticate(user, "password doesn't matter")
    finally verify(search, atLeastOnce).close()
  }
}
