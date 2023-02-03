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

import javax.naming.{NamingEnumeration, NamingException}
import javax.naming.directory.{DirContext, SearchControls, SearchResult}

import org.mockito.ArgumentMatchers.{any, anyString, contains, eq => mockEq}
import org.mockito.Mockito.{atLeastOnce, verify, when}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.ldap.LdapTestUtils._

class LdapSearchSuite extends KyuubiFunSuite {
  private var conf: KyuubiConf = _
  private var ctx: DirContext = _
  private var search: LdapSearch = _

  override protected def beforeEach(): Unit = {
    conf = new KyuubiConf()
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_USER_MEMBERSHIP_KEY, "memberOf")
    ctx = mock[DirContext]
    super.beforeEach()
  }

  test("close") {
    search = new LdapSearch(conf, ctx)
    search.close()
    verify(ctx, atLeastOnce).close()
  }

  test("FindUserDnWhenUserDnPositive") {
    val searchResult: NamingEnumeration[SearchResult] =
      mockNamingEnumeration("CN=User1,OU=org1,DC=foo,DC=bar")
    when(ctx.search(anyString, anyString, any(classOf[SearchControls])))
      .thenReturn(searchResult)
      .thenThrow(classOf[NamingException])
    search = new LdapSearch(conf, ctx)
    val expected: String = "CN=User1,OU=org1,DC=foo,DC=bar"
    val actual: String = search.findUserDn("CN=User1,OU=org1")
    assert(expected === actual)
  }

  test("FindUserDnWhenUserDnNegativeDuplicates") {
    val searchResult: NamingEnumeration[SearchResult] =
      mockNamingEnumeration("CN=User1,OU=org1,DC=foo,DC=bar", "CN=User1,OU=org2,DC=foo,DC=bar")
    when(ctx.search(anyString, anyString, any(classOf[SearchControls])))
      .thenReturn(searchResult)
    search = new LdapSearch(conf, ctx)
    assert(search.findUserDn("CN=User1,DC=foo,DC=bar") === null)
  }

  test("FindUserDnWhenUserDnNegativeNone") {
    val searchResult: NamingEnumeration[SearchResult] = mockEmptyNamingEnumeration
    when(ctx.search(anyString, anyString, any(classOf[SearchControls])))
      .thenReturn(searchResult)
    search = new LdapSearch(conf, ctx)
    assert(search.findUserDn("CN=User1,DC=foo,DC=bar") === null)
  }

  test("FindUserDnWhenUserPatternFoundBySecondPattern") {
    conf.set(
      KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN,
      "CN=%s,OU=org1,DC=foo,DC=bar:CN=%s,OU=org2,DC=foo,DC=bar")
    val emptyResult: NamingEnumeration[SearchResult] = mockEmptyNamingEnumeration
    val validResult: NamingEnumeration[SearchResult] =
      mockNamingEnumeration("CN=User1,OU=org2,DC=foo,DC=bar")
    when(ctx.search(anyString, anyString, any(classOf[SearchControls])))
      .thenReturn(emptyResult)
      .thenReturn(validResult)
    search = new LdapSearch(conf, ctx)
    val expected: String = "CN=User1,OU=org2,DC=foo,DC=bar"
    val actual: String = search.findUserDn("User1")
    assert(expected === actual)
    verify(ctx).search(
      mockEq("OU=org1,DC=foo,DC=bar"),
      contains("CN=User1"),
      any(classOf[SearchControls]))
    verify(ctx).search(
      mockEq("OU=org2,DC=foo,DC=bar"),
      contains("CN=User1"),
      any(classOf[SearchControls]))
  }

  test("FindUserDnWhenUserPatternFoundByFirstPattern") {
    conf.set(
      KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN,
      "CN=%s,OU=org1,DC=foo,DC=bar:CN=%s,OU=org2,DC=foo,DC=bar")
    val emptyResult: NamingEnumeration[SearchResult] = mockEmptyNamingEnumeration
    val validResult: NamingEnumeration[SearchResult] =
      mockNamingEnumeration("CN=User1,OU=org2,DC=foo,DC=bar")
    when(ctx.search(anyString, anyString, any(classOf[SearchControls])))
      .thenReturn(validResult)
      .thenReturn(emptyResult)
    search = new LdapSearch(conf, ctx)
    val expected: String = "CN=User1,OU=org2,DC=foo,DC=bar"
    val actual: String = search.findUserDn("User1")
    assert(expected === actual)
    verify(ctx).search(
      mockEq("OU=org1,DC=foo,DC=bar"),
      contains("CN=User1"),
      any(classOf[SearchControls]))
  }

  test("FindUserDnWhenUserPatternFoundByUniqueIdentifier") {
    conf.set(
      KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN,
      "CN=%s,OU=org1,DC=foo,DC=bar")
    val validResult: NamingEnumeration[SearchResult] =
      mockNamingEnumeration("CN=User1,OU=org1,DC=foo,DC=bar")
    when(ctx.search(anyString, anyString, any(classOf[SearchControls])))
      .thenReturn(null)
      .thenReturn(validResult)
    search = new LdapSearch(conf, ctx)
    val expected: String = "CN=User1,OU=org1,DC=foo,DC=bar"
    val actual: String = search.findUserDn("User1")
    assert(expected === actual)
    verify(ctx).search(
      mockEq("OU=org1,DC=foo,DC=bar"),
      contains("CN=User1"),
      any(classOf[SearchControls]))
    verify(ctx).search(
      mockEq("OU=org1,DC=foo,DC=bar"),
      contains("uid=User1"),
      any(classOf[SearchControls]))
  }

  test("FindUserDnWhenUserPatternFoundByUniqueIdentifierNegativeNone") {
    conf.set(
      KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN,
      "CN=%s,OU=org1,DC=foo,DC=bar")
    when(ctx.search(anyString, anyString, any(classOf[SearchControls])))
      .thenReturn(null)
      .thenReturn(null)
    search = new LdapSearch(conf, ctx)
    assert(search.findUserDn("User1") === null)
  }

  test("FindUserDnWhenUserPatternFoundByUniqueIdentifierNegativeMany") {
    conf.set(
      KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN,
      "CN=%s,OU=org1,DC=foo,DC=bar")
    val manyResult: NamingEnumeration[SearchResult] =
      mockNamingEnumeration("CN=User1,OU=org1,DC=foo,DC=bar", "CN=User12,OU=org1,DC=foo,DC=bar")
    when(ctx.search(anyString, anyString, any(classOf[SearchControls])))
      .thenReturn(null)
      .thenReturn(manyResult)
    search = new LdapSearch(conf, ctx)
    assert(search.findUserDn("User1") === null)
  }

  test("FindGroupsForUser") {
    conf.set(
      KyuubiConf.AUTHENTICATION_LDAP_GROUP_DN_PATTERN,
      "CN=%s,OU=org1,DC=foo,DC=bar")
    val groupsResult: NamingEnumeration[SearchResult] =
      mockNamingEnumeration("CN=Group1,OU=org1,DC=foo,DC=bar")
    when(
      ctx.search(
        mockEq("OU=org1,DC=foo,DC=bar"),
        contains("User1"),
        any(classOf[SearchControls]))).thenReturn(groupsResult)
    search = new LdapSearch(conf, ctx)
    val expected = Array("CN=Group1,OU=org1,DC=foo,DC=bar")
    val actual = search.findGroupsForUser("CN=User1,OU=org1,DC=foo,DC=bar")
    assert(expected === actual)
  }

  test("ExecuteCustomQuery") {
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_BASE_DN, "dc=example,dc=com")
    val customQueryResult: NamingEnumeration[SearchResult] = mockNamingEnumeration(Array(
      mockSearchResult(
        "uid=group1,ou=Groups,dc=example,dc=com",
        mockAttributes("member", "uid=user1,ou=People,dc=example,dc=com")),
      mockSearchResult(
        "uid=group2,ou=Groups,dc=example,dc=com",
        mockAttributes("member", "uid=user2,ou=People,dc=example,dc=com"))))
    when(
      ctx.search(
        mockEq("dc=example,dc=com"),
        anyString,
        any(classOf[SearchControls])))
      .thenReturn(customQueryResult)
    search = new LdapSearch(conf, ctx)
    val expected = Array(
      "uid=group1,ou=Groups,dc=example,dc=com",
      "uid=user1,ou=People,dc=example,dc=com",
      "uid=group2,ou=Groups,dc=example,dc=com",
      "uid=user2,ou=People,dc=example,dc=com")
    val actual = search.executeCustomQuery("(&(objectClass=groupOfNames)(|(cn=group1)(cn=group2)))")
    assert(expected.sorted === actual.sorted)
  }

  test("FindGroupDnPositive") {
    conf.set(
      KyuubiConf.AUTHENTICATION_LDAP_GROUP_DN_PATTERN,
      "CN=%s,OU=org1,DC=foo,DC=bar")
    val groupDn: String = "CN=Group1"
    val result: NamingEnumeration[SearchResult] = mockNamingEnumeration(groupDn)
    when(ctx.search(anyString, anyString, any(classOf[SearchControls])))
      .thenReturn(result)
    search = new LdapSearch(conf, ctx)
    val expected: String = groupDn
    val actual: String = search.findGroupDn("grp1")
    assert(expected === actual)
  }

  test("FindGroupDNNoResults") {
    intercept[NamingException] {
      conf.set(
        KyuubiConf.AUTHENTICATION_LDAP_GROUP_DN_PATTERN,
        "CN=%s,OU=org1,DC=foo,DC=bar")
      val result: NamingEnumeration[SearchResult] = mockEmptyNamingEnumeration
      when(ctx.search(anyString, anyString, any(classOf[SearchControls])))
        .thenReturn(result)
      search = new LdapSearch(conf, ctx)
      search.findGroupDn("anyGroup")
    }
  }

  test("FindGroupDNTooManyResults") {
    intercept[NamingException] {
      conf.set(
        KyuubiConf.AUTHENTICATION_LDAP_GROUP_DN_PATTERN,
        "CN=%s,OU=org1,DC=foo,DC=bar")
      val result: NamingEnumeration[SearchResult] =
        LdapTestUtils.mockNamingEnumeration("Result1", "Result2", "Result3")
      when(ctx.search(anyString, anyString, any(classOf[SearchControls])))
        .thenReturn(result)
      search = new LdapSearch(conf, ctx)
      search.findGroupDn("anyGroup")
    }

  }

  test("FindGroupDNWhenExceptionInSearch") {
    conf.set(
      KyuubiConf.AUTHENTICATION_LDAP_GROUP_DN_PATTERN,
      Array("CN=%s,OU=org1,DC=foo,DC=bar", "CN=%s,OU=org2,DC=foo,DC=bar").mkString(":"))
    val result: NamingEnumeration[SearchResult] = LdapTestUtils.mockNamingEnumeration("CN=Group1")
    when(ctx.search(anyString, anyString, any(classOf[SearchControls])))
      .thenReturn(result)
      .thenThrow(classOf[NamingException])
    search = new LdapSearch(conf, ctx)
    val expected: String = "CN=Group1"
    val actual: String = search.findGroupDn("grp1")
    assert(expected === actual)
  }

  test("IsUserMemberOfGroupWhenUserId") {
    conf.set(
      KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN,
      "CN=%s,OU=org1,DC=foo,DC=bar")
    val validResult: NamingEnumeration[SearchResult] =
      LdapTestUtils.mockNamingEnumeration("CN=User1")
    val emptyResult: NamingEnumeration[SearchResult] = LdapTestUtils.mockEmptyNamingEnumeration
    when(ctx.search(anyString, contains("(uid=usr1)"), any(classOf[SearchControls])))
      .thenReturn(validResult)
    when(ctx.search(anyString, contains("(uid=usr2)"), any(classOf[SearchControls])))
      .thenReturn(emptyResult)
    search = new LdapSearch(conf, ctx)
    assert(search.isUserMemberOfGroup("usr1", "grp1"))
    assert(!search.isUserMemberOfGroup("usr2", "grp2"))
  }

  test("IsUserMemberOfGroupWhenUserDn") {
    conf.set(
      KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN,
      "CN=%s,OU=org1,DC=foo,DC=bar")
    val validResult: NamingEnumeration[SearchResult] =
      LdapTestUtils.mockNamingEnumeration("CN=User1")
    val emptyResult: NamingEnumeration[SearchResult] = LdapTestUtils.mockEmptyNamingEnumeration
    when(ctx.search(anyString, contains("(uid=User1)"), any(classOf[SearchControls])))
      .thenReturn(validResult)
    when(ctx.search(anyString, contains("(uid=User2)"), any(classOf[SearchControls])))
      .thenReturn(emptyResult)
    search = new LdapSearch(conf, ctx)
    assert(search.isUserMemberOfGroup("CN=User1,OU=org1,DC=foo,DC=bar", "grp1"))
    assert(!search.isUserMemberOfGroup("CN=User2,OU=org1,DC=foo,DC=bar", "grp2"))
  }
}
