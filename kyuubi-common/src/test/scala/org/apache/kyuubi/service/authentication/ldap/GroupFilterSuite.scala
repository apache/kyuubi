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

import javax.security.sasl.AuthenticationException

import org.mockito.ArgumentMatchers.{eq => mockEq}
import org.mockito.Mockito.{lenient, when}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

class GroupFilterSuite extends KyuubiFunSuite {
  private val factory: FilterFactory = GroupFilterFactory
  private var conf: KyuubiConf = _
  private var search: DirSearch = _

  override def beforeEach(): Unit = {
    conf = new KyuubiConf
    search = mock[DirSearch]
    super.beforeEach()
  }

  test("GetInstanceWhenGroupFilterIsEmpty") {
    conf.unset(KyuubiConf.AUTHENTICATION_LDAP_GROUP_FILTER)
    assert(factory.getInstance(conf).isEmpty)
  }

  test("GetInstanceOfGroupMembershipKeyFilter") {
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_GROUP_FILTER.key, "G1")
    val instance: Filter = factory.getInstance(conf).get
    assert(instance.isInstanceOf[GroupMembershipKeyFilter])
  }

  test("GetInstanceOfUserMembershipKeyFilter") {
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_GROUP_FILTER.key, "G1")
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_USER_MEMBERSHIP_KEY, "memberof")
    val instance: Filter = factory.getInstance(conf).get
    assert(instance.isInstanceOf[UserMembershipKeyFilter])
  }

  test("GroupMembershipKeyFilterApplyPositive") {
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_GROUP_FILTER.key, "HiveUsers")
    when(search.findUserDn(mockEq("user1")))
      .thenReturn("cn=user1,ou=People,dc=example,dc=com")
    when(search.findUserDn(mockEq("cn=user2,dc=example,dc=com")))
      .thenReturn("cn=user2,ou=People,dc=example,dc=com")
    when(search.findUserDn(mockEq("user3@mydomain.com")))
      .thenReturn("cn=user3,ou=People,dc=example,dc=com")
    when(search.findGroupsForUser(mockEq("cn=user1,ou=People,dc=example,dc=com")))
      .thenReturn(Array(
        "cn=SuperUsers,ou=Groups,dc=example,dc=com",
        "cn=Office1,ou=Groups,dc=example,dc=com",
        "cn=HiveUsers,ou=Groups,dc=example,dc=com",
        "cn=G1,ou=Groups,dc=example,dc=com"))
    when(search.findGroupsForUser(mockEq("cn=user2,ou=People,dc=example,dc=com")))
      .thenReturn(Array("cn=HiveUsers,ou=Groups,dc=example,dc=com"))
    when(search.findGroupsForUser(mockEq("cn=user3,ou=People,dc=example,dc=com")))
      .thenReturn(Array(
        "cn=HiveUsers,ou=Groups,dc=example,dc=com",
        "cn=G1,ou=Groups,dc=example,dc=com",
        "cn=G2,ou=Groups,dc=example,dc=com"))
    val filter: Filter = factory.getInstance(conf).get
    filter.apply(search, "user1")
    filter.apply(search, "cn=user2,dc=example,dc=com")
    filter.apply(search, "user3@mydomain.com")
  }

  test("GroupMembershipKeyCaseInsensitiveFilterApplyPositive") {
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_GROUP_FILTER.key, "hiveusers,g1")
    when(search.findUserDn(mockEq("user1")))
      .thenReturn("cn=user1,ou=People,dc=example,dc=com")
    when(search.findUserDn(mockEq("cn=user2,dc=example,dc=com")))
      .thenReturn("cn=user2,ou=People,dc=example,dc=com")
    when(search.findUserDn(mockEq("user3@mydomain.com")))
      .thenReturn("cn=user3,ou=People,dc=example,dc=com")
    when(search.findGroupsForUser(mockEq("cn=user1,ou=People,dc=example,dc=com")))
      .thenReturn(Array(
        "cn=SuperUsers,ou=Groups,dc=example,dc=com",
        "cn=Office1,ou=Groups,dc=example,dc=com",
        "cn=HiveUsers,ou=Groups,dc=example,dc=com",
        "cn=G1,ou=Groups,dc=example,dc=com"))
    when(search.findGroupsForUser(mockEq("cn=user2,ou=People,dc=example,dc=com")))
      .thenReturn(Array("cn=HiveUsers,ou=Groups,dc=example,dc=com"))
    when(search.findGroupsForUser(mockEq("cn=user3,ou=People,dc=example,dc=com")))
      .thenReturn(Array(
        "cn=G1,ou=Groups,dc=example,dc=com",
        "cn=G2,ou=Groups,dc=example,dc=com"))
    val filter: Filter = factory.getInstance(conf).get
    filter.apply(search, "user1")
    filter.apply(search, "cn=user2,dc=example,dc=com")
    filter.apply(search, "user3@mydomain.com")
  }

  test("GroupMembershipKeyCaseInsensitiveFilterApplyNegative") {
    intercept[AuthenticationException] {
      conf.set(KyuubiConf.AUTHENTICATION_LDAP_GROUP_FILTER.key, "hiveusers,containsg1")
      lenient.when(search.findGroupsForUser(mockEq("user1")))
        .thenReturn(Array("SuperUsers", "Office1", "G1", "G2"))
      val filter: Filter = factory.getInstance(conf).get
      filter.apply(search, "user1")
    }
  }

  test("GroupMembershipKeyFilterApplyNegative") {
    intercept[AuthenticationException] {
      conf.set(KyuubiConf.AUTHENTICATION_LDAP_GROUP_FILTER.key, "HiveUsers")
      lenient.when(search.findGroupsForUser(mockEq("user1")))
        .thenReturn(Array("SuperUsers", "Office1", "G1", "G2"))
      val filter: Filter = factory.getInstance(conf).get
      filter.apply(search, "user1")
    }
  }

  test("UserMembershipKeyFilterApplyPositiveWithUserId") {
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_USER_MEMBERSHIP_KEY.key, "memberOf")
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_GROUP_FILTER.key, "Group1,Group2")
    when(search.findGroupDn("Group1")).thenReturn("cn=Group1,dc=a,dc=b")
    when(search.findGroupDn("Group2")).thenReturn("cn=Group2,dc=a,dc=b")
    when(search.isUserMemberOfGroup("User1", "cn=Group2,dc=a,dc=b")).thenReturn(true)
    val filter: Filter = factory.getInstance(conf).get
    filter.apply(search, "User1")
  }

  test("UserMembershipKeyFilterApplyPositiveWithUserDn") {
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_USER_MEMBERSHIP_KEY.key, "memberOf")
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_GROUP_FILTER.key, "Group1,Group2")
    when(search.findGroupDn("Group1")).thenReturn("cn=Group1,dc=a,dc=b")
    when(search.findGroupDn("Group2")).thenReturn("cn=Group2,dc=a,dc=b")
    when(search.isUserMemberOfGroup("cn=User1,dc=a,dc=b", "cn=Group2,dc=a,dc=b")).thenReturn(true)
    val filter: Filter = factory.getInstance(conf).get
    filter.apply(search, "cn=User1,dc=a,dc=b")
  }

  test("UserMembershipKeyFilterApplyNegative") {
    intercept[AuthenticationException] {
      conf.set(KyuubiConf.AUTHENTICATION_LDAP_USER_MEMBERSHIP_KEY.key, "memberOf")
      conf.set(KyuubiConf.AUTHENTICATION_LDAP_GROUP_FILTER.key, "Group1,Group2")
      when(search.findGroupDn("Group1")).thenReturn("cn=Group1,dc=a,dc=b")
      when(search.findGroupDn("Group2")).thenReturn("cn=Group2,dc=a,dc=b")
      val filter: Filter = factory.getInstance(conf).get
      filter.apply(search, "User1")
    }
  }
}
