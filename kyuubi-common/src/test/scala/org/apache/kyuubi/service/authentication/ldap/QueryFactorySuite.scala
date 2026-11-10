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

class QueryFactorySuite extends KyuubiFunSuite {
  private var conf: KyuubiConf = _
  private var queries: QueryFactory = _

  override def beforeEach(): Unit = {
    conf = new KyuubiConf()
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_GUID_KEY, "guid")
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_GROUP_CLASS_KEY, "superGroups")
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_GROUP_MEMBERSHIP_KEY, "member")
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_USER_MEMBERSHIP_KEY, "partOf")
    queries = new QueryFactory(conf)
    super.beforeEach()
  }

  test("FindGroupDnById") {
    assert(queries.findGroupDnById("unique_group_id").filter ===
      "(&(objectClass=superGroups)(guid=unique_group_id))")
    assert(queries.findGroupDnById("unique*group_id").filter ===
      "(&(objectClass=superGroups)(guid=unique\\2agroup_id))")
  }

  test("FindUserDnByRdn") {
    val q = queries.findUserDnByRdn("cn=User1")
    val expected =
      "(&(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))(cn=User1))"
    val actual = q.filter
    assert(expected === actual)
  }

  test("FindDnByPattern") {
    val q = queries.findDnByPattern("cn=User1")
    val expected = "(cn=User1)"
    val actual = q.filter
    assert(expected === actual)
  }

  test("FindUserDnByName") {
    val q = queries.findUserDnByName("unique_user_id")
    val expected =
      "(&(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))" +
        "(|(uid=unique_user_id)(sAMAccountName=unique_user_id)))"
    val actual = q.filter
    assert(expected === actual)
  }

  test("FindGroupsForUser") {
    val q = queries.findGroupsForUser("user_name", "user_Dn")
    val expected = "(&(objectClass=superGroups)(|(member=user_Dn)(member=user_name)))"
    val actual = q.filter
    assert(expected === actual)
  }

  test("IsUserMemberOfGroup") {
    val q = queries.isUserMemberOfGroup("unique_user", "cn=MyGroup,ou=Groups,dc=mycompany,dc=com")
    val expected =
      "(&(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))" +
        "(partOf=cn=MyGroup,ou=Groups,dc=mycompany,dc=com)(guid=unique_user))"
    val actual = q.filter
    assert(expected === actual)
  }

  test("IsUserMemberOfGroupWhenMisconfigured") {
    intercept[IllegalArgumentException] {
      val misconfiguredQueryFactory = new QueryFactory(new KyuubiConf())
      misconfiguredQueryFactory.isUserMemberOfGroup("user", "cn=MyGroup")
    }
  }

  test("FindUserDnByNameWithSpecialCharacters") {
    val q = queries.findUserDnByName("user1*)(objectClass=*")
    val expected =
      "(&(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))" +
        "(|(uid=user1\\2a\\29\\28objectClass=\\2a)" +
        "(sAMAccountName=user1\\2a\\29\\28objectClass=\\2a)))"
    assert(expected === q.filter)
  }

  test("FindGroupsForUserWithSpecialCharacters") {
    val q = queries.findGroupsForUser("user1*", "user_Dn")
    val expected = "(&(objectClass=superGroups)(|(member=user_Dn)(member=user1\\2a)))"
    assert(expected === q.filter)
  }

  test("FindGroupsForUserWithSpecialCharactersInUserDn") {
    val q = queries.findGroupsForUser("user_name", "uid=user_Dn*,ou=People,dc=example,dc=com")
    val expected =
      "(&(objectClass=superGroups)" +
        "(|(member=uid=user_Dn\\2a,ou=People,dc=example,dc=com)(member=user_name)))"
    assert(expected === q.filter)
  }

  test("IsUserMemberOfGroupWithSpecialCharacters") {
    // both the client-supplied user and the directory-sourced group DN are embedded in the
    // filter, so both need escaping
    val q = queries.isUserMemberOfGroup("user1*", "cn=My*Group,ou=Groups,dc=mycompany,dc=com")
    val expected =
      "(&(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))" +
        "(partOf=cn=My\\2aGroup,ou=Groups,dc=mycompany,dc=com)(guid=user1\\2a))"
    assert(expected === q.filter)
  }
}
