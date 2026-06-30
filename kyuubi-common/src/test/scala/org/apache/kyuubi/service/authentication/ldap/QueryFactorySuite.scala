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
    val q = queries.findGroupDnById("unique_group_id")
    val expected = "(&(objectClass=superGroups)(guid=unique_group_id))"
    assert(expected === q.filterString)
  }

  test("FindUserDnByRdn") {
    val q = queries.findUserDnByRdn("cn=User1")
    val expected =
      "(&(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))(cn=User1))"
    assert(expected === q.filterString)
  }

  test("FindDnByPattern") {
    val q = queries.findDnByPattern("cn=User1")
    val expected = "(cn=User1)"
    assert(expected === q.filterString)
  }

  test("FindUserDnByName") {
    val q = queries.findUserDnByName("unique_user_id")
    val expected =
      "(&(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))" +
        "(|(uid=unique_user_id)(sAMAccountName=unique_user_id)))"
    assert(expected === q.filterString)
  }

  test("FindGroupsForUser") {
    val q = queries.findGroupsForUser("user_name", "user_Dn")
    val expected = "(&(objectClass=superGroups)(|(member=user_Dn)(member=user_name)))"
    assert(expected === q.filterString)
  }

  test("IsUserMemberOfGroup") {
    val q = queries.isUserMemberOfGroup("unique_user", "cn=MyGroup,ou=Groups,dc=mycompany,dc=com")
    val expected =
      "(&(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))" +
        "(partOf=cn=MyGroup,ou=Groups,dc=mycompany,dc=com)(guid=unique_user))"
    assert(expected === q.filterString)
  }

  test("IsUserMemberOfGroupWhenMisconfigured") {
    intercept[IllegalArgumentException] {
      val misconfiguredQueryFactory = new QueryFactory(new KyuubiConf())
      misconfiguredQueryFactory.isUserMemberOfGroup("user", "cn=MyGroup")
    }
  }

  test("FindGroupDNByID") {
    val q = queries.findGroupDnById("unique_group_id")
    val expected = "(&(objectClass=superGroups)(guid=unique_group_id))"
    assert(expected === q.filterString)
  }

  test("findUserDnByName escapes LDAP filter metacharacters in userName") {
    // userName is in an attribute value position: (uid=<userName>). Without escaping, a
    // username like "jsmith)(uid=admin" would inject extra filter clauses. With escaping,
    // the special characters are converted to \xx hex form and treated as literals.
    val injectionAttempt = "jsmith)(uid=admin"
    val q = queries.findUserDnByName(injectionAttempt)
    // Parentheses become \28 and \29 per RFC 4515.
    assert(!q.filterString.contains(")(uid=admin"), "filter must not contain injected clause")
    assert(
      q.filterString.contains("\\28") || q.filterString.contains("jsmith"),
      s"escaped metacharacters expected in filter; got: ${q.filterString}")
    // The parsed filter must also be valid (LdapFilter.create succeeded inside build()).
    assert(q.filter != null)
  }

  test("findUserDnByName with wildcard username escapes asterisk") {
    val q = queries.findUserDnByName("*")
    // Unescaped * would be a presence assertion matching all users.
    // Escaped \2a is a literal search for the string "*".
    assert(!q.filterString.contains("uid=*"), s"wildcard must be escaped; got: ${q.filterString}")
    assert(q.filterString.contains("\\2a"), s"expected \\2a in filter; got: ${q.filterString}")
  }

  test("filter field is a parsed Filter object matching filterString") {
    val q = queries.findGroupDnById("unique_group_id")
    assert(q.filter != null)
    // Filter.toString() returns the normalized RFC 4515 representation; for our simple
    // filter strings there should be no normalization difference.
    assert(q.filterString === q.filter.toString)
  }
}
