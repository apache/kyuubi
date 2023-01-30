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

import org.apache.kyuubi.config.KyuubiConf

/**
 * A factory for common types of directory service search queries.
 */
final class QueryFactory(conf: KyuubiConf) {
  private val USER_OBJECT_CLASSES = Array("person", "user", "inetOrgPerson")

  private val guidAttr = conf.get(KyuubiConf.AUTHENTICATION_LDAP_GUID_KEY)
  private val groupClassAttr = conf.get(KyuubiConf.AUTHENTICATION_LDAP_GROUP_CLASS_KEY)
  private val groupMembershipAttr = conf.get(KyuubiConf.AUTHENTICATION_LDAP_GROUP_MEMBERSHIP_KEY)
  private val userMembershipAttrOpt = conf.get(KyuubiConf.AUTHENTICATION_LDAP_USER_MEMBERSHIP_KEY)

  /**
   * Returns a query for finding Group DN based on group unique ID.
   *
   * @param groupId group unique identifier
   * @return an instance of [[Query]]
   */
  def findGroupDnById(groupId: String): Query = Query.builder
    .filter("(&(objectClass=<groupClassAttr>)(<guidAttr>=<groupID>))")
    .map("guidAttr", guidAttr)
    .map("groupClassAttr", groupClassAttr)
    .map("groupID", groupId).limit(2)
    .build

  /**
   * Returns a query for finding user DN based on user RDN.
   *
   * @param userRdn user RDN
   * @return an instance of [[Query]]
   */
  def findUserDnByRdn(userRdn: String): Query = Query.builder
    .filter("(&(|<classes:{ class |(objectClass=<class>)}>)(<userRdn>))")
    .limit(2)
    .map("classes", USER_OBJECT_CLASSES)
    .map("userRdn", userRdn).build

  /**
   * Returns a query for finding user DN based on DN pattern.
   * <br>
   * Name of this method was derived from the original implementation of LDAP authentication.
   * This method should be replaced by [[QueryFactory.findUserDnByRdn]].
   *
   * @param rdn user RDN
   * @return an instance of [[Query]]
   */
  def findDnByPattern(rdn: String): Query = Query.builder
    .filter("(<rdn>)")
    .map("rdn", rdn)
    .limit(2)
    .build

  /**
   * Returns a query for finding user DN based on user unique name.
   *
   * @param userName user unique name (uid or sAMAccountName)
   * @return an instance of [[Query]]
   */
  def findUserDnByName(userName: String): Query = Query.builder
    .filter("(&(|<classes:{ class |(objectClass=<class>)}>)" +
      "(|(uid=<userName>)(sAMAccountName=<userName>)))")
    .map("classes", USER_OBJECT_CLASSES)
    .map("userName", userName)
    .limit(2)
    .build

  /**
   * Returns a query for finding groups to which the user belongs.
   *
   * @param userName username
   * @param userDn   user DN
   * @return an instance of [[Query]]
   */
  def findGroupsForUser(userName: String, userDn: String): Query = Query.builder
    .filter("(&(objectClass=<groupClassAttr>)" +
      "(|(<groupMembershipAttr>=<userDn>)(<groupMembershipAttr>=<userName>)))")
    .map("groupClassAttr", groupClassAttr)
    .map("groupMembershipAttr", groupMembershipAttr)
    .map("userName", userName)
    .map("userDn", userDn)
    .build

  /**
   * Returns a query for checking whether specified user is a member of specified group.
   *
   * The query requires [[KyuubiConf.AUTHENTICATION_LDAP_USER_MEMBERSHIP_KEY]]
   * configuration property to be set.
   *
   * @param userId  user unique identifier
   * @param groupDn group DN
   * @return an instance of [[Query]]
   * @see [[KyuubiConf.AUTHENTICATION_LDAP_USER_MEMBERSHIP_KEY]]
   */
  def isUserMemberOfGroup(userId: String, groupDn: String): Query = {
    require(
      userMembershipAttrOpt.isDefined,
      s"${KyuubiConf.AUTHENTICATION_LDAP_USER_MEMBERSHIP_KEY.key} is not configured.")

    Query.builder
      .filter("(&(|<classes:{ class |(objectClass=<class>)}>)" +
        "(<userMembershipAttr>=<groupDn>)(<guidAttr>=<userId>))")
      .map("classes", USER_OBJECT_CLASSES)
      .map("guidAttr", guidAttr)
      .map("userMembershipAttr", userMembershipAttrOpt.get)
      .map("userId", userId)
      .map("groupDn", groupDn)
      .limit(2)
      .build
  }

  /**
   * Returns a query object created for the custom filter.
   * <br>
   * This query is configured to return a group membership attribute as part of the search result.
   *
   * @param searchFilter custom search filter
   * @return an instance of [[Query]]
   */
  def customQuery(searchFilter: String): Query = {
    val builder = Query.builder
    builder.filter(searchFilter)
    builder.returnAttribute(groupMembershipAttr)
    builder.build
  }
}
