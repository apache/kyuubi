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

import java.io.Closeable
import javax.naming.NamingException

/**
 * The object used for executing queries on the Directory Service.
 */
trait DirSearch extends Closeable {

  /**
   * Finds user's distinguished name.
   *
   * @param user username
   * @return DN for the specified username
   */
  @throws[NamingException]
  def findUserDn(user: String): String

  /**
   * Finds group's distinguished name.
   *
   * @param group group name or unique identifier
   * @return DN for the specified group name
   */
  @throws[NamingException]
  def findGroupDn(group: String): String

  /**
   * Verifies that specified user is a member of specified group.
   *
   * @param user    user id or distinguished name
   * @param groupDn group's DN
   * @return true if the user is a member of the group, false - otherwise.
   */
  @throws[NamingException]
  def isUserMemberOfGroup(user: String, groupDn: String): Boolean

  /**
   * Finds groups that contain the specified user.
   *
   * @param userDn user's distinguished name
   * @return list of groups
   */
  @throws[NamingException]
  def findGroupsForUser(userDn: String): Array[String]

  /**
   * Executes an arbitrary query.
   *
   * @param query any query
   * @return list of names in the namespace
   */
  @throws[NamingException]
  def executeCustomQuery(query: String): Array[String]
}
