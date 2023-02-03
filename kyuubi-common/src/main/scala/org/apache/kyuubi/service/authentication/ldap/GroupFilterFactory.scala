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

import javax.naming.NamingException
import javax.security.sasl.AuthenticationException

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf

object GroupFilterFactory extends FilterFactory {
  override def getInstance(conf: KyuubiConf): Option[Filter] = {
    val groupFilter = conf.get(KyuubiConf.AUTHENTICATION_LDAP_GROUP_FILTER)
    if (groupFilter.isEmpty) {
      None
    } else if (conf.get(KyuubiConf.AUTHENTICATION_LDAP_USER_MEMBERSHIP_KEY).isDefined) {
      Some(new UserMembershipKeyFilter(groupFilter))
    } else {
      Some(new GroupMembershipKeyFilter(groupFilter))
    }
  }
}

class GroupMembershipKeyFilter(groupFilter: Seq[String]) extends Filter with Logging {

  @throws[AuthenticationException]
  override def apply(ldap: DirSearch, user: String): Unit = {
    info(s"Authenticating user '$user' using ${classOf[GroupMembershipKeyFilter].getSimpleName})")

    var memberOf: Array[String] = null
    try {
      val userDn = ldap.findUserDn(user)
      // Workaround for magic things on Mockito:
      // unmatched invocation returns an empty list if the method return type is JList,
      // but null if the method return type is Array
      memberOf = Option(ldap.findGroupsForUser(userDn)).getOrElse(Array.empty)
      debug(s"User $userDn member of: ${memberOf.mkString(",")}")
    } catch {
      case e: NamingException =>
        throw new AuthenticationException("LDAP Authentication failed for user", e)
    }
    memberOf.foreach { groupDn =>
      val shortName = LdapUtils.getShortName(groupDn)
      if (groupFilter.exists(shortName.equalsIgnoreCase)) {
        debug(s"GroupMembershipKeyFilter passes: user '$user' is a member of '$groupDn' group")
        info("Authentication succeeded based on group membership")
        return
      }
    }
    info("Authentication failed based on user membership")
    throw new AuthenticationException(
      "Authentication failed: User not a member of specified list")
  }
}

class UserMembershipKeyFilter(groupFilter: Seq[String]) extends Filter with Logging {
  @throws[AuthenticationException]
  override def apply(ldap: DirSearch, user: String): Unit = {
    info(s"Authenticating user '$user' using $classOf[UserMembershipKeyFilter].getSimpleName")
    val groupDns = new ArrayBuffer[String]
    groupFilter.foreach { groupId =>
      try {
        val groupDn = ldap.findGroupDn(groupId)
        groupDns += groupDn
      } catch {
        case e: NamingException =>
          warn("Cannot find DN for group", e)
          debug(s"Cannot find DN for group $groupId", e)
      }
    }
    if (groupDns.isEmpty) {
      debug(s"No DN(s) has been found for any of group(s): ${groupFilter.mkString(",")}")
      throw new AuthenticationException("No DN(s) has been found for any of specified group(s)")
    }
    groupDns.foreach { groupDn =>
      try {
        if (ldap.isUserMemberOfGroup(user, groupDn)) {
          debug(s"UserMembershipKeyFilter passes: user '$user' is a member of '$groupDn' group")
          info("Authentication succeeded based on user membership")
          return
        }
      } catch {
        case e: NamingException =>
          warn("Cannot match user and group", e)
          debug(s"Cannot match user '$user' and group '$groupDn'", e)
      }
    }
    throw new AuthenticationException(
      s"Authentication failed: User '$user' is not a member of listed groups")
  }
}
