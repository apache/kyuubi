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

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf

object UserFilterFactory extends FilterFactory with Logging {
  override def getInstance(conf: KyuubiConf): Option[Filter] = {
    val userFilter = conf.get(KyuubiConf.AUTHENTICATION_LDAP_USER_FILTER)
    if (userFilter.isEmpty) None else Some(new UserFilter(userFilter))
  }
}

class UserFilter(_userFilter: Set[String]) extends Filter with Logging {

  lazy val userFilter: Set[String] = _userFilter.map(_.toLowerCase)

  @throws[AuthenticationException]
  override def apply(ldap: DirSearch, user: String): Unit = {
    info(s"Authenticating user '$user' using user filter")
    val userName = LdapUtils.extractUserName(user).toLowerCase
    if (!userFilter.contains(userName)) {
      info("Authentication failed based on user membership")
      throw new AuthenticationException(
        "Authentication failed: User not a member of specified list")
    }
  }
}
