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

import org.apache.kyuubi.config.KyuubiConf

/**
 * A factory for a [[Filter]] that check whether provided user could be found in the directory.
 * <br>
 * The produced filter object filters out all users that are not found in the directory.
 */
object UserSearchFilterFactory extends FilterFactory {
  override def getInstance(conf: KyuubiConf): Option[Filter] = {
    val groupFilter = conf.get(KyuubiConf.AUTHENTICATION_LDAP_GROUP_FILTER)
    val userFilter = conf.get(KyuubiConf.AUTHENTICATION_LDAP_USER_FILTER)
    if (groupFilter.isEmpty && userFilter.isEmpty) None else Some(UserSearchFilter)
  }
}

object UserSearchFilter extends Filter {
  @throws[AuthenticationException]
  override def apply(client: DirSearch, user: String): Unit = {
    try {
      val userDn = client.findUserDn(user)
      // This should not be null because we were allowed to bind with this username
      // safe check in case we were able to bind anonymously.
      if (userDn == null) {
        throw new AuthenticationException("Authentication failed: User search failed")
      }
    } catch {
      case e: NamingException =>
        throw new AuthenticationException("LDAP Authentication failed for user", e)
    }
  }
}
