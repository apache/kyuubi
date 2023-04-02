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

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf

/**
 * A factory for a [[Filter]] based on a custom query.
 * <br>
 * The produced filter object filters out all users that are not found in the search result
 * of the query provided in Kyuubi configuration.
 *
 * @see [[KyuubiConf.AUTHENTICATION_LDAP_CUSTOM_LDAP_QUERY]]
 */
object CustomQueryFilterFactory extends FilterFactory {
  override def getInstance(conf: KyuubiConf): Option[Filter] =
    conf.get(KyuubiConf.AUTHENTICATION_LDAP_CUSTOM_LDAP_QUERY)
      .map { customQuery => new CustomQueryFilter(customQuery) }
}
class CustomQueryFilter(query: String) extends Filter with Logging {
  @throws[AuthenticationException]
  override def apply(client: DirSearch, user: String): Unit = {
    var resultList: Array[String] = null
    try {
      resultList = client.executeCustomQuery(query)
    } catch {
      case e: NamingException =>
        throw new AuthenticationException(s"LDAP Authentication failed for $user", e)
    }
    if (resultList != null) {
      resultList.foreach { matchedDn =>
        val shortUserName = LdapUtils.getShortName(matchedDn)
        info(s"<queried user=$shortUserName,user=$user>")
        if (shortUserName.equalsIgnoreCase(user) || matchedDn.equalsIgnoreCase(user)) {
          info("Authentication succeeded based on result set from LDAP query")
          return
        }
      }
      // try a generic user search
      if (query.contains("%s")) {
        val userSearchQuery = query.replace("%s", user)
        info("Trying with generic user search in ldap:" + userSearchQuery)
        try resultList = client.executeCustomQuery(userSearchQuery)
        catch {
          case e: NamingException =>
            throw new AuthenticationException("LDAP Authentication failed for user", e)
        }
        if (resultList != null && resultList.length == 1) {
          info("Authentication succeeded based on result from custom user search query")
          return
        }
      }
    }
    info("Authentication failed based on result set from custom LDAP query")
    throw new AuthenticationException(
      "Authentication failed: LDAP query from property returned no data")
  }
}
