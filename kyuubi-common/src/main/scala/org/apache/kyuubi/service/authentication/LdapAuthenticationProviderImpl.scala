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

package org.apache.kyuubi.service.authentication

import javax.naming.{Context, NamingException}
import javax.naming.directory.{InitialDirContext, SearchControls, SearchResult}
import javax.naming.ldap.InitialLdapContext
import javax.security.sasl.AuthenticationException

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.service.ServiceUtils

class LdapAuthenticationProviderImpl(conf: KyuubiConf) extends PasswdAuthenticationProvider {

  /**
   * The authenticate method is called by the Kyuubi Server authentication layer
   * to authenticate users for their requests.
   * If a user is to be granted, return nothing/throw nothing.
   * When a user is to be disallowed, throw an appropriate [[AuthenticationException]].
   *
   * @param user     The username received over the connection request
   * @param password The password received over the connection request
   *
   * @throws AuthenticationException When a user is found to be invalid by the implementation
   */
  override def authenticate(user: String, password: String): Unit = {
    if (StringUtils.isBlank(user)) {
      throw new AuthenticationException(s"Error validating LDAP user, user is null" +
        s" or contains blank space")
    }

    if (StringUtils.isBlank(password)) {
      throw new AuthenticationException(s"Error validating LDAP user, password is null" +
        s" or contains blank space")
    }

    val env = new java.util.Hashtable[String, Any]()
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
    env.put(Context.SECURITY_AUTHENTICATION, "simple")

    conf.get(AUTHENTICATION_LDAP_URL).foreach(env.put(Context.PROVIDER_URL, _))

    val guidKey = conf.get(AUTHENTICATION_LDAP_GUIDKEY)
    val baseDn = conf.get(AUTHENTICATION_LDAP_BASEDN).get
    val bindnPw = conf.get(AUTHENTICATION_LDAP_PASSWORD).get
    val attrs = conf.get(AUTHENTICATION_LDAP_ATTRIBUTES).toArray

    env.put(Context.SECURITY_PRINCIPAL, guidKey)
    env.put(Context.SECURITY_CREDENTIALS, bindnPw)
    import javax.naming.NamingEnumeration
    var nameEnuResults: NamingEnumeration[SearchResult] = null
    try {
      val ctx = new InitialLdapContext(env, null)
      val sc = new SearchControls
      sc.setReturningAttributes(attrs)
      sc.setSearchScope(SearchControls.SUBTREE_SCOPE)
      val searchFilter = String.format("(%s=%s)", "name", user)
      nameEnuResults = ctx.search(baseDn, searchFilter, sc)
    } catch {
      case e: NamingException =>
        throw new AuthenticationException(
          s"LDAP InitialLdapContext failed, LDAP user: $user, " +
            s"Error validating LDAP baseDn: $baseDn",
          e)
    }
    if (nameEnuResults != null && nameEnuResults.hasMore) {
      val searchResult = nameEnuResults.next
      val attrs = searchResult.getAttributes.getAll
      while (attrs.hasMore) {
        attrs.next
        env.put(Context.SECURITY_PRINCIPAL, searchResult.getNameInNamespace)
        env.put(Context.SECURITY_CREDENTIALS, password)
        try {
          val ctx = new InitialDirContext(env)
          ctx.close()
        } catch {
          case e: NamingException =>
            throw new AuthenticationException(
              s"LDAP InitialDirContext failed, LDAP user: $user, " +
                s"Error validating LDAP baseDn: $baseDn",
              e)
        }
      }
    }

  }

  private def hasDomain(userName: String): Boolean = ServiceUtils.indexOfDomainMatch(userName) > 0
}
