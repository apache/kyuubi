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

import java.util
import java.util.Collections
import javax.security.sasl.AuthenticationException

import scala.collection.JavaConverters._

import com.unboundid.ldap.sdk.{LDAPConnection, LDAPException}
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

    var bindUser = conf.get(AUTHENTICATION_LDAP_BIND_USER).getOrElse("")
    var bindPassword = conf.get(AUTHENTICATION_LDAP_BIND_PASSWORD).getOrElse("")

    val usedBind = bindUser.nonEmpty && bindPassword.nonEmpty

    if (!usedBind) {
      // If no bind user or bind password was specified,
      // we assume the user we are authenticating has the ability to search
      // the LDAP tree, so we use it as the "binding" account.
      // This is the way it worked before bind users were allowed in the LDAP authenticator,
      // so we keep existing systems working.
      bindUser = user
      bindPassword = password
    }

    if (StringUtils.isBlank(bindUser)) {
      throw new AuthenticationException(s"Error validating LDAP user, user is null" +
        s" or contains blank space")
    }

    if (StringUtils.isBlank(bindPassword)) {
      throw new AuthenticationException(s"Error validating LDAP user, password is null" +
        s" or contains blank space")
    }

    val ldapUrl = conf.get(AUTHENTICATION_LDAP_URL).getOrElse("")
    val ldapUrlSplit = ldapUrl.split(":")
    if (ldapUrlSplit.size != 3) {
      throw new AuthenticationException(
        s"LDAP url is wrong, the provided url is $ldapUrl ," +
          s"the error validating LDAP user: $bindUser")
    }
    val ldapHost = ldapUrlSplit(1).stripPrefix("//")
    val ldapPort = ldapUrlSplit(2).toInt

    val candidatePrincipals = createCandidatePrincipals(conf, bindUser)
    if (candidatePrincipals.size() == 0) {
      throw new AuthenticationException(s"No candidate principals for $bindUser was found.")
    }
    val principalsItertor = candidatePrincipals.iterator()
    var ldapConnection: LDAPConnection = null
    while (principalsItertor.hasNext && ldapConnection == null) {
      val candidatePrincipal = principalsItertor.next()
      try {
        ldapConnection = new LDAPConnection(
          ldapHost,
          ldapPort,
          candidatePrincipal,
          bindPassword)
      } catch {
        case e: LDAPException =>
          if (!principalsItertor.hasNext) {
            throw new AuthenticationException(
              s"Error validating LDAP user: $user, bindDn: $candidatePrincipal.",
              e)
          }
      }
    }
    if (ldapConnection != null) {
      ldapConnection.close()
    }

  }

  private def hasDomain(userName: String): Boolean = ServiceUtils.indexOfDomainMatch(userName) > 0

  private def isDn(name: String): Boolean = name.contains("=")

  /**
   * Creates a list of principals to be used for user authentication.
   *
   * @param conf Kyuubi configuration
   * @param user username
   * @return a list of user's principals
   */
  private def createCandidatePrincipals(conf: KyuubiConf, user: String): util.List[String] = {
    if (hasDomain(user) || isDn(user)) return Collections.singletonList(user)
    val ldapDomain = conf.get(AUTHENTICATION_LDAP_DOMAIN).getOrElse("")
    if (StringUtils.isNotBlank(ldapDomain)) {
      return Collections.singletonList(user + "@" + ldapDomain)
    }
    val userPatterns = parseDnPatterns(conf)
    if (userPatterns.isEmpty) return Collections.singletonList(user)
    val candidatePrincipals = new util.ArrayList[String]
    for (userPattern <- userPatterns.asScala) {
      candidatePrincipals.add(userPattern.replaceAll("%s", user))
    }
    candidatePrincipals
  }

  /**
   * Reads and parses DN patterns from Kyuubi configuration.
   * <br>
   * If no patterns are provided in the configuration, then the base DN will be used.
   *
   * @param conf Kyuubi configuration
   * @return a list of DN patterns
   */
  private def parseDnPatterns(conf: KyuubiConf): util.List[String] = {
    val patternsString = conf.get(AUTHENTICATION_LDAP_USERDNPATTERN).getOrElse("")
    val result = new util.ArrayList[String]
    if (StringUtils.isBlank(patternsString)) {
      val defaultBaseDn = conf.get(AUTHENTICATION_LDAP_BASEDN).getOrElse("")
      val guidAttr = conf.get(AUTHENTICATION_LDAP_GUIDKEY)
      if (StringUtils.isNotBlank(defaultBaseDn)) result.add(guidAttr + "=%s," + defaultBaseDn)
    } else {
      val patterns = patternsString.split(":")
      for (pattern <- patterns) {
        if (pattern.contains(",") && pattern.contains("=")) result.add(pattern)
      }
    }
    result
  }

}
