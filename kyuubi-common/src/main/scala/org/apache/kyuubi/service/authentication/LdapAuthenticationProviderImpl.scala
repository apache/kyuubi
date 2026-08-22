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

import javax.naming.NamingException
import javax.security.sasl.AuthenticationException

import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.ServiceUtils
import org.apache.kyuubi.service.authentication.LdapAuthenticationProviderImpl.FILTER_FACTORIES
import org.apache.kyuubi.service.authentication.ldap._
import org.apache.kyuubi.service.authentication.ldap.LdapUtils.getUserName

class LdapAuthenticationProviderImpl(
    conf: KyuubiConf,
    searchFactory: DirSearchFactory = new LdapSearchFactory)
  extends PasswdAuthenticationProvider with Logging {

  private val filterOpt: Option[Filter] = FILTER_FACTORIES
    .map { f => f.getInstance(conf) }
    .collectFirst { case Some(f: Filter) => f }

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
    // The actual auth flow is delegated to doAuthenticate; this wrapper is the single
    // place where the kyuubi.authentication.success / failure metric counters are
    // updated, classified by [[LdapAuthFailureClassifier]] for failures.
    try {
      doAuthenticate(user, password)
      AuthenticationProviderFactory.recordLdapAuthSuccess()
    } catch {
      case e: AuthenticationException =>
        AuthenticationProviderFactory.recordLdapAuthFailure(LdapAuthFailureClassifier.classify(e))
        throw e
    }
  }

  private def doAuthenticate(user: String, password: String): Unit = {
    val (usedBind, bindUser, bindPassword) = (
      conf.get(KyuubiConf.AUTHENTICATION_LDAP_BIND_USER),
      conf.get(KyuubiConf.AUTHENTICATION_LDAP_BIND_PASSWORD)) match {
      case (Some(_bindUser), Some(_bindPw)) => (true, _bindUser, _bindPw)
      case _ =>
        // If no bind user or bind password was specified,
        // we assume the user we are authenticating has the ability to search
        // the LDAP tree, so we use it as the "binding" account.
        // This is the way it worked before bind users were allowed in the LDAP authenticator,
        // so we keep existing systems working.
        (false, user, password)
    }

    var search: DirSearch = null
    // The end-user credential-verification DirSearch (bind-user flow only) wraps an
    // ephemeral LDAPConnection -- its close() is the only path that frees the underlying
    // socket. Tracked separately so the finally block can close both.
    var userSearch: DirSearch = null
    try {
      search = createDirSearch(bindUser, bindPassword)
      applyFilter(search, user)
      if (usedBind) {
        // If we used the bind user, then we need to authenticate again,
        // this time using the full user name we got during the bind process.
        val username = getUserName(user)
        userSearch = createDirSearch(search.findUserDn(username), password)
      }
    } catch {
      case e: NamingException =>
        // Chain the cause so the original LDAPException (result code, server address) is
        // visible in audit logs and stack traces. Classification is unaffected: the message
        // prefix falls through to classifyByMessage -> INFRASTRUCTURE regardless, but
        // preserving the cause allows classifyByCause to also reach that result via the
        // NamingException branch, and keeps the full diagnostic chain intact for operators.
        throw new AuthenticationException(
          s"Unable to find the user in the LDAP tree. ${e.getMessage}",
          e)
      case e: IllegalStateException =>
        // Query.QueryBuilder.build wraps an unparseable rendered filter as
        // IllegalStateException. The most common trigger is a username containing
        // LDAP filter metacharacters ("*", "(", ")", "\") that break RFC 4515 after
        // template substitution. Surface this as an AuthenticationException so the
        // metric recorder/classifier sees it (invalid_input bucket) instead of letting
        // an unclassified RuntimeException escape the auth contract.
        throw new AuthenticationException(
          s"Error validating LDAP user, invalid filter for $user: ${e.getMessage}",
          e)
    } finally {
      ServiceUtils.cleanup(logger, search, userSearch)
    }
  }

  @throws[AuthenticationException]
  private def createDirSearch(user: String, password: String): DirSearch = {
    if (StringUtils.isBlank(user)) {
      throw new AuthenticationException(s"Error validating LDAP user, user is null" +
        s" or contains blank space")
    }

    if (StringUtils.isBlank(password) || password.getBytes()(0) == 0) {
      throw new AuthenticationException(s"Error validating LDAP user, password is null" +
        s" or contains blank space")
    }

    val principals = LdapUtils.createCandidatePrincipals(conf, user)
    val iterator = principals.iterator
    while (iterator.hasNext) {
      val principal = iterator.next
      try {
        return searchFactory.getInstance(conf, principal, password)
      } catch {
        case ex: AuthenticationException => if (iterator.isEmpty) throw ex
      }
    }
    throw new AuthenticationException(s"No candidate principals for $user was found.")
  }

  @throws[AuthenticationException]
  private def applyFilter(client: DirSearch, user: String): Unit = filterOpt.foreach { filter =>
    filter.apply(client, getUserName(user))
  }
}

object LdapAuthenticationProviderImpl {
  val FILTER_FACTORIES: Array[FilterFactory] = Array[FilterFactory](
    CustomQueryFilterFactory,
    new ChainFilterFactory(UserSearchFilterFactory, UserFilterFactory, GroupFilterFactory))
}
