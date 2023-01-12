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

import scala.collection.mutable

import org.scalatest.Assertions.{fail, intercept}

import org.apache.kyuubi.config.{ConfigEntry, KyuubiConf}
import org.apache.kyuubi.service.authentication.LdapAuthenticationProviderImpl

object LdapAuthenticationTestCase {
  def builder: LdapAuthenticationTestCase.Builder = new LdapAuthenticationTestCase.Builder

  class Builder {
    private val overrides: mutable.Map[ConfigEntry[_], String] = new mutable.HashMap

    var conf: KyuubiConf = _

    def baseDN(baseDN: String): LdapAuthenticationTestCase.Builder =
      setVarOnce(KyuubiConf.AUTHENTICATION_LDAP_BASE_DN, baseDN)

    def guidKey(guidKey: String): LdapAuthenticationTestCase.Builder =
      setVarOnce(KyuubiConf.AUTHENTICATION_LDAP_GUID_KEY, guidKey)

    def userDNPatterns(userDNPatterns: String*): LdapAuthenticationTestCase.Builder =
      setVarOnce(KyuubiConf.AUTHENTICATION_LDAP_USER_DN_PATTERN, userDNPatterns.mkString(":"))

    def userFilters(userFilters: String*): LdapAuthenticationTestCase.Builder =
      setVarOnce(KyuubiConf.AUTHENTICATION_LDAP_USER_FILTER, userFilters.mkString(","))

    def groupDNPatterns(groupDNPatterns: String*): LdapAuthenticationTestCase.Builder =
      setVarOnce(KyuubiConf.AUTHENTICATION_LDAP_GROUP_DN_PATTERN, groupDNPatterns.mkString(":"))

    def groupFilters(groupFilters: String*): LdapAuthenticationTestCase.Builder =
      setVarOnce(KyuubiConf.AUTHENTICATION_LDAP_GROUP_FILTER, groupFilters.mkString(","))

    def groupClassKey(groupClassKey: String): LdapAuthenticationTestCase.Builder =
      setVarOnce(KyuubiConf.AUTHENTICATION_LDAP_GROUP_CLASS_KEY, groupClassKey)

    def ldapUrl(ldapUrl: String): LdapAuthenticationTestCase.Builder =
      setVarOnce(KyuubiConf.AUTHENTICATION_LDAP_URL, ldapUrl)

    def customQuery(customQuery: String): LdapAuthenticationTestCase.Builder =
      setVarOnce(KyuubiConf.AUTHENTICATION_LDAP_CUSTOM_LDAP_QUERY, customQuery)

    def groupMembershipKey(groupMembershipKey: String): LdapAuthenticationTestCase.Builder =
      setVarOnce(KyuubiConf.AUTHENTICATION_LDAP_GROUP_MEMBERSHIP_KEY, groupMembershipKey)

    def userMembershipKey(userMembershipKey: String): LdapAuthenticationTestCase.Builder =
      setVarOnce(KyuubiConf.AUTHENTICATION_LDAP_USER_MEMBERSHIP_KEY, userMembershipKey)

    private def setVarOnce(
        confVar: ConfigEntry[_],
        value: String): LdapAuthenticationTestCase.Builder = {
      require(!overrides.contains(confVar), s"Property $confVar has been set already")
      overrides.put(confVar, value)
      this
    }

    def build: LdapAuthenticationTestCase = {
      require(conf == null, "Test Case Builder should not be reused. Please create a new instance.")
      conf = new KyuubiConf()
      overrides.foreach { case (k, v) => conf.set(k.key, v) }
      new LdapAuthenticationTestCase(this)
    }
  }
}

final class LdapAuthenticationTestCase(builder: LdapAuthenticationTestCase.Builder) {

  private val ldapProvider = new LdapAuthenticationProviderImpl(builder.conf)

  def assertAuthenticatePasses(credentials: Credentials): Unit =
    try {
      ldapProvider.authenticate(credentials.user, credentials.password)
    } catch {
      case e: AuthenticationException =>
        throw new AssertionError(
          s"Authentication failed for user '${credentials.user}' " +
            s"with password '${credentials.password}'",
          e)
    }

  def assertAuthenticateFails(credentials: Credentials): Unit = {
    assertAuthenticateFails(credentials.user, credentials.password)
  }

  def assertAuthenticateFailsUsingWrongPassword(credentials: Credentials): Unit = {
    assertAuthenticateFails(credentials.user, "not" + credentials.password)
  }

  def assertAuthenticateFails(user: String, password: String): Unit = {
    val e = intercept[AuthenticationException] {
      ldapProvider.authenticate(user, password)
      fail(s"Expected authentication to fail for $user")
    }
    assert(e != null)
  }
}
