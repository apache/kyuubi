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

import javax.naming.CommunicationException
import javax.security.sasl.AuthenticationException

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

class LdapAuthenticationProviderImplSuite extends WithLdapServer {
  override protected val ldapUser: String = "kentyao"
  override protected val ldapUserPasswd: String = "kentyao"

  private val conf = new KyuubiConf()

  override def beforeAll(): Unit = {
    super.beforeAll()
    conf.set(AUTHENTICATION_LDAP_URL, ldapUrl)
    conf.set(AUTHENTICATION_LDAP_BASEDN, ldapBaseDn)
    conf.set(AUTHENTICATION_LDAP_GUIDKEY, ldapGuidKey)
    conf.set(AUTHENTICATION_LDAP_BINDDN, ldapBinddn)
    conf.set(AUTHENTICATION_LDAP_PASSWORD, ldapBindpw)
    conf.set(AUTHENTICATION_LDAP_DOMAIN, ldapDomain)
    conf.set(AUTHENTICATION_LDAP_ATTRIBUTES, ldapAttrs)
  }

  override def afterAll(): Unit = {
    ldapServer.close()
    super.afterAll()
  }

  test("ldap server is started") {
    assert(ldapServer.getListenPort > 0)
  }

  test("authenticate tests") {
    val providerImpl = new LdapAuthenticationProviderImpl(conf)
    val e1 = intercept[AuthenticationException](providerImpl.authenticate("", ""))
    assert(e1.getMessage.contains("user is null"))
    val e2 = intercept[AuthenticationException](providerImpl.authenticate("kyuubi", ""))
    assert(e2.getMessage.contains("password is null"))

    providerImpl.authenticate(ldapUser, ldapUserPasswd)

    conf.set(AUTHENTICATION_LDAP_BASEDN, "dc=com")
    val providerImpl1 = new LdapAuthenticationProviderImpl(conf)
    val e3 = intercept[AuthenticationException](
      providerImpl1.authenticate(ldapUser, ldapUserPasswd))
    assert(e3.getMessage contains (ldapUser))
    assert(e3.getMessage startsWith "LDAP InitialLdapContext failed")
    assert(e3.getCause.isInstanceOf[javax.naming.NameNotFoundException])
    conf.set(AUTHENTICATION_LDAP_BASEDN, ldapBaseDn)

    conf.set(
      AUTHENTICATION_LDAP_BINDDN,
      s"uid=admin,cn=Directory Manager,ou=users,dc=example,dc=com")
    conf.set(AUTHENTICATION_LDAP_PASSWORD, "adminPasswordTest")
    val providerImpl2 = new LdapAuthenticationProviderImpl(conf)
    val e4 = intercept[AuthenticationException](
      providerImpl2.authenticate(ldapUser, ldapUserPasswd))
    assert(e4.getMessage contains (ldapUser))
    assert(e4.getMessage startsWith "LDAP InitialLdapContext failed")
    assert(e4.getCause.isInstanceOf[javax.naming.AuthenticationException])
    conf.set(AUTHENTICATION_LDAP_PASSWORD, ldapBindpw)

    val providerImpl3 = new LdapAuthenticationProviderImpl(conf)
    val e5 = intercept[AuthenticationException](
      providerImpl3.authenticate(ldapUser, "kent"))
    assert(e5.getMessage contains (ldapUser))
    assert(e5.getMessage startsWith "LDAP InitialDirContext failed")
    assert(e5.getCause.isInstanceOf[javax.naming.AuthenticationException])

    val providerImpl4 = new LdapAuthenticationProviderImpl(conf)
    val e6 = intercept[AuthenticationException](
      providerImpl4.authenticate("kent", ldapUserPasswd))
    assert(e6.getMessage contains ("kent"))
    assert(e6.getMessage startsWith "LDAP InitialLdapContext search results are empty")

    conf.set(AUTHENTICATION_LDAP_DOMAIN, "kyuubi.com")
    val providerImpl5 = new LdapAuthenticationProviderImpl(conf)
    val e7 =
      intercept[AuthenticationException](providerImpl5.authenticate(ldapUser, ldapUserPasswd))
    assert(e7.getMessage startsWith "LDAP InitialLdapContext search results are empty")
    conf.set(AUTHENTICATION_LDAP_DOMAIN, "example")

    conf.set(AUTHENTICATION_LDAP_ATTRIBUTES, Seq("cn"))
    val providerImpl6 = new LdapAuthenticationProviderImpl(conf)
    providerImpl6.authenticate(ldapUser, ldapUserPasswd)
    conf.set(AUTHENTICATION_LDAP_ATTRIBUTES, Seq("mail"))

    conf.set(AUTHENTICATION_LDAP_ATTRIBUTES, Seq("mobile"))
    val providerImpl7 = new LdapAuthenticationProviderImpl(conf)
    val e8 =
      intercept[AuthenticationException](providerImpl7.authenticate(ldapUser, ldapUserPasswd))
    assert(e8.getMessage startsWith "LDAP attributes are empty")
    conf.set(AUTHENTICATION_LDAP_ATTRIBUTES, Seq("mail"))

    conf.unset(AUTHENTICATION_LDAP_URL)
    val providerImpl8 = new LdapAuthenticationProviderImpl(conf)
    val e9 = intercept[AuthenticationException](
      providerImpl8.authenticate(ldapUser, ldapUserPasswd))
    assert(e9.getMessage.contains(ldapUser))
    assert(e9.getCause.isInstanceOf[CommunicationException])
  }
}
