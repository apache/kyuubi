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

    val user = "uid=kentyao,ou=users"
    providerImpl.authenticate(user, "kentyao")
    val e3 = intercept[AuthenticationException](
      providerImpl.authenticate(user, "kent"))
    assert(e3.getMessage.contains(user))
    assert(e3.getCause.isInstanceOf[javax.naming.AuthenticationException])

    val dn = "ou=users"
    conf.set(AUTHENTICATION_LDAP_BASEDN, dn)
    val providerImpl2 = new LdapAuthenticationProviderImpl(conf)
    providerImpl2.authenticate("kentyao", "kentyao")

    val e4 = intercept[AuthenticationException](
      providerImpl.authenticate("kentyao", "kent"))
    assert(e4.getMessage.contains(user))

    conf.unset(AUTHENTICATION_LDAP_URL)
    val providerImpl3 = new LdapAuthenticationProviderImpl(conf)
    val e5 = intercept[AuthenticationException](
      providerImpl3.authenticate("kentyao", "kentyao"))

    assert(e5.getMessage.contains(user))
    assert(e5.getCause.isInstanceOf[CommunicationException])

    conf.set(AUTHENTICATION_LDAP_DOMAIN, "kyuubi.com")
    val providerImpl4 = new LdapAuthenticationProviderImpl(conf)
    intercept[AuthenticationException](providerImpl4.authenticate("kentyao", "kentyao"))
  }
}
