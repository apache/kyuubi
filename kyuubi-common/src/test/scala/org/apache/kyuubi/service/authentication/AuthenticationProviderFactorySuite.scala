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

import javax.security.sasl.AuthenticationException

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{AUTHENTICATION_LDAP_ATTRIBUTES, AUTHENTICATION_LDAP_BASEDN, AUTHENTICATION_LDAP_BINDDN, AUTHENTICATION_LDAP_DOMAIN, AUTHENTICATION_LDAP_GUIDKEY, AUTHENTICATION_LDAP_PASSWORD}

class AuthenticationProviderFactorySuite extends KyuubiFunSuite {

  import AuthenticationProviderFactory._

  private val conf = new KyuubiConf()

  override def beforeAll(): Unit = {
    super.beforeAll()
    conf.set(AUTHENTICATION_LDAP_BASEDN, "test")
    conf.set(AUTHENTICATION_LDAP_GUIDKEY, "test")
    conf.set(AUTHENTICATION_LDAP_BINDDN, "test")
    conf.set(AUTHENTICATION_LDAP_PASSWORD, "test")
    conf.set(AUTHENTICATION_LDAP_DOMAIN, "test")
    conf.set(AUTHENTICATION_LDAP_ATTRIBUTES, Seq("test"))
  }

  test("get auth provider") {
    val p1 = getAuthenticationProvider(AuthMethods.withName("NONE"), conf)
    p1.authenticate(Utils.currentUser, "")
    val p2 = getAuthenticationProvider(AuthMethods.withName("LDAP"), conf)
    val e1 = intercept[AuthenticationException](p2.authenticate("test", "test"))
    assert(e1.getMessage.contains("LDAP InitialLdapContext failed"))
    val e2 = intercept[AuthenticationException](
      AuthenticationProviderFactory.getAuthenticationProvider(null, conf))
    assert(e2.getMessage === "Not a valid authentication method")
  }

}
