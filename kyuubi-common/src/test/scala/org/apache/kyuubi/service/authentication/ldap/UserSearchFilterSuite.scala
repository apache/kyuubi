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

import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

class UserSearchFilterSuite extends KyuubiFunSuite {
  private val factory: FilterFactory = UserSearchFilterFactory
  private var conf: KyuubiConf = _
  private var search: DirSearch = _

  override def beforeEach(): Unit = {
    conf = new KyuubiConf()
    search = mock[DirSearch]
    super.beforeEach()
  }

  test("FactoryWhenNoGroupOrUserFilters") {
    assert(factory.getInstance(conf).isEmpty)
  }

  test("FactoryWhenGroupFilter") {
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_GROUP_FILTER.key, "Grp1,Grp2")
    assert(factory.getInstance(conf).isDefined)
  }

  test("FactoryWhenUserFilter") {
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_USER_FILTER.key, "User1,User2")
    assert(factory.getInstance(conf).isDefined)
  }

  test("ApplyPositive") {
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_USER_FILTER.key, "User1")
    val filter = factory.getInstance(conf).get
    when(search.findUserDn(anyString)).thenReturn("cn=User1,ou=People,dc=example,dc=com")
    filter.apply(search, "User1")
  }

  test("ApplyWhenNamingException") {
    intercept[AuthenticationException] {
      conf.set(KyuubiConf.AUTHENTICATION_LDAP_USER_FILTER.key, "User1")
      val filter = factory.getInstance(conf).get
      when(search.findUserDn(anyString)).thenThrow(classOf[NamingException])
      filter.apply(search, "User3")
    }
  }

  test("ApplyWhenNotFound") {
    intercept[AuthenticationException] {
      conf.set(KyuubiConf.AUTHENTICATION_LDAP_USER_FILTER.key, "User1")
      val filter = factory.getInstance(conf).get
      when(search.findUserDn(anyString)).thenReturn(null)
      filter.apply(search, "User3")
    }
  }
}
