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

import org.mockito.ArgumentMatchers.{eq => mockEq}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

class CustomQueryFilterSuite extends KyuubiFunSuite {
  private val USER2_DN: String = "uid=user2,ou=People,dc=example,dc=com"
  private val USER1_DN: String = "uid=user1,ou=People,dc=example,dc=com"
  private val CUSTOM_QUERY: String = "(&(objectClass=person)(|(uid=user1)(uid=user2)))"

  private val factory: FilterFactory = CustomQueryFilterFactory
  private var conf: KyuubiConf = _
  private var search: DirSearch = _

  override def beforeEach(): Unit = {
    conf = new KyuubiConf()
    search = mock[DirSearch]
    super.beforeEach()
  }

  test("Factory") {
    conf.unset(KyuubiConf.AUTHENTICATION_LDAP_CUSTOM_LDAP_QUERY)
    assert(factory.getInstance(conf).isEmpty)
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_CUSTOM_LDAP_QUERY, CUSTOM_QUERY)
    assert(factory.getInstance(conf).isDefined)
  }

  test("ApplyPositive") {
    conf.set(KyuubiConf.AUTHENTICATION_LDAP_CUSTOM_LDAP_QUERY, CUSTOM_QUERY)
    when(search.executeCustomQuery(mockEq(CUSTOM_QUERY)))
      .thenReturn(Array(USER1_DN, USER2_DN))
    val filter: Filter = factory.getInstance(conf).get
    filter.apply(search, "user1")
    filter.apply(search, "user2")
  }

  test("ApplyNegative") {
    intercept[AuthenticationException] {
      conf.set(KyuubiConf.AUTHENTICATION_LDAP_CUSTOM_LDAP_QUERY, CUSTOM_QUERY)
      when(search.executeCustomQuery(mockEq(CUSTOM_QUERY)))
        .thenReturn(Array(USER1_DN, USER2_DN))
      val filter: Filter = factory.getInstance(conf).get
      filter.apply(search, "user3")
    }
  }
}
