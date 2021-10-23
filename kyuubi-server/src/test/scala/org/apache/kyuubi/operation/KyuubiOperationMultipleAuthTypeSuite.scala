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

package org.apache.kyuubi.operation

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.{UserDefineAuthenticationProviderImpl, WithLdapServer}

class KyuubiOperationMultipleAuthTypeSuite extends
  WithKyuubiServer with WithLdapServer with JDBCTestUtils {
  override protected val ldapPasswd: String = "ldapPassword"
  private val customPasswd: String = "password"

  override protected def jdbcUrl: String = getJdbcUrl

  override protected lazy val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.AUTHENTICATION_METHOD, Seq("LDAP", "CUSTOM"))
      .set(KyuubiConf.AUTHENTICATION_LDAP_URL, ldapUrl)
      .set(KyuubiConf.AUTHENTICATION_LDAP_BASEDN, ldapBaseDn)
      .set(KyuubiConf.AUTHENTICATION_CUSTOM_CLASS,
        classOf[UserDefineAuthenticationProviderImpl].getCanonicalName)
  }

  test("test with LDAP authentication") {
    withMultipleConnectionJdbcStatementWithPasswd(ldapPasswd)() { statement =>
      val resultSet = statement.executeQuery("select engine_name()")
      assert(resultSet.next())
      assert(resultSet.getString(1).nonEmpty)
    }
  }

  test("test with CUSTOM authentication") {
    withMultipleConnectionJdbcStatementWithPasswd(customPasswd)() { statement =>
      val resultSet = statement.executeQuery("select engine_name()")
      assert(resultSet.next())
      assert(resultSet.getString(1).nonEmpty)
    }
  }
}
