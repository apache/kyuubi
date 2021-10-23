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

import java.sql.{DriverManager, SQLException}

import org.apache.kyuubi.{KerberizedTestHelper, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.authentication.UserDefineAuthenticationProviderImpl

class KyuubiOperationMultipleAuthTypeSuite extends
  WithKyuubiServer with KerberizedTestHelper with JDBCTestUtils {
  private val customPasswd: String = "password"

  override protected def jdbcUrl: String = getJdbcUrl
  private def kerberosJdbcUrl: String = jdbcUrl + s"principal=${testPrincipal}"

  override protected lazy val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.AUTHENTICATION_METHOD, Seq("KERBEROS", "CUSTOM"))
      .set(KyuubiConf.SERVER_KEYTAB, testKeytab)
      .set(KyuubiConf.SERVER_PRINCIPAL, testPrincipal)
      .set(KyuubiConf.AUTHENTICATION_CUSTOM_CLASS,
        classOf[UserDefineAuthenticationProviderImpl].getCanonicalName)
  }

  test("test with KERBEROS authentication") {
    val conn = DriverManager.getConnection(jdbcUrlWithConf(kerberosJdbcUrl), user, "")
    try {
      val statement = conn.createStatement()
      val resultSet = statement.executeQuery("select engine_name()")
      assert(resultSet.next())
      assert(resultSet.getString(1).nonEmpty)
    } finally {
      conn.close()
    }
  }

  test("test with CUSTOM authentication") {
    val conn = DriverManager.getConnection(jdbcUrlWithConf, user, customPasswd)
    try {
      val statement = conn.createStatement()
      val resultSet = statement.executeQuery("select engine_name()")
      assert(resultSet.next())
      assert(resultSet.getString(1).nonEmpty)
    } finally {
      conn.close()
    }
  }

  test("test with invalid password") {
    intercept[SQLException] {
      val conn = DriverManager.getConnection(jdbcUrlWithConf, user, "invalidPassword")
      try {
        val statement = conn.createStatement()
        statement.executeQuery("select engine_name()")
      } finally {
        conn.close()
      }
    }
  }
}
