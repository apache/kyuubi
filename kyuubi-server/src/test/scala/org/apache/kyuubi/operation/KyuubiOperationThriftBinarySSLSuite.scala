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

import org.apache.kyuubi.{Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf

class KyuubiOperationThriftBinarySSLSuite extends WithKyuubiServer with HiveJDBCTestHelper {
  val keyStoreLocation = getClass.getResource("/ssl/keystore.jks").getPath
  val trustStoreLocation = getClass.getResource("/ssl/truststore.jks").getPath
  private val KEY_STORE_PASSWORD = "KyuubiJDBC"
  private val TRUST_STORE_PASSWORD = "KyuubiJDBC"

  override protected def jdbcUrl: String = getJdbcUrl.stripSuffix(";") +
    s";ssl=true;sslTrustStore=$trustStoreLocation;trustStorePassword=$TRUST_STORE_PASSWORD"

  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(KyuubiConf.FRONTEND_THRIFT_BINARY_SSL_ENABLED, true)
      .set(KyuubiConf.FRONTEND_SSL_KEYSTORE_PATH, keyStoreLocation)
      .set(KyuubiConf.FRONTEND_SSL_KEYSTORE_PASSWORD, KEY_STORE_PASSWORD)
  }

  test("ssl connection") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT system_user(), session_user()")
      assert(rs.next())
      assert(rs.getString(1) === Utils.currentUser)
      assert(rs.getString(2) === Utils.currentUser)
    }
  }
}
