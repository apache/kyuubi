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

import java.sql.{SQLException, SQLTimeoutException}

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{OPERATION_QUERY_TIMEOUT, OPERATION_QUERY_TIMEOUT_COMPATIBLE_STATE}

class KyuubiOperationManagerTimeoutCompatibleSuite extends WithKyuubiServer
  with HiveJDBCTestHelper {
  override protected val conf: KyuubiConf = {
    KyuubiConf().set(OPERATION_QUERY_TIMEOUT.key, "PT1S").set(
      OPERATION_QUERY_TIMEOUT_COMPATIBLE_STATE.key,
      "true")
  }

  test("clientTimeout is not set, timeout state switches to cancel state") {
    withJdbcStatement() { statement =>
      Seq(-1, 0, 5, 10, 20).foreach { clientTimeout =>
        statement.setQueryTimeout(clientTimeout)

        def query(): Unit = {
          statement.executeQuery("select java_method('java.lang.Thread', 'sleep', 10000L)")
        }

        if (clientTimeout > 0) {
          val e = intercept[SQLTimeoutException] {
            query()
          }.getMessage
          assert(e.contains("Query timed out after"))
        } else {
          val e = intercept[SQLException] {
            query()
          }.getMessage
          assert(e.contains("Query was cancelled"))
        }
      }
    }
  }

  override protected def jdbcUrl: String = getJdbcUrl
}
