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

import java.sql.SQLException

import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.SESSION_CONF_ADVISOR

class OperationLanguageSuite extends WithKyuubiServer with HiveJDBCTestHelper {

  override protected def jdbcUrl: String =
    s"jdbc:kyuubi://${server.frontendServices.head.connectionUrl}/;"

  override protected val URL_PREFIX: String = "jdbc:kyuubi://"

  override protected val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.ENGINE_SHARE_LEVEL, "connection")
      .set(SESSION_CONF_ADVISOR.key, classOf[TestSessionConfAdvisor].getName)
  }

  test("kyuubi #3311: Operation language with an incorrect value") {
    withSessionConf()(Map(KyuubiConf.OPERATION_LANGUAGE.key -> "SQL"))(Map.empty) {
      withJdbcStatement() { statement =>
        statement.executeQuery(s"set ${KyuubiConf.OPERATION_LANGUAGE.key}=AAA")
        val e = intercept[SQLException](statement.executeQuery("select 1"))
        assert(e.getMessage.contains("The operation language UNKNOWN doesn't support"))
        statement.executeQuery(s"set ${KyuubiConf.OPERATION_LANGUAGE.key}=SQL")
        val result = statement.executeQuery("select 1")
        assert(result.next())
        assert(result.getInt(1) === 1)
      }
    }
  }

}
