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
package org.apache.kyuubi.engine.jdbc.oracle

import org.apache.kyuubi.operation.HiveJDBCTestHelper

class OracleSessionSuite extends WithOracleEngine with HiveJDBCTestHelper {
  test("oracle session suite") {
    withJdbcStatement() { statement =>
      {
        val resultSet = statement.executeQuery(
          "SELECT '1' AS ID FROM DUAL")
        val metadata = resultSet.getMetaData
        for (i <- 1 to metadata.getColumnCount) {
          assert(metadata.getColumnName(i) == "ID")
        }
        while (resultSet.next()) {
          val id = resultSet.getObject(1)
          assert(id == "1")
        }
      }
    }
  }

  override protected def jdbcUrl: String = jdbcConnectionUrl
}
