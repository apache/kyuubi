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

package org.apache.kyuubi.engine.hive.operation

import org.apache.kyuubi.engine.hive.HiveSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class HiveOperationSuite extends WithHiveSQLEngine with HiveJDBCTestHelper {
  override protected def jdbcUrl: String = {
    "jdbc:hive2://" + HiveSQLEngine.currentEngine.get.frontendServices.head.connectionUrl + "/;"
  }

  test("execute statement - select null") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT NULL AS col")
      assert(resultSet.next())
      assert(resultSet.getString("col") === null)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.NULL)
      assert(metaData.getPrecision(1) === 0)
      assert(metaData.getScale(1) === 0)
    }
  }
}
