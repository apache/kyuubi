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

package org.apache.kyuubi.operation.parser

class DescribeEngineSuite extends ExecutedCommandExecSuite {

  test("desc/describe kyuubi engine") {
    Seq("DESC", "DESCRIBE").foreach { desc =>
      withJdbcStatement() { statement =>
        val resultSet = statement.executeQuery(s"KYUUBI $desc ENGINE")
        assert(resultSet.next())

        assert(resultSet.getMetaData.getColumnCount == 6)
        assert(resultSet.getMetaData.getColumnName(1) == "ENGINE_ID")
        assert(resultSet.getMetaData.getColumnName(2) == "ENGINE_NAME")
        assert(resultSet.getMetaData.getColumnName(3) == "ENGINE_URL")
        assert(resultSet.getMetaData.getColumnName(4) == "ENGINE_INSTANCE")
        assert(resultSet.getMetaData.getColumnName(5) == "ENGINE_VERSION")
        assert(resultSet.getMetaData.getColumnName(6) == "ENGINE_ATTRIBUTES")
      }
    }
  }
}
