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

class RestartEngineSuite extends ExecutedCommandExecSuite {

  test("Restart kyuubi engine") {
    withJdbcStatement() { statement =>
      var resultSet = statement.executeQuery("KYUUBI DESC ENGINE")
      assert(resultSet.next())
      assert(resultSet.getMetaData.getColumnCount == 3)
      assert(resultSet.getMetaData.getColumnName(1) == "ENGINE_ID")
      assert(resultSet.getMetaData.getColumnName(2) == "ENGINE_NAME")
      assert(resultSet.getMetaData.getColumnName(3) == "ENGINE_URL")
      val engineDied = resultSet.getString("ENGINE_ID")

      resultSet = statement.executeQuery("KYUUBI RESTART ENGINE")
      assert(resultSet.next())
      assert(resultSet.getMetaData.getColumnCount == 3)
      assert(resultSet.getMetaData.getColumnName(1) == "ENGINE_ID")
      assert(resultSet.getMetaData.getColumnName(2) == "ENGINE_NAME")
      assert(resultSet.getMetaData.getColumnName(3) == "ENGINE_URL")

      val engineAlive = resultSet.getString("ENGINE_ID")

      resultSet = statement.executeQuery("KYUUBI DESC ENGINE")
      assert(resultSet.next())
      assert(resultSet.getMetaData.getColumnCount == 3)
      assert(resultSet.getMetaData.getColumnName(1) == "ENGINE_ID")
      assert(resultSet.getMetaData.getColumnName(2) == "ENGINE_NAME")
      assert(resultSet.getMetaData.getColumnName(3) == "ENGINE_URL")

      val engineDesc = resultSet.getString("ENGINE_ID")

      assert(engineDied != engineAlive)
      assert(engineDesc == engineAlive)
    }
  }

  test("Restart/Select kyuubi engine") {
    withJdbcStatement() { statement =>
      var resultSet = statement.executeQuery("KYUUBI RESTART ENGINE")
      assert(resultSet.next())
      assert(resultSet.getMetaData.getColumnCount == 3)
      assert(resultSet.getMetaData.getColumnName(1) == "ENGINE_ID")
      assert(resultSet.getMetaData.getColumnName(2) == "ENGINE_NAME")
      assert(resultSet.getMetaData.getColumnName(3) == "ENGINE_URL")

      resultSet = statement.executeQuery("SELECT 1")
      assert(resultSet.next())
      assert(resultSet.getInt(1) == 1)
    }
  }
}
