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

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.engine.hive.HiveSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant.{TABLE_CATALOG, TABLE_SCHEM}

class HiveOperationSuite extends HiveJDBCTestHelper {

  override def beforeAll(): Unit = {
    HiveSQLEngine.startEngine()
    super.beforeAll()
  }

  override protected def jdbcUrl: String = {
    "jdbc:hive2://" + HiveSQLEngine.currentEngine.get.frontendServices.head.connectionUrl + "/;"
  }

  test("get catalogs") {
    withJdbcStatement() { statement =>
      val catalogs = statement.getConnection.getMetaData.getCatalogs
      assert(!catalogs.next())
    }
  }

  test("get schemas") {
    withDatabases("test_schema") { statement =>
      statement.execute("CREATE SCHEMA IF NOT EXISTS test_schema")
      val metaData = statement.getConnection.getMetaData
      var resultSet = metaData.getSchemas(null, null)
      val resultSetBuffer = ArrayBuffer[(String, String)]()
      while (resultSet.next()) {
        resultSetBuffer += Tuple2(
          resultSet.getString(TABLE_CATALOG),
          resultSet.getString(TABLE_SCHEM))
      }
      assert(resultSetBuffer.contains(("", "default")))
      assert(resultSetBuffer.contains(("", "test_schema")))

      resultSet = metaData.getSchemas("", "test")
      while (resultSet.next()) {
        assert(resultSet.getString(TABLE_CATALOG) == "")
        assert(resultSet.getString(TABLE_SCHEM) == "test_schema")
      }
    }
  }

  test("basic execute statements, create, insert query") {
    withJdbcStatement("hive_engine_test") { statement =>
      statement.execute("CREATE TABLE hive_engine_test(id int, value string) stored as orc")
      statement.execute("INSERT INTO hive_engine_test SELECT 1, '2'")

      val resultSet = statement.executeQuery("SELECT ID, VALUE FROM hive_engine_test")
      assert(resultSet.next())
      assert(resultSet.getInt("ID") === 1)
      assert(resultSet.getString("VALUE") === "2")

      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.INTEGER)
      assert(metaData.getPrecision(1) === 10)
      assert(metaData.getScale(1) === 0)
    }
  }
}
