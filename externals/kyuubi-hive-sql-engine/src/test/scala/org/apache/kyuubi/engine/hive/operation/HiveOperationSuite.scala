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

import org.apache.hadoop.hive.ql.exec.FunctionInfo

import org.apache.kyuubi.engine.hive.HiveSQLEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._

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

  test("get tables") {
    withDatabases("test_schema") { statement =>
      statement.execute("CREATE SCHEMA IF NOT EXISTS test_schema")
      statement.execute("CREATE TABLE IF NOT EXISTS test_schema.test_table(a string)")
      statement.execute(
        "CREATE OR REPLACE VIEW test_schema.test_view AS SELECT  * FROM test_schema.test_table")

      try {
        val meta = statement.getConnection.getMetaData
        var resultSet = meta.getTables(null, null, null, null)
        val resultSetBuffer = ArrayBuffer[(String, String, String, String)]()
        while (resultSet.next()) {
          resultSetBuffer += Tuple4(
            resultSet.getString(TABLE_CAT),
            resultSet.getString(TABLE_SCHEM),
            resultSet.getString(TABLE_NAME),
            resultSet.getString(TABLE_TYPE))
        }
        assert(resultSetBuffer.contains(("", "test_schema", "test_table", "TABLE")))
        assert(resultSetBuffer.contains(("", "test_schema", "test_view", "VIEW")))

        resultSet = meta.getTables("", null, null, null)
        resultSetBuffer.clear()
        while (resultSet.next()) {
          resultSetBuffer += Tuple4(
            resultSet.getString(TABLE_CAT),
            resultSet.getString(TABLE_SCHEM),
            resultSet.getString(TABLE_NAME),
            resultSet.getString(TABLE_TYPE))
        }
        assert(resultSetBuffer.contains(("", "test_schema", "test_table", "TABLE")))
        assert(resultSetBuffer.contains(("", "test_schema", "test_view", "VIEW")))

        resultSet = meta.getTables(null, "test_schema", null, null)
        resultSetBuffer.clear()
        while (resultSet.next()) {
          resultSetBuffer += Tuple4(
            resultSet.getString(TABLE_CAT),
            resultSet.getString(TABLE_SCHEM),
            resultSet.getString(TABLE_NAME),
            resultSet.getString(TABLE_TYPE))
        }
        assert(resultSetBuffer.contains(("", "test_schema", "test_table", "TABLE")))
        assert(resultSetBuffer.contains(("", "test_schema", "test_view", "VIEW")))

        resultSet = meta.getTables(null, null, "test_table", null)
        while (resultSet.next()) {
          assert(resultSet.getString(TABLE_CAT) == "")
          assert(resultSet.getString(TABLE_SCHEM) == "test_schema")
          assert(resultSet.getString(TABLE_NAME) == "test_table")
          assert(resultSet.getString(TABLE_TYPE) == "TABLE")
        }

        resultSet = meta.getTables(null, null, null, Array("VIEW"))
        while (resultSet.next()) {
          assert(resultSet.getString(TABLE_CAT) == "")
          assert(resultSet.getString(TABLE_SCHEM) == "test_schema")
          assert(resultSet.getString(TABLE_NAME) == "test_view")
          assert(resultSet.getString(TABLE_TYPE) == "VIEW")
        }
      } finally {
        statement.execute("DROP VIEW test_schema.test_view")
        statement.execute("DROP TABLE test_schema.test_table")
      }
    }
  }

  test("get columns") {
    withDatabases("test_schema") { statement =>
      statement.execute("CREATE SCHEMA IF NOT EXISTS test_schema")
      statement.execute("CREATE TABLE IF NOT EXISTS test_schema.test_table(a int, b string)")

      try {
        val meta = statement.getConnection.getMetaData
        var resultSet = meta.getColumns(null, null, null, null)
        var resultSetBuffer = ArrayBuffer[(String, String, String, String, String)]()
        while (resultSet.next()) {
          resultSetBuffer += Tuple5(
            resultSet.getString(TABLE_CAT),
            resultSet.getString(TABLE_SCHEM),
            resultSet.getString(TABLE_NAME),
            resultSet.getString(COLUMN_NAME),
            resultSet.getString(TYPE_NAME))
        }
        assert(resultSetBuffer.contains((null, "test_schema", "test_table", "a", "INT")))
        assert(resultSetBuffer.contains((null, "test_schema", "test_table", "b", "STRING")))

        resultSet = meta.getColumns("", null, null, null)
        resultSetBuffer.clear()
        while (resultSet.next()) {
          resultSetBuffer += Tuple5(
            resultSet.getString(TABLE_CAT),
            resultSet.getString(TABLE_SCHEM),
            resultSet.getString(TABLE_NAME),
            resultSet.getString(COLUMN_NAME),
            resultSet.getString(TYPE_NAME))
        }
        assert(resultSetBuffer.contains((null, "test_schema", "test_table", "a", "INT")))
        assert(resultSetBuffer.contains((null, "test_schema", "test_table", "b", "STRING")))

        resultSet = meta.getColumns(null, "test_schema", null, null)
        resultSetBuffer.clear()
        while (resultSet.next()) {
          resultSetBuffer += Tuple5(
            resultSet.getString(TABLE_CAT),
            resultSet.getString(TABLE_SCHEM),
            resultSet.getString(TABLE_NAME),
            resultSet.getString(COLUMN_NAME),
            resultSet.getString(TYPE_NAME))
        }
        assert(resultSetBuffer.contains((null, "test_schema", "test_table", "a", "INT")))
        assert(resultSetBuffer.contains((null, "test_schema", "test_table", "b", "STRING")))

        resultSet = meta.getColumns(null, null, "test_table", null)
        resultSetBuffer.clear()
        while (resultSet.next()) {
          resultSetBuffer += Tuple5(
            resultSet.getString(TABLE_CAT),
            resultSet.getString(TABLE_SCHEM),
            resultSet.getString(TABLE_NAME),
            resultSet.getString(COLUMN_NAME),
            resultSet.getString(TYPE_NAME))
        }
        assert(resultSetBuffer.contains((null, "test_schema", "test_table", "a", "INT")))
        assert(resultSetBuffer.contains((null, "test_schema", "test_table", "b", "STRING")))

        resultSet = meta.getColumns(null, null, null, "a")
        while (resultSet.next()) {
          assert(resultSet.getString(TABLE_CAT) == null)
          assert(resultSet.getString(TABLE_SCHEM) == "test_schema")
          assert(resultSet.getString(TABLE_NAME) == "test_table")
          assert(resultSet.getString(COLUMN_NAME) == "a")
          assert(resultSet.getString(TYPE_NAME) == "INT")
        }
      } finally {
        statement.execute("DROP VIEW test_schema.test_view")
        statement.execute("DROP TABLE test_schema.test_table")
      }
    }
  }

  test("get functions") {
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      Seq("from_unixtime", "to_date", "date_format", "date_format", "round", "sin").foreach {
        func =>
          val resultSet = metaData.getFunctions(null, null, func)
          while (resultSet.next()) {
            assert(resultSet.getString(FUNCTION_CAT) === null)
            assert(resultSet.getString(FUNCTION_SCHEM) === null)
            assert(resultSet.getString(FUNCTION_NAME) === func)
            assert(resultSet.getString(REMARKS).isEmpty)
            assert(resultSet.getString(SPECIFIC_NAME) === classOf[FunctionInfo].getName)
          }
      }
    }
  }

  test("get table types") {
    withJdbcStatement() { statement =>
      val resultSet = statement.getConnection.getMetaData.getTableTypes
      val expected = Set("TABLE", "VIEW", "INDEX_TABLE", "MATERIALIZED_VIEW")
      var tableTypes = Set[String]()
      while (resultSet.next()) {
        assert(expected.contains(resultSet.getString(TABLE_TYPE)))
        tableTypes += resultSet.getString(TABLE_TYPE)
      }
      assert(!resultSet.next())
      assert(expected.size === tableTypes.size)
    }
  }

  test("get primary keys") {
    withDatabases("test_schema") { statement =>
      statement.execute("CREATE SCHEMA IF NOT EXISTS test_schema")
      statement.execute("CREATE TABLE IF NOT EXISTS test_schema.test_table(a string, " +
        "PRIMARY KEY(a) DISABLE NOVALIDATE)")

      try {
        val meta = statement.getConnection.getMetaData
        val resultSet = meta.getPrimaryKeys(null, "test_schema", "test_table")
        val resultSetBuffer = ArrayBuffer[(String, String, String, String)]()
        while (resultSet.next()) {
          resultSetBuffer += Tuple4(
            resultSet.getString(TABLE_CAT),
            resultSet.getString(TABLE_SCHEM),
            resultSet.getString(TABLE_NAME),
            resultSet.getString(COLUMN_NAME))
        }
        assert(resultSetBuffer.contains((null, "test_schema", "test_table", "a")))
      } finally {
        statement.execute("DROP TABLE test_schema.test_table")
      }
    }
  }

  test("get cross reference") {
    withDatabases("test_schema") { statement =>
      statement.execute("CREATE SCHEMA IF NOT EXISTS test_schema")
      statement.execute("CREATE TABLE IF NOT EXISTS test_schema.test_table1(a string, " +
        "PRIMARY KEY(a) DISABLE NOVALIDATE)")
      statement.execute("CREATE TABLE IF NOT EXISTS test_schema.test_table2(a string, b string, " +
        "FOREIGN KEY(b) REFERENCES test_schema.test_table1(a) DISABLE NOVALIDATE)")

      try {
        val meta = statement.getConnection.getMetaData
        val resultSet = meta.getCrossReference(
          null,
          "test_schema",
          "test_table1",
          null,
          "test_schema",
          "test_table2")
        val resultSetBuffer =
          ArrayBuffer[(String, String, String, String, String, String, String, String)]()
        while (resultSet.next()) {
          resultSetBuffer += Tuple8(
            resultSet.getString("PKTABLE_CAT"),
            resultSet.getString("PKTABLE_SCHEM"),
            resultSet.getString("PKTABLE_NAME"),
            resultSet.getString("PKCOLUMN_NAME"),
            resultSet.getString("FKTABLE_CAT"),
            resultSet.getString("FKTABLE_SCHEM"),
            resultSet.getString("FKTABLE_NAME"),
            resultSet.getString("FKCOLUMN_NAME"))
        }
        assert(resultSetBuffer.contains((
          null,
          "test_schema",
          "test_table1",
          "a",
          null,
          "test_schema",
          "test_table2",
          "b")))
      } finally {
        statement.execute("DROP TABLE test_schema.test_table2")
        statement.execute("DROP TABLE test_schema.test_table1")
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

  test("test clientInfo") {
    withJdbcStatement() { statement =>
      val res = statement.getConnection.getMetaData.getClientInfoProperties
      assert(res.next())
      assert(res.getString(1) === "ApplicationName")
      assert(res.getInt("MAX_LEN") === 1000);
      assert(!res.next());

      val connection = statement.getConnection
      connection.setClientInfo("ApplicationName", "test kyuubi hive jdbc")
      assert(connection.getClientInfo("ApplicationName") == "test kyuubi hive jdbc")
    }
  }
}
