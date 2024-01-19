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

package org.apache.kyuubi

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.{JavaVersion, SystemUtils}

import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.util.JdbcUtils

/**
 * hive tests disabled for JAVA 11
 * https://issues.apache.org/jira/browse/HIVE-22415
 * https://issues.apache.org/jira/browse/HIVE-21584
 * hive 3.x not works with java 11
 */
trait HiveEngineTests extends HiveJDBCTestHelper {

  test("get catalogs") {
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))
    withJdbcStatement() { statement =>
      val catalogs = statement.getConnection.getMetaData.getCatalogs
      assert(!catalogs.next())
    }
  }

  test("get schemas") {
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))
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
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))
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
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))
    withDatabases("test_schema") { statement =>
      statement.execute("CREATE SCHEMA IF NOT EXISTS test_schema")
      statement.execute("CREATE TABLE IF NOT EXISTS test_schema.test_table(a int, b string)")

      try {
        val meta = statement.getConnection.getMetaData
        var resultSet = meta.getColumns(null, null, null, null)
        val resultSetBuffer = ArrayBuffer[(String, String, String, String, String)]()
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
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))
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
            assert(resultSet.getString(SPECIFIC_NAME) ===
              "org.apache.hadoop.hive.ql.exec.FunctionInfo")
          }
      }
    }
  }

  test("get table types") {
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))
    withJdbcStatement() { statement =>
      val resultSet = statement.getConnection.getMetaData.getTableTypes
      val hive2_1Expected = Set("TABLE", "VIEW", "INDEX_TABLE")
      val hive2_3Expected = Set("TABLE", "VIEW", "MATERIALIZED_VIEW", "INDEX_TABLE")
      val hive3Expected = Set("TABLE", "VIEW", "MATERIALIZED_VIEW")
      val tableTypes = JdbcUtils.mapResultSet(resultSet) { rs => rs.getString(TABLE_TYPE) }.toSet
      assert(tableTypes === hive2_1Expected || tableTypes === hive2_3Expected ||
        tableTypes === hive3Expected)
    }
  }

  test("get primary keys") {
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))
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
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))
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
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))
    withJdbcStatement("hive_engine_test") { statement =>
      statement.execute("CREATE TABLE hive_engine_test(id int, value string) stored as textfile")
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

  test("test getTypeInfo") {
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))
    withJdbcStatement() { statement =>
      val typeInfo = statement.getConnection.getMetaData.getTypeInfo
      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "VOID")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.NULL)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "BOOLEAN")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.BOOLEAN)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "TINYINT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.TINYINT)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "SMALLINT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.SMALLINT)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "INT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.INTEGER)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "BIGINT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.BIGINT)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "FLOAT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.FLOAT)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "DOUBLE")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.DOUBLE)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "STRING")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.VARCHAR)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "CHAR")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.CHAR)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "VARCHAR")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.VARCHAR)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "DATE")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.DATE)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "TIMESTAMP")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.TIMESTAMP)

      typeInfo.next()
      // Hive3 supports TIMESTAMP WITH LOCAL TIME ZONE
      if (typeInfo.getString(TYPE_NAME) == "TIMESTAMP WITH LOCAL TIME ZONE") {
        assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.OTHER)
        typeInfo.next()
      }

      assert(typeInfo.getString(TYPE_NAME) === "INTERVAL_YEAR_MONTH")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.OTHER)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "INTERVAL_DAY_TIME")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.OTHER)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "BINARY")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.BINARY)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "DECIMAL")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.DECIMAL)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "ARRAY")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.ARRAY)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "MAP")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.JAVA_OBJECT)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "STRUCT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.STRUCT)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "UNIONTYPE")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.OTHER)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "USER_DEFINED")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.OTHER)
    }
  }

  test("test setClientInfo") {
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))
    withJdbcStatement() { statement =>
      val res = statement.getConnection.getMetaData.getClientInfoProperties
      assert(res.next())
      assert(res.getString(1) === "ApplicationName")
      assert(res.getInt("MAX_LEN") === 1000)
      assert(!res.next())

      val connection = statement.getConnection
      connection.setClientInfo("ApplicationName", "test kyuubi hive jdbc")
      assert(connection.getClientInfo("ApplicationName") == "test kyuubi hive jdbc")
    }
  }

  test("test set command") {
    assume(SystemUtils.isJavaVersionAtMost(JavaVersion.JAVA_1_8))
    withJdbcStatement() { statement =>
      statement.execute("set hive.query.name=test")
      val resultSet = statement.executeQuery("set hive.query.name")
      assert(resultSet.next())
      assert(resultSet.getString(1) === "hive.query.name=test")
    }
  }
}
