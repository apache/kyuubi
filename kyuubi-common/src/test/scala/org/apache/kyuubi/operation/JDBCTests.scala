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

import java.sql.{Date, DriverManager, ResultSet, SQLException, Statement, Timestamp}
import java.util.Locale

import org.apache.hive.service.cli.HiveSQLException

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._

trait JDBCTests extends KyuubiFunSuite {

  protected val dftSchema = "default"
  protected val user: String = Utils.currentUser
  protected def jdbcUrl: String

  protected def withMultipleConnectionJdbcStatement(
      tableNames: String*)(fs: (Statement => Unit)*): Unit = {
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUrl, user, "") }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      tableNames.foreach { name =>
        if (name.toUpperCase(Locale.ROOT).startsWith("VIEW")) {
          statements.head.execute(s"DROP VIEW IF EXISTS $name")
        } else {
          statements.head.execute(s"DROP TABLE IF EXISTS $name")
        }
      }
      info("Closing statements")
      statements.foreach(_.close())
      info("Closed statements")
      connections.foreach(_.close())
      info("Closing connections")
    }
  }

  protected def withDatabases(dbNames: String*)(fs: (Statement => Unit)*): Unit = {
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUrl, user, "") }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      dbNames.foreach { name =>
        statements.head.execute(s"DROP DATABASE IF EXISTS $name")
      }
      info("Closing statements")
      statements.foreach(_.close())
      info("Closed statements")
      connections.foreach(_.close())
      info("Closing connections")
    }
  }

  protected def withJdbcStatement(tableNames: String*)(f: Statement => Unit): Unit = {
    withMultipleConnectionJdbcStatement(tableNames: _*)(f)
  }


  test("get catalogs") {
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      val catalogs = metaData.getCatalogs
      catalogs.next()
      assert(catalogs.getString(TABLE_CAT) === "spark_catalog")
      assert(!catalogs.next())
    }
  }

  test("get schemas") {
    def checkResult(rs: ResultSet, dbNames: Seq[String]): Unit = {
      val expected = dbNames.iterator
      while(rs.next() || expected.hasNext) {
        assert(rs.getString("TABLE_SCHEM") === expected.next)
        assert(rs.getString("TABLE_CATALOG").isEmpty)
      }
      // Make sure there are no more elements
      assert(!rs.next())
      assert(!expected.hasNext, "All expected schemas should be visited")
    }

    val dbs = Seq("db1", "db2", "db33", "db44")
    val dbDflts = Seq("default", "global_temp")

    withDatabases(dbs: _*) { statement =>
      dbs.foreach(db => statement.execute(s"CREATE DATABASE IF NOT EXISTS $db"))
      val metaData = statement.getConnection.getMetaData

      Seq("", "*", "%", null, ".*", "_*", "_%", ".%") foreach { pattern =>
        checkResult(metaData.getSchemas(null, pattern), dbs ++ dbDflts)
      }

      Seq("db%", "db*") foreach { pattern =>
        checkResult(metaData.getSchemas(null, pattern), dbs)
      }

      Seq("db_", "db.") foreach { pattern =>
        checkResult(metaData.getSchemas(null, pattern), dbs.take(2))
      }

      checkResult(metaData.getSchemas(null, "db1"), Seq("db1"))
      checkResult(metaData.getSchemas(null, "db_not_exist"), Seq.empty)
    }
  }

  test("get tables") {
    val table_test = "table_1_test"
    val table_external_test = "table_2_test"
    val view_test = "view_1_test"
    val view_global_test = "view_2_test"
    val tables = Seq(table_test, table_external_test, view_test, view_global_test)
    val schemas = Seq("default", "default", "default", "global_temp")
    val tableTypes = Seq("MANAGED", "EXTERNAL", "VIEW", "VIEW")
    withJdbcStatement(view_test, view_global_test, table_test, view_test) { statement =>
      statement.execute(
        s"CREATE TABLE IF NOT EXISTS $table_test(key int) USING parquet COMMENT '$table_test'")
      val loc = Utils.createTempDir()
      statement.execute(s"CREATE EXTERNAL TABLE IF NOT EXISTS $table_external_test(key int)" +
        s" COMMENT '$table_external_test' LOCATION '$loc'")
      statement.execute(s"CREATE VIEW IF NOT EXISTS $view_test COMMENT '$view_test'" +
        s" AS SELECT * FROM $table_test")
      statement.execute(s"CREATE GLOBAL TEMP VIEW $view_global_test" +
        s" COMMENT '$view_global_test' AS SELECT * FROM $table_test")

      val metaData = statement.getConnection.getMetaData
      val rs1 = metaData.getTables(null, null, null, null)
      var i = 0
      while(rs1.next()) {
        assert(rs1.getString(TABLE_CAT).isEmpty)
        assert(rs1.getString(TABLE_SCHEM) === schemas(i))
        assert(rs1.getString(TABLE_NAME) == tables(i))
        assert(rs1.getString(TABLE_TYPE) == tableTypes(i))
        assert(rs1.getString(REMARKS) === tables(i).replace(view_global_test, ""))
        i += 1
      }
      assert(i === 4)

      val rs2 = metaData.getTables(null, null, null, Array("VIEW"))
      i = 2
      while(rs2.next()) {
        assert(rs2.getString(TABLE_NAME) == tables(i))
        i += 1
      }
      assert(i === 4)

      val rs3 = metaData.getTables(null, null, "table%", Array("VIEW"))
      assert(!rs3.next())

      val rs4 = metaData.getTables(null, null, "table%", Array("TABLE"))
      i = 0
      while(rs4.next()) {
        assert(rs4.getString(TABLE_NAME) == tables(i))
        i += 1
      }
      assert(i === 2)

      val rs5 = metaData.getTables(null, "default", "%", Array("VIEW"))
      i = 2
      while(rs5.next()) {
        assert(rs5.getString(TABLE_NAME) == view_test)
      }

      val e = intercept[HiveSQLException](metaData.getTables(null, "*", null, null))
      assert(e.getCause.getMessage contains "Dangling meta character '*' near index 0\n*\n^")
    }
  }

  test("get type info") {
    withJdbcStatement() { statement =>
      val typeInfo = statement.getConnection.getMetaData.getTypeInfo
      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "VOID")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.NULL)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "BOOLEAN")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.BOOLEAN)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "TINYINT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.TINYINT)
      assert(typeInfo.getInt(PRECISION) === 3)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "SMALLINT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.SMALLINT)
      assert(typeInfo.getInt(PRECISION) === 5)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "INTEGER")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.INTEGER)
      assert(typeInfo.getInt(PRECISION) === 10)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "BIGINT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.BIGINT)
      assert(typeInfo.getInt(PRECISION) === 19)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "FLOAT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.FLOAT)
      assert(typeInfo.getInt(PRECISION) === 7)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "DOUBLE")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.DOUBLE)
      assert(typeInfo.getInt(PRECISION) === 15)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "STRING")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.VARCHAR)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "BINARY")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.BINARY)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "DECIMAL")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.DECIMAL)
      assert(typeInfo.getInt(PRECISION) === 38)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "DATE")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.DATE)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "TIMESTAMP")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.TIMESTAMP)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "ARRAY")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.ARRAY)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "MAP")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.JAVA_OBJECT)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "STRUCT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.STRUCT)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "INTERVAL")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.OTHER)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)
    }
  }

  test("execute statement -  select null") {
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

  test("execute statement -  select boolean") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT false AS col")
      assert(resultSet.next())
      assert(!resultSet.getBoolean("col"))
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.BOOLEAN)
      assert(metaData.getPrecision(1) === 1)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement -  select tinyint") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 1Y AS col")
      assert(resultSet.next())
      assert(resultSet.getByte("col") === 1.toByte)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.TINYINT)
      assert(metaData.getPrecision(1) === 3)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement -  select smallint") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 1S AS col")
      assert(resultSet.next())
      assert(resultSet.getShort("col") === 1.toShort)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.SMALLINT)
      assert(metaData.getPrecision(1) === 5)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement -  select int") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 4 AS col")
      assert(resultSet.next())
      assert(resultSet.getInt("col") === 4)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.INTEGER)
      assert(metaData.getPrecision(1) === 10)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement -  select long") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 4L AS col")
      assert(resultSet.next())
      assert(resultSet.getLong("col") === 4L)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.BIGINT)
      assert(metaData.getPrecision(1) === 19)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement -  select float") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT cast(1.2 as float) AS col")
      assert(resultSet.next())
      assert(resultSet.getFloat("col") === 1.2f)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.FLOAT)
      assert(metaData.getPrecision(1) === 7)
      assert(metaData.getScale(1) === 7)
    }
  }

  test("execute statement -  select double") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 4.2D AS col")
      assert(resultSet.next())
      assert(resultSet.getDouble("col") === 4.2d)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.DOUBLE)
      assert(metaData.getPrecision(1) === 15)
      assert(metaData.getScale(1) === 15)
    }
  }

  test("execute statement -  select string") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 'kentyao' AS col")
      assert(resultSet.next())
      assert(resultSet.getString("col") === "kentyao")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.VARCHAR)
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement -  select binary") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT cast('kyuubi' as binary) AS col")
      assert(resultSet.next())
      assert(resultSet.getObject("col") === "kyuubi".getBytes)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.BINARY)
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement -  select date") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT DATE '2018-11-17' AS col")
      assert(resultSet.next())
      assert(resultSet.getDate("col") === Date.valueOf("2018-11-17"))
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.DATE)
      assert(metaData.getPrecision(1) === 10)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement -  select timestamp") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT TIMESTAMP '2018-11-17 13:33:33' AS col")
      assert(resultSet.next())
      assert(resultSet.getTimestamp("col") === Timestamp.valueOf("2018-11-17 13:33:33"))
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.TIMESTAMP)
      assert(metaData.getPrecision(1) === 29)
      assert(metaData.getScale(1) === 9)
    }
  }

  test("execute statement -  select interval") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT interval '1' day AS col")
      assert(resultSet.next())
      assert(resultSet.getString("col") === "1 days")
      assert(resultSet.getMetaData.getColumnType(1) === java.sql.Types.VARCHAR)
      val metaData = resultSet.getMetaData
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getScale(1) === 0)
    }
  }

  test("execute statement -  select array") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT array() AS col1, array(1) AS col2")
      assert(resultSet.next())
      assert(resultSet.getObject("col1") === "[]")
      assert(resultSet.getObject("col2") === "[1]")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.ARRAY)
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getPrecision(2) == Int.MaxValue)
      assert(metaData.getScale(1) == 0)
      assert(metaData.getScale(2) == 0)
    }
  }

  test("execute statement - select map") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT map() AS col1, map(1, 2, 3, 4) AS col2")
      assert(resultSet.next())
      assert(resultSet.getObject("col1") === "{}")
      assert(resultSet.getObject("col2") === "{1:2,3:4}")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.JAVA_OBJECT)
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getPrecision(2) == Int.MaxValue)
      assert(metaData.getScale(1) == 0)
      assert(metaData.getScale(2) == 0)
    }
  }

  test("execute statement - select struct") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery(
        "SELECT struct('1', '2') AS col1, named_struct('a', 2, 'b', 4) AS col2")
      assert(resultSet.next())
      assert(resultSet.getObject("col1") === "{\"col1\":\"1\",\"col2\":\"2\"}")
      assert(resultSet.getObject("col2") === "{\"a\":2,\"b\":4}")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.STRUCT)
      assert(metaData.getPrecision(1) === Int.MaxValue)
      assert(metaData.getPrecision(2) == Int.MaxValue)
      assert(metaData.getScale(1) == 0)
      assert(metaData.getScale(2) == 0)
    }
  }

  test("execute statement - analysis exception") {
    val sql = "select date_sub(date'2011-11-11', '1.2')"

    withJdbcStatement() { statement =>
      val e = intercept[SQLException] {
        statement.executeQuery(sql)
      }
      assert(e.getMessage
        .contains("The second argument of 'date_sub' function needs to be an integer."))
    }
  }
}
