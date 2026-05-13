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
package org.apache.kyuubi.engine.jdbc.postgresql

import java.sql.{DriverManager, ResultSet}

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.engine.jdbc.MetadataTestHelpers._
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._

abstract class PostgreSQLOperationSuite extends WithPostgreSQLEngine with HiveJDBCTestHelper {
  test("postgreSQL - get catalog") {
    case class Catalog(catalog: String)

    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val resultBuffer = ArrayBuffer[Catalog]()

      val catalogs = meta.getCatalogs
      while (catalogs.next()) {
        resultBuffer +=
          Catalog(catalogs.getString(TABLE_CAT))
      }
      assert(resultBuffer.contains(Catalog("postgres")))
      resultBuffer.clear()

    }
  }

  test("postgreSQL - get schemas") {
    case class Schema(catalog: String, schema: String)

    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val resultBuffer = ArrayBuffer[Schema]()

      val schemas = meta.getSchemas
      while (schemas.next()) {
        resultBuffer +=
          Schema(schemas.getString(TABLE_CATALOG), schemas.getString(TABLE_SCHEM))
      }
      assert(resultBuffer.contains(Schema("postgres", "information_schema")))
      resultBuffer.clear()
    }
  }

  test("postgreSQL - get tables") {
    case class Table(catalog: String, schema: String, tableName: String, tableType: String)

    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val resultBuffer = ArrayBuffer[Table]()

      var tables = meta.getTables(null, null, null, null)
      while (tables.next()) {
        resultBuffer +=
          Table(
            null,
            null,
            tables.getString(TABLE_NAME),
            tables.getString(TABLE_TYPE))
      }
      // PostgreSQL JDBC driver classifies tables/views in pg_catalog as SYSTEM TABLE/SYSTEM VIEW,
      // and user tables/views as TABLE/VIEW (not "BASE TABLE", which only appears in
      // INFORMATION_SCHEMA).
      assert(resultBuffer.contains(Table(null, null, "pg_statistic", "SYSTEM TABLE")))
      assert(resultBuffer.contains(Table(null, null, "pg_roles", "SYSTEM VIEW")))
      resultBuffer.clear()

      statement.execute("create table public.test1(id bigint primary key)")
      statement.execute("create table public.test2(id bigint primary key)")

      tables = meta.getTables(null, null, "test1", Array("TABLE"))
      while (tables.next()) {
        val table = Table(
          null,
          null,
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
        assert(table == Table(null, null, "test1", "TABLE"))
      }

      tables = meta.getTables(null, null, "test2", null)
      while (tables.next()) {
        resultBuffer += Table(
          null,
          null,
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table(null, null, "test2", "TABLE")))
      resultBuffer.clear()

      tables = meta.getTables(null, null, null, Array("TABLE"))
      while (tables.next()) {
        resultBuffer += Table(
          null,
          null,
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table(null, null, "test1", "TABLE")))
      assert(resultBuffer.contains(Table(null, null, "test2", "TABLE")))
      resultBuffer.clear()

      tables = meta.getTables(null, null, null, Array("TABLE", "SYSTEM VIEW"))
      while (tables.next()) {
        resultBuffer += Table(
          null,
          null,
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table(null, null, "test1", "TABLE")))
      assert(resultBuffer.contains(Table(null, null, "test2", "TABLE")))
      assert(resultBuffer.contains(Table(null, null, "pg_shadow", "SYSTEM VIEW")))
      assert(resultBuffer.contains(Table(null, null, "pg_roles", "SYSTEM VIEW")))
      resultBuffer.clear()

      statement.execute("drop table public.test1")
      statement.execute("drop table public.test2")
    }
  }

  test("postgreSQL - get columns") {
    case class Column(tableName: String, columnName: String)

    def buildColumn(resultSet: ResultSet): Column = {
      val tableName = resultSet.getString(TABLE_NAME)
      val columnName = resultSet.getString(COLUMN_NAME)
      val column = Column(tableName, columnName)
      column
    }

    withJdbcStatement() { statement =>
      val metadata = statement.getConnection.getMetaData
      statement.execute("create table if not exists public.test1" +
        "(id bigint primary key, str1 varchar, str2 varchar, age integer)")

      statement.execute("create table if not exists public.test2" +
        "(id bigint primary key, str1 varchar, str2 varchar, age integer)")

      val resultBuffer = ArrayBuffer[Column]()
      val resultSet1 = metadata.getColumns(null, null, null, null)
      while (resultSet1.next()) {
        val column = buildColumn(resultSet1)
        resultBuffer += column
      }

      assert(resultBuffer.contains(Column("test1", "id")))
      assert(resultBuffer.contains(Column("test1", "str1")))
      assert(resultBuffer.contains(Column("test1", "str2")))
      assert(resultBuffer.contains(Column("test1", "age")))

      assert(resultBuffer.contains(Column("test2", "id")))
      assert(resultBuffer.contains(Column("test2", "str1")))
      assert(resultBuffer.contains(Column("test2", "str2")))
      assert(resultBuffer.contains(Column("test2", "age")))

      resultBuffer.clear()

      val resultSet2 = metadata.getColumns(null, null, "test1", null)
      while (resultSet2.next()) {
        val column = buildColumn(resultSet2)
        resultBuffer += column
      }

      assert(resultBuffer.contains(Column("test1", "id")))
      assert(resultBuffer.contains(Column("test1", "str1")))
      assert(resultBuffer.contains(Column("test1", "str2")))
      assert(resultBuffer.contains(Column("test1", "age")))

      resultBuffer.clear()

      val resultSet3 = metadata.getColumns(null, null, null, "age")
      while (resultSet3.next()) {
        val column = buildColumn(resultSet3)
        resultBuffer += column
      }

      assert(resultBuffer.contains(Column("test1", "age")))
      assert(resultBuffer.contains(Column("test2", "age")))

      resultBuffer.clear()

      val resultSet4 = metadata.getColumns(null, null, "t%1", "str%")
      while (resultSet4.next()) {
        val column = buildColumn(resultSet4)
        resultBuffer += column
      }

      assert(resultBuffer.contains(Column("test1", "str1")))

      resultBuffer.clear()

      val resultSet5 = metadata.getColumns(null, null, "t%1", "fake")
      assert(!resultSet5.next())

      statement.execute("drop table public.test1")
      statement.execute("drop table public.test2")
    }
  }

  test("postgreSQL - additional metadata operations") {
    // getCatalogs / getSchemas already covered by dedicated tests above.
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      assert(collectCol(meta.getTableTypes, "TABLE_TYPE").contains("TABLE"))
      assert(rowCount(meta.getTypeInfo) > 0)
      assert(rowCount(meta.getFunctions(null, null, "%")) > 0)
    }
  }

  test("postgreSQL - URL database is applied to backend connection") {
    withJdbcStatement() { statement =>
      try {
        statement.execute("create schema if not exists db1")

        val connection = DriverManager.getConnection(
          jdbcUrlWithConf(s"jdbc:hive2://$connectionUrl/db1;"),
          user,
          password)
        try {
          // PostgreSQL exposes the JDBC schema as the connection's `search_path`. After session
          // open the URL-scoped schema is applied via `Connection#setSchema`, so
          // `current_schema()` returns "db1".
          val s = connection.createStatement()
          try {
            val rs = s.executeQuery("select current_schema()")
            try {
              assert(rs.next())
              assert(rs.getString(1) == "db1")
            } finally rs.close()
          } finally s.close()
        } finally connection.close()
      } finally {
        statement.execute("drop schema if exists db1")
      }
    }
  }
}
