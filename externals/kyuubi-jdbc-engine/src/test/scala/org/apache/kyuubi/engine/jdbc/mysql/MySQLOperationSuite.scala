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
package org.apache.kyuubi.engine.jdbc.mysql

import java.sql.{DriverManager, ResultSet}

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.engine.jdbc.MetadataTestHelpers._
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._

abstract class MySQLOperationSuite extends WithMySQLEngine with HiveJDBCTestHelper {
  test("mysql - get tables") {
    case class Table(catalog: String, schema: String, tableName: String, tableType: String)

    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val resultBuffer = ArrayBuffer[Table]()

      var tables = meta.getTables(null, null, null, null)
      while (tables.next()) {
        resultBuffer +=
          Table(
            tables.getString(TABLE_CAT),
            tables.getString(TABLE_SCHEM),
            tables.getString(TABLE_NAME),
            tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table("information_schema", null, "TABLES", "SYSTEM VIEW")))
      assert(resultBuffer.contains(Table("information_schema", null, "VIEWS", "SYSTEM VIEW")))
      resultBuffer.clear()

      statement.execute("create database if not exists db1")
      statement.execute("create table db1.test1(id bigint)" +
        "ENGINE=InnoDB DEFAULT CHARSET=utf8;")
      statement.execute("create table db1.test2(id bigint)" +
        "ENGINE=InnoDB DEFAULT CHARSET=utf8;")

      statement.execute("create database if not exists db2")
      statement.execute("create table db2.test1(id bigint)" +
        "ENGINE=InnoDB DEFAULT CHARSET=utf8;")
      statement.execute("create table db2.test2(id bigint)" +
        "ENGINE=InnoDB DEFAULT CHARSET=utf8;")

      statement.execute("create view db1.view1 (k1) as select id from db1.test1")

      tables = meta.getTables("db1", null, "test1", Array("TABLE"))
      while (tables.next()) {
        val table = Table(
          tables.getString(TABLE_CAT),
          tables.getString(TABLE_SCHEM),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
        assert(table == Table("db1", null, "test1", "TABLE"))
      }

      tables = meta.getTables("db1", null, null, null)
      while (tables.next()) {
        resultBuffer += Table(
          tables.getString(TABLE_CAT),
          tables.getString(TABLE_SCHEM),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table("db1", null, "test2", "TABLE")))
      resultBuffer.clear()

      tables = meta.getTables(null, null, "test1", null)
      while (tables.next()) {
        resultBuffer += Table(
          tables.getString(TABLE_CAT),
          tables.getString(TABLE_SCHEM),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table("db1", null, "test1", "TABLE")))
      assert(resultBuffer.contains(Table("db2", null, "test1", "TABLE")))
      resultBuffer.clear()

      // NOTE: per JDBC spec, the `catalog` argument of getTables is not a pattern (only
      // schemaPattern / tableNamePattern / columnNamePattern accept LIKE wildcards). Under
      // mysql-connector-j default databaseTerm=CATALOG the database is the catalog, so
      // pattern-matching across databases via getTables is not available - the previous
      // hand-written SQL exposed this; the standard API does not.

      tables = meta.getTables("db2", null, "test%", null)
      while (tables.next()) {
        resultBuffer += Table(
          tables.getString(TABLE_CAT),
          tables.getString(TABLE_SCHEM),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table("db2", null, "test1", "TABLE")))
      assert(resultBuffer.contains(Table("db2", null, "test2", "TABLE")))
      resultBuffer.clear()

      tables = meta.getTables("fake_db", null, "test1", null)
      assert(!tables.next())

      tables = meta.getTables("db1", null, null, Array("VIEW"))
      while (tables.next()) {
        val table = Table(
          tables.getString(TABLE_CAT),
          tables.getString(TABLE_SCHEM),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
        assert(table == Table("db1", null, "view1", "VIEW"))
      }

      tables = meta.getTables(null, null, null, Array("VIEW", "TABLE"))
      while (tables.next()) {
        resultBuffer += Table(
          tables.getString(TABLE_CAT),
          tables.getString(TABLE_SCHEM),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table("db1", null, "test1", "TABLE")))
      assert(resultBuffer.contains(Table("db1", null, "test2", "TABLE")))
      assert(resultBuffer.contains(Table("db2", null, "test1", "TABLE")))
      assert(resultBuffer.contains(Table("db2", null, "test2", "TABLE")))
      assert(resultBuffer.contains(Table("db1", null, "view1", "VIEW")))
      resultBuffer.clear()

      statement.execute("drop view db1.view1")
      statement.execute("drop table db1.test1")
      statement.execute("drop table db1.test2")
      statement.execute("drop table db2.test1")
      statement.execute("drop table db2.test2")
      statement.execute("drop database db1")
      statement.execute("drop database db2")
    }
  }

  test("mysql - get columns") {
    case class Column(tableSchema: String, tableName: String, columnName: String)

    def buildColumn(resultSet: ResultSet): Column = {
      // Under mysql-connector-j default databaseTerm=CATALOG, the database name lives in
      // TABLE_CAT and TABLE_SCHEM is null. Read from TABLE_CAT here.
      val schema = resultSet.getString(TABLE_CAT)
      val tableName = resultSet.getString(TABLE_NAME)
      val columnName = resultSet.getString(COLUMN_NAME)
      val column = Column(schema, tableName, columnName)
      column
    }

    withJdbcStatement() { statement =>
      val metadata = statement.getConnection.getMetaData
      statement.execute("create database if not exists db1")
      statement.execute("create table if not exists db1.test1" +
        "(id bigint, str1 varchar(255), str2 varchar(255), age int)" +
        "ENGINE=InnoDB DEFAULT CHARSET=utf8;")
      statement.execute("create table if not exists db1.test2" +
        "(id bigint, str1 varchar(255), str2 varchar(255), age int)" +
        "ENGINE=InnoDB DEFAULT CHARSET=utf8;")

      statement.execute("create database if not exists db2")

      statement.execute("create table if not exists db2.test1" +
        "(id bigint, str1 varchar(255), str2 varchar(255), age int)" +
        "ENGINE=InnoDB DEFAULT CHARSET=utf8;")

      val resultBuffer = ArrayBuffer[Column]()
      val resultSet1 = metadata.getColumns("db1", null, null, null)
      while (resultSet1.next()) {
        val column = buildColumn(resultSet1)
        resultBuffer += column
      }

      assert(resultBuffer.contains(Column("db1", "test1", "id")))
      assert(resultBuffer.contains(Column("db1", "test1", "str1")))
      assert(resultBuffer.contains(Column("db1", "test1", "str2")))
      assert(resultBuffer.contains(Column("db1", "test1", "age")))

      assert(resultBuffer.contains(Column("db1", "test2", "id")))
      assert(resultBuffer.contains(Column("db1", "test2", "str1")))
      assert(resultBuffer.contains(Column("db1", "test2", "str2")))
      assert(resultBuffer.contains(Column("db1", "test2", "age")))

      resultBuffer.clear()

      val resultSet2 = metadata.getColumns(null, null, "test1", null)
      while (resultSet2.next()) {
        val column = buildColumn(resultSet2)
        resultBuffer += column
      }

      assert(resultBuffer.contains(Column("db1", "test1", "id")))
      assert(resultBuffer.contains(Column("db1", "test1", "str1")))
      assert(resultBuffer.contains(Column("db1", "test1", "str2")))
      assert(resultBuffer.contains(Column("db1", "test1", "age")))

      assert(resultBuffer.contains(Column("db2", "test1", "id")))
      assert(resultBuffer.contains(Column("db2", "test1", "str1")))
      assert(resultBuffer.contains(Column("db2", "test1", "str2")))
      assert(resultBuffer.contains(Column("db2", "test1", "age")))

      resultBuffer.clear()

      val resultSet3 = metadata.getColumns(null, null, null, "age")
      while (resultSet3.next()) {
        val column = buildColumn(resultSet3)
        resultBuffer += column
      }

      assert(resultBuffer.contains(Column("db1", "test1", "age")))
      assert(resultBuffer.contains(Column("db1", "test2", "age")))
      assert(resultBuffer.contains(Column("db2", "test1", "age")))

      resultBuffer.clear()

      // NOTE: per JDBC spec, the `catalog` argument of getColumns is not a pattern (only
      // tableNamePattern / columnNamePattern accept LIKE wildcards). Under mysql-connector-j
      // default databaseTerm=CATALOG the database is the catalog, so the previous "d%1"
      // pattern-on-database sub-test cannot be expressed via the standard API.

      val resultSet4 = metadata.getColumns("db1", null, "t%1", "str%")
      while (resultSet4.next()) {
        val column = buildColumn(resultSet4)
        resultBuffer += column
      }

      assert(resultBuffer.contains(Column("db1", "test1", "str1")))
      assert(resultBuffer.contains(Column("db1", "test1", "str2")))

      resultBuffer.clear()

      val resultSet5 = metadata.getColumns("db1", null, "t%1", "fake")
      assert(!resultSet5.next())

      statement.execute("drop table db1.test1")
      statement.execute("drop table db1.test2")
      statement.execute("drop database db1")
      statement.execute("drop table db2.test1")
      statement.execute("drop database db2")
    }
  }

  test("mysql - additional metadata operations") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      assert(collectCol(meta.getCatalogs, TABLE_CAT).contains("information_schema"))
      // mysql-connector-j default databaseTerm=CATALOG: schemas are reported as null for the
      // server-level getSchemas call. Just confirm the call completes and returns a result set.
      drain(meta.getSchemas)
      assert(collectCol(meta.getTableTypes, "TABLE_TYPE").contains("TABLE"))
      assert(rowCount(meta.getTypeInfo) > 0)

      // `DatabaseMetaData#getFunctions` reports stored functions, not built-ins. A clean MySQL
      // container has none, so create one explicitly and assert it surfaces, then clean up.
      try {
        statement.execute("create database if not exists kyuubi_meta_db")
        statement.execute("drop function if exists kyuubi_meta_db.kyuubi_test_fn")
        statement.execute(
          "create function kyuubi_meta_db.kyuubi_test_fn() returns int " +
            "deterministic return 1")
        assert(collectCol(
          meta.getFunctions("kyuubi_meta_db", null, "kyuubi_test_fn"),
          "FUNCTION_NAME").contains("kyuubi_test_fn"))
      } finally {
        statement.execute("drop function if exists kyuubi_meta_db.kyuubi_test_fn")
        statement.execute("drop database if exists kyuubi_meta_db")
      }
    }
  }

  test("mysql - runtime catalog/database switching via KyuubiConnection") {
    // Exercises the runtime lifecycle path (SetCurrentCatalog / SetCurrentDatabase ops, fired
    // when a client calls Connection#setCatalog / setSchema *after* the session is open). The
    // URL-database test above covers only the session-open path.
    //
    // Use two distinct databases so we can prove the *second* call actually moves the backend:
    // MySQLDialect double-writes both catalog and schema, so reusing the same target across
    // setSchema/setCatalog would let a no-op setCatalog look correct. `select database()` is
    // read directly from the backend - the strongest assertion that the current-database state
    // really shifted.
    def assertBackendDatabase(connection: java.sql.Connection, expected: String): Unit = {
      val s = connection.createStatement()
      try {
        val rs = s.executeQuery("select database()")
        try {
          assert(rs.next())
          assert(rs.getString(1) == expected)
        } finally rs.close()
      } finally s.close()
    }

    withJdbcStatement() { statement =>
      try {
        statement.execute("create database if not exists kyuubi_switch_db1")
        statement.execute("create database if not exists kyuubi_switch_db2")

        val connection = DriverManager.getConnection(
          jdbcUrlWithConf(s"jdbc:hive2://$connectionUrl/"),
          user,
          password)
        try {
          connection.setSchema("kyuubi_switch_db1")
          assert(connection.getSchema == "kyuubi_switch_db1")
          assertBackendDatabase(connection, "kyuubi_switch_db1")

          connection.setCatalog("kyuubi_switch_db2")
          assert(connection.getCatalog == "kyuubi_switch_db2")
          assertBackendDatabase(connection, "kyuubi_switch_db2")
        } finally connection.close()
      } finally {
        statement.execute("drop database if exists kyuubi_switch_db1")
        statement.execute("drop database if exists kyuubi_switch_db2")
      }
    }
  }

  test("mysql - URL database is applied to backend connection") {
    withJdbcStatement() { statement =>
      try {
        statement.execute("create database if not exists db1")

        val connection = DriverManager.getConnection(
          jdbcUrlWithConf(s"jdbc:hive2://$connectionUrl/db1;"),
          user,
          password)
        try {
          val s = connection.createStatement()
          try {
            val rs = s.executeQuery("select database()")
            try {
              assert(rs.next())
              assert(rs.getString(1) == "db1")
            } finally rs.close()
          } finally s.close()
        } finally connection.close()
      } finally {
        statement.execute("drop database if exists db1")
      }
    }
  }
}
