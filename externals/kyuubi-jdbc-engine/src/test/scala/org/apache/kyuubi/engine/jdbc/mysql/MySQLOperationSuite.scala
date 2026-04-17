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
            tables.getString(TABLE_CATALOG),
            tables.getString(TABLE_SCHEMA),
            tables.getString(TABLE_NAME),
            tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table("def", "information_schema", "TABLES", "SYSTEM VIEW")))
      assert(resultBuffer.contains(Table("def", "information_schema", "VIEWS", "SYSTEM VIEW")))
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

      tables = meta.getTables(null, "db1", "test1", Array("BASE TABLE"))
      while (tables.next()) {
        val table = Table(
          tables.getString(TABLE_CATALOG),
          tables.getString(TABLE_SCHEMA),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
        assert(table == Table("def", "db1", "test1", "BASE TABLE"))
      }

      tables = meta.getTables("def", "db1", null, null)
      while (tables.next()) {
        resultBuffer += Table(
          tables.getString(TABLE_CATALOG),
          tables.getString(TABLE_SCHEMA),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table("def", "db1", "test2", "BASE TABLE")))
      resultBuffer.clear()

      tables = meta.getTables(null, null, "test1", null)
      while (tables.next()) {
        resultBuffer += Table(
          tables.getString(TABLE_CATALOG),
          tables.getString(TABLE_SCHEMA),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table("def", "db1", "test1", "BASE TABLE")))
      assert(resultBuffer.contains(Table("def", "db2", "test1", "BASE TABLE")))
      resultBuffer.clear()

      tables = meta.getTables(null, "db%", "test1", null)
      while (tables.next()) {
        resultBuffer += Table(
          tables.getString(TABLE_CATALOG),
          tables.getString(TABLE_SCHEMA),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table("def", "db1", "test1", "BASE TABLE")))
      assert(resultBuffer.contains(Table("def", "db2", "test1", "BASE TABLE")))
      resultBuffer.clear()

      tables = meta.getTables(null, "db2", "test%", null)
      while (tables.next()) {
        resultBuffer += Table(
          tables.getString(TABLE_CATALOG),
          tables.getString(TABLE_SCHEMA),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table("def", "db2", "test1", "BASE TABLE")))
      assert(resultBuffer.contains(Table("def", "db2", "test2", "BASE TABLE")))
      resultBuffer.clear()

      tables = meta.getTables(null, "fake_db", "test1", null)
      assert(!tables.next())

      tables = meta.getTables(null, "db1", null, Array("VIEW"))
      while (tables.next()) {
        val table = Table(
          tables.getString(TABLE_CATALOG),
          tables.getString(TABLE_SCHEMA),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
        assert(table == Table("def", "db1", "view1", "VIEW"))
      }

      tables = meta.getTables(null, null, null, Array("VIEW", "BASE TABLE"))
      while (tables.next()) {
        resultBuffer += Table(
          tables.getString(TABLE_CATALOG),
          tables.getString(TABLE_SCHEMA),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table("def", "db1", "test1", "BASE TABLE")))
      assert(resultBuffer.contains(Table("def", "db1", "test2", "BASE TABLE")))
      assert(resultBuffer.contains(Table("def", "db2", "test1", "BASE TABLE")))
      assert(resultBuffer.contains(Table("def", "db2", "test2", "BASE TABLE")))
      assert(resultBuffer.contains(Table("def", "db1", "view1", "VIEW")))
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
      val schema = resultSet.getString(TABLE_SCHEMA)
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
      val resultSet1 = metadata.getColumns(null, "db1", null, null)
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

      val resultSet4 = metadata.getColumns(null, "d%1", "t%1", "str%")
      while (resultSet4.next()) {
        val column = buildColumn(resultSet4)
        resultBuffer += column
      }

      assert(resultBuffer.contains(Column("db1", "test1", "str1")))
      assert(resultBuffer.contains(Column("db1", "test1", "str2")))

      resultBuffer.clear()

      val resultSet5 = metadata.getColumns(null, "d%1", "t%1", "fake")
      assert(!resultSet5.next())

      statement.execute("drop table db1.test1")
      statement.execute("drop table db1.test2")
      statement.execute("drop database db1")
      statement.execute("drop table db2.test1")
      statement.execute("drop database db2")
    }
  }

  test("mysql - getTables scopes to database from connection URL (KYUUBI #7305)") {
    withJdbcStatement() { statement =>
      statement.execute("create database if not exists db7305a")
      statement.execute("create table db7305a.ta(id bigint)" +
        "ENGINE=InnoDB DEFAULT CHARSET=utf8;")
      statement.execute("create database if not exists db7305b")
      statement.execute("create table db7305b.tb(id bigint)" +
        "ENGINE=InnoDB DEFAULT CHARSET=utf8;")
    }

    val hostAndPort = jdbcUrl.stripPrefix("jdbc:hive2://").split("/;").head
    val scopedUrl = s"jdbc:hive2://$hostAndPort/db7305a;"
    val conn = DriverManager.getConnection(scopedUrl, user, password)
    try {
      val tables = conn.getMetaData.getTables(null, null, "t%", null)
      val found = ArrayBuffer[(String, String)]()
      while (tables.next()) {
        found += ((tables.getString(TABLE_SCHEMA), tables.getString(TABLE_NAME)))
      }
      assert(found.contains(("db7305a", "ta")))
      assert(!found.exists { case (schema, _) => schema == "db7305b" })

      // Hive driver converts schemaPattern=null to "%", and the fix routes "%" back to
      // the effective database, so the behavior should match the call above.
      val tablesPct = conn.getMetaData.getTables(null, "%", "t%", null)
      val foundPct = ArrayBuffer[(String, String)]()
      while (tablesPct.next()) {
        foundPct += ((tablesPct.getString(TABLE_SCHEMA), tablesPct.getString(TABLE_NAME)))
      }
      assert(foundPct.contains(("db7305a", "ta")))
      assert(!foundPct.exists { case (schema, _) => schema == "db7305b" })
    } finally {
      conn.close()
    }

    withJdbcStatement() { statement =>
      statement.execute("drop table db7305a.ta")
      statement.execute("drop database db7305a")
      statement.execute("drop table db7305b.tb")
      statement.execute("drop database db7305b")
    }
  }

  test("mysql - getTables returns all tables when no database in URL") {
    withJdbcStatement() { statement =>
      statement.execute("create database if not exists db7305c")
      statement.execute("create table db7305c.tc(id bigint)" +
        "ENGINE=InnoDB DEFAULT CHARSET=utf8;")
      statement.execute("create database if not exists db7305d")
      statement.execute("create table db7305d.td(id bigint)" +
        "ENGINE=InnoDB DEFAULT CHARSET=utf8;")
    }

    withJdbcStatement() { statement =>
      val tables = statement.getConnection.getMetaData.getTables(null, null, "t%", null)
      val found = ArrayBuffer[(String, String)]()
      while (tables.next()) {
        found += ((tables.getString(TABLE_SCHEMA), tables.getString(TABLE_NAME)))
      }
      // Without USE_DATABASE, the fix must not filter, so both dbs are visible.
      assert(found.contains(("db7305c", "tc")))
      assert(found.contains(("db7305d", "td")))

      statement.execute("drop table db7305c.tc")
      statement.execute("drop database db7305c")
      statement.execute("drop table db7305d.td")
      statement.execute("drop database db7305d")
    }
  }

  test("mysql - getSchemas returns all schemas (KYUUBI #7305)") {
    withJdbcStatement() { statement =>
      statement.execute("create database if not exists db7305e")
      statement.execute("create database if not exists db7305f")
    }

    withJdbcStatement() { statement =>
      val rs = statement.getConnection.getMetaData.getSchemas
      val schemas = ArrayBuffer[String]()
      while (rs.next()) schemas += rs.getString("TABLE_SCHEM")
      assert(schemas.contains("db7305e"))
      assert(schemas.contains("db7305f"))

      statement.execute("drop database db7305e")
      statement.execute("drop database db7305f")
    }
  }

  test("mysql - session open fails when URL specifies a non-existent database") {
    val hostAndPort = jdbcUrl.stripPrefix("jdbc:hive2://").split("/;").head
    val badUrl = s"jdbc:hive2://$hostAndPort/does_not_exist_db_7305;"
    val ex = intercept[java.sql.SQLException] {
      DriverManager.getConnection(badUrl, user, password).close()
    }
    // Error surface could come from the engine or JDBC driver; we only assert it is raised.
    assert(ex != null)
  }
}
