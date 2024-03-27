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
package org.apache.kyuubi.engine.jdbc.clickhouse

import java.sql.ResultSet

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._

abstract class ClickHouseOperationSuite extends WithClickHouseEngine with HiveJDBCTestHelper {
  test("clickhouse - get tables") {
    case class Table(catalog: String, schema: String, tableName: String, tableType: String)

    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val resultBuffer = ArrayBuffer[Table]()

      var tables = meta.getTables(null, null, null, null)
      while (tables.next()) {
        resultBuffer +=
          Table(
            tables.getString(TABLE_CATALOG),
            tables.getString(CK_TABLE_SCHEMA),
            tables.getString(TABLE_NAME),
            tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table(
        "INFORMATION_SCHEMA",
        "INFORMATION_SCHEMA",
        "TABLES",
        "VIEW")))
      assert(resultBuffer.contains(Table(
        "INFORMATION_SCHEMA",
        "INFORMATION_SCHEMA",
        "VIEWS",
        "VIEW")))
      resultBuffer.clear()

      statement.execute("create database if not exists db1")
      statement.execute("create table db1.test1(id bigInt) ENGINE=File(TabSeparated)")
      statement.execute("create table db1.test2(id bigint) ENGINE=File(TabSeparated)")

      statement.execute("create database if not exists db2")
      statement.execute("create table db2.test1(id bigint) ENGINE=File(TabSeparated)")
      statement.execute("create table db2.test2(id bigint) ENGINE=File(TabSeparated)")

      statement.execute("create view db1.view1 as select id from db1.test1")

      tables = meta.getTables(null, "db1", "test1", Array("BASE TABLE"))
      while (tables.next()) {
        val table = Table(
          tables.getString(TABLE_CATALOG),
          tables.getString(CK_TABLE_SCHEMA),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
        assert(table == Table("db1", "db1", "test1", "BASE TABLE"))
      }

      tables = meta.getTables("db1", "db1", null, null)
      while (tables.next()) {
        resultBuffer += Table(
          tables.getString(TABLE_CATALOG),
          tables.getString(CK_TABLE_SCHEMA),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table("db1", "db1", "test2", "BASE TABLE")))
      resultBuffer.clear()

      tables = meta.getTables(null, null, "test1", null)
      while (tables.next()) {
        resultBuffer += Table(
          tables.getString(TABLE_CATALOG),
          tables.getString(CK_TABLE_SCHEMA),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table("db1", "db1", "test1", "BASE TABLE")))
      assert(resultBuffer.contains(Table("db2", "db2", "test1", "BASE TABLE")))
      resultBuffer.clear()

      tables = meta.getTables(null, "db%", "test1", null)
      while (tables.next()) {
        resultBuffer += Table(
          tables.getString(TABLE_CATALOG),
          tables.getString(CK_TABLE_SCHEMA),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table("db1", "db1", "test1", "BASE TABLE")))
      assert(resultBuffer.contains(Table("db2", "db2", "test1", "BASE TABLE")))
      resultBuffer.clear()

      tables = meta.getTables(null, "db2", "test%", null)
      while (tables.next()) {
        resultBuffer += Table(
          tables.getString(TABLE_CATALOG),
          tables.getString(CK_TABLE_SCHEMA),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table("db2", "db2", "test1", "BASE TABLE")))
      assert(resultBuffer.contains(Table("db2", "db2", "test2", "BASE TABLE")))
      resultBuffer.clear()

      tables = meta.getTables(null, "fake_db", "test1", null)
      assert(!tables.next())

      tables = meta.getTables(null, "db1", null, Array("VIEW"))
      while (tables.next()) {
        val table = Table(
          tables.getString(TABLE_CATALOG),
          tables.getString(CK_TABLE_SCHEMA),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
        assert(table == Table("db1", "db1", "view1", "VIEW"))
      }

      tables = meta.getTables(null, null, null, Array("VIEW", "BASE TABLE"))
      while (tables.next()) {
        resultBuffer += Table(
          tables.getString(TABLE_CATALOG),
          tables.getString(CK_TABLE_SCHEMA),
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table("db1", "db1", "test1", "BASE TABLE")))
      assert(resultBuffer.contains(Table("db1", "db1", "test2", "BASE TABLE")))
      assert(resultBuffer.contains(Table("db2", "db2", "test1", "BASE TABLE")))
      assert(resultBuffer.contains(Table("db2", "db2", "test2", "BASE TABLE")))
      assert(resultBuffer.contains(Table("db1", "db1", "view1", "VIEW")))
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

  test("clickhouse - get columns") {
    case class Column(name: String, columnType: String)

    def buildColumn(resultSet: ResultSet): Column = {
      val columnName = resultSet.getString("COLUMN_NAME")
      val columnType = resultSet.getString("DATA_TYPE")
      Column(columnName, columnType)
    }

    withJdbcStatement() { statement =>
      val metadata = statement.getConnection.getMetaData
      statement.execute("create table if not exists test1" +
        "(id bigint, str1 String, str2 String, age int) ENGINE=File(TabSeparated)")

      statement.execute("create database db1")
      statement.execute("create table if not exists db1.test2" +
        "(id bigint, str1 String) ENGINE=File(TabSeparated)")

      val resultBuffer = ArrayBuffer[Column]()
      val resultSet1 = metadata.getColumns(null, null, "test1", null)
      while (resultSet1.next()) {
        resultBuffer += buildColumn(resultSet1)
      }

      assert(resultBuffer.contains(Column("id", "Int64")))
      assert(resultBuffer.contains(Column("str1", "String")))
      assert(resultBuffer.contains(Column("str2", "String")))
      assert(resultBuffer.contains(Column("age", "Int32")))

      resultBuffer.clear()

      val resultSet2 = metadata.getColumns(null, null, "test2", null)
      while (resultSet2.next()) {
        resultBuffer += buildColumn(resultSet2)
      }
      assert(resultBuffer.contains(Column("id", "Int64")))
      assert(resultBuffer.contains(Column("str1", "String")))

      statement.execute("drop table test1")
      statement.execute("drop table db1.test2")
      statement.execute("drop database db1")
    }
  }
}
