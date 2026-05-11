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
package org.apache.kyuubi.engine.jdbc.phoenix

import java.sql.{DriverManager, ResultSet}

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.engine.jdbc.MetadataTestHelpers._
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._

abstract class PhoenixOperationSuite extends WithPhoenixEngine with HiveJDBCTestHelper {
  test("phoenix - get tables") {
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
      // Phoenix Query Server (Avatica) returns standard JDBC table-type values:
      // user tables/views as "TABLE"/"VIEW", system catalog entries as "SYSTEM TABLE".
      // The single-letter codes ("u"/"s") were specific to Phoenix's custom SQL builder
      // that has been replaced by `DatabaseMetaData.getTables`.
      assert(resultBuffer.contains(Table(null, null, "CATALOG", "SYSTEM TABLE")))
      assert(resultBuffer.contains(Table(null, null, "LOG", "SYSTEM TABLE")))
      resultBuffer.clear()

      statement.execute("drop table if exists db1.test1")
      statement.execute("drop table if exists db1.test2")
      statement.execute("create table db1.test1(id bigint primary key)")
      statement.execute("create table db1.test2(id bigint primary key)")

      tables = meta.getTables(null, null, "TEST1", Array("TABLE"))
      while (tables.next()) {
        val table = Table(
          null,
          null,
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
        assert(table == Table(null, null, "TEST1", "TABLE"))
      }

      tables = meta.getTables(null, null, "TEST2", null)
      while (tables.next()) {
        resultBuffer += Table(
          null,
          null,
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table(null, null, "TEST2", "TABLE")))
      resultBuffer.clear()

      tables = meta.getTables(null, null, null, Array("TABLE"))
      while (tables.next()) {
        resultBuffer += Table(
          null,
          null,
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table(null, null, "TEST1", "TABLE")))
      assert(resultBuffer.contains(Table(null, null, "TEST2", "TABLE")))
      resultBuffer.clear()

      tables = meta.getTables(null, null, null, Array("TABLE", "SYSTEM TABLE"))
      while (tables.next()) {
        resultBuffer += Table(
          null,
          null,
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table(null, null, "TEST1", "TABLE")))
      assert(resultBuffer.contains(Table(null, null, "TEST2", "TABLE")))
      assert(resultBuffer.contains(Table(null, null, "LOG", "SYSTEM TABLE")))
      assert(resultBuffer.contains(Table(null, null, "CATALOG", "SYSTEM TABLE")))
      resultBuffer.clear()

      statement.execute("drop table db1.test1")
      statement.execute("drop table db1.test2")
    }
  }

  test("phoenix - get columns") {
    case class Column(tableName: String, columnName: String)

    def buildColumn(resultSet: ResultSet): Column = {
      val tableName = resultSet.getString(TABLE_NAME)
      val columnName = resultSet.getString(COLUMN_NAME)
      val column = Column(tableName, columnName)
      column
    }

    withJdbcStatement() { statement =>
      val metadata = statement.getConnection.getMetaData
      statement.execute("drop table if exists db1.test1")
      statement.execute("drop table if exists db1.test2")
      statement.execute("create table db1.test1" +
        "(id bigint primary key, str1 varchar, str2 varchar, age integer)")
      statement.execute("create table db1.test2" +
        "(id bigint primary key, str1 varchar, str2 varchar, age integer)")

      val resultBuffer = ArrayBuffer[Column]()
      val resultSet1 = metadata.getColumns(null, null, null, null)
      while (resultSet1.next()) {
        val column = buildColumn(resultSet1)
        resultBuffer += column
      }

      assert(resultBuffer.contains(Column("TEST1", "ID")))
      assert(resultBuffer.contains(Column("TEST1", "STR1")))
      assert(resultBuffer.contains(Column("TEST1", "STR2")))
      assert(resultBuffer.contains(Column("TEST1", "AGE")))

      assert(resultBuffer.contains(Column("TEST2", "ID")))
      assert(resultBuffer.contains(Column("TEST2", "STR1")))
      assert(resultBuffer.contains(Column("TEST2", "STR2")))
      assert(resultBuffer.contains(Column("TEST2", "AGE")))

      resultBuffer.clear()

      val resultSet2 = metadata.getColumns(null, null, "TEST1", null)
      while (resultSet2.next()) {
        val column = buildColumn(resultSet2)
        resultBuffer += column
      }

      assert(resultBuffer.contains(Column("TEST1", "ID")))
      assert(resultBuffer.contains(Column("TEST1", "STR1")))
      assert(resultBuffer.contains(Column("TEST1", "STR2")))
      assert(resultBuffer.contains(Column("TEST1", "AGE")))

      resultBuffer.clear()

      val resultSet3 = metadata.getColumns(null, null, null, "AGE")
      while (resultSet3.next()) {
        val column = buildColumn(resultSet3)
        resultBuffer += column
      }

      assert(resultBuffer.contains(Column("TEST1", "AGE")))
      assert(resultBuffer.contains(Column("TEST2", "AGE")))

      resultBuffer.clear()

      val resultSet4 = metadata.getColumns(null, null, "T%1", "STR%")
      while (resultSet4.next()) {
        val column = buildColumn(resultSet4)
        resultBuffer += column
      }

      assert(resultBuffer.contains(Column("TEST1", "STR1")))

      resultBuffer.clear()

      val resultSet5 = metadata.getColumns(null, null, "T%1", "fake")
      assert(!resultSet5.next())

      statement.execute("drop table db1.test1")
      statement.execute("drop table db1.test2")
    }
  }

  test("phoenix - additional metadata operations") {
    // Phoenix Avatica's queryserver implements these unevenly: getTypeInfo returns a result
    // set with zero columns (which would crash Thrift serialisation if `TTableSchema.columns`
    // is left null - see SchemaHelper). getFunctions is not implemented at all so we omit it.
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      drain(meta.getCatalogs)
      drain(meta.getSchemas)
      assert(collectCol(meta.getTableTypes, "TABLE_TYPE").contains("TABLE"))
      drain(meta.getTypeInfo)
    }
  }

  test("phoenix - URL database is applied to backend connection") {
    // Phoenix has no `current_database()` / `current_schema()` SQL function. Verify the
    // URL-scoped schema reaches the backend connection by reading it back via
    // `connection.getSchema()`, which roundtrips through `_GET_DATABASE` to the engine's
    // `dialect.getCurrentSchema` and then to the Phoenix backend connection.
    val connection = DriverManager.getConnection(
      jdbcUrlWithConf(s"jdbc:hive2://$connectionUrl/db1;"),
      user,
      password)
    try {
      assert(connection.getSchema == "db1")
    } finally connection.close()
  }
}
