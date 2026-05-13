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
package org.apache.kyuubi.engine.jdbc.impala

import java.sql.{DriverManager, ResultSet}

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.engine.jdbc.MetadataTestHelpers._
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._

abstract class ImpalaOperationSuite extends WithImpalaEngine with HiveJDBCTestHelper {
  test("impala - get tables") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData

      // `IF NOT EXISTS` keeps the test idempotent across reruns against a long-running Impala.
      statement.execute("create table if not exists test1(id bigint)")
      statement.execute("create table if not exists test2(id bigint)")
      statement.execute("create database if not exists db1")
      statement.execute("create table if not exists db1.test3(id bigint)")

      var tables = meta.getTables(null, null, "test1", null)
      while (tables.next()) {
        assert(tables.getString(TABLE_NAME) == "test1")
      }

      tables = meta.getTables(null, null, "test2", null)
      while (tables.next()) {
        assert(tables.getString(TABLE_NAME) == "test2")
      }

      // Standard JDBC `getTables` accepts SQL LIKE patterns (`%` / `_`), not the `*`
      // wildcard that the previous Impala custom-SQL builder used.
      tables = meta.getTables(null, null, "test%", null)
      val actualTables = ArrayBuffer[String]()
      while (tables.next()) {
        actualTables += tables.getString(TABLE_NAME)
      }
      assert(actualTables.contains("test1"))
      assert(actualTables.contains("test2"))

      tables = meta.getTables(null, "db1", "test%", null)
      while (tables.next()) {
        assert(tables.getString(TABLE_NAME) == "test3")
      }

      statement.execute("drop table test1")
      statement.execute("drop table test2")
      statement.execute("drop table db1.test3")
      statement.execute("drop database db1")
    }
  }

  test("impala - get columns") {
    case class Column(name: String, columnType: String)

    def buildColumn(resultSet: ResultSet): Column = {
      // Standard JDBC `getColumns` returns COLUMN_NAME / TYPE_NAME; the literal
      // "Column" / "Type" labels were specific to the old `show column stats` path.
      val columnName = resultSet.getString(COLUMN_NAME)
      val columnType = resultSet.getString(TYPE_NAME)
      Column(columnName, columnType)
    }

    withJdbcStatement() { statement =>
      val metadata = statement.getConnection.getMetaData
      statement.execute("create table if not exists test1" +
        "(id bigint, str1 string, str2 string, age int)")

      statement.execute("create database if not exists db1")
      statement.execute("create table if not exists db1.test2" +
        "(id bigint, str1 string)")

      val resultBuffer = ArrayBuffer[Column]()
      val resultSet1 = metadata.getColumns(null, null, "test1", null)
      while (resultSet1.next()) {
        resultBuffer += buildColumn(resultSet1)
      }

      assert(resultBuffer.contains(Column("id", "BIGINT")))
      assert(resultBuffer.contains(Column("str1", "STRING")))
      assert(resultBuffer.contains(Column("str2", "STRING")))
      assert(resultBuffer.contains(Column("age", "INT")))

      resultBuffer.clear()

      val resultSet2 = metadata.getColumns(null, "db1", "test2", null)
      while (resultSet2.next()) {
        resultBuffer += buildColumn(resultSet2)
      }
      assert(resultBuffer.contains(Column("id", "BIGINT")))
      assert(resultBuffer.contains(Column("str1", "STRING")))

      statement.execute("drop table test1")
      statement.execute("drop table db1.test2")
      statement.execute("drop database db1")
    }
  }

  test("impala - additional metadata operations") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      drain(meta.getCatalogs)
      assert(collectCol(meta.getSchemas, TABLE_SCHEM).contains("default"))
      assert(collectCol(meta.getTableTypes, "TABLE_TYPE").contains("TABLE"))
      assert(rowCount(meta.getTypeInfo) > 0)
      drain(meta.getFunctions(null, null, "%"))
    }
  }

  test("impala - URL database is applied to backend connection") {
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
            val rs = s.executeQuery("select current_database()")
            try {
              assert(rs.next())
              assert(rs.getString(1) == "db1")
            } finally rs.close()
          } finally s.close()
          // Roundtrip via JDBC `Connection#getSchema` to confirm the engine's
          // `dialect.getCurrentSchema` path also works against Impalad (the default would
          // ship a Kyuubi-private query option Impala rejects).
          assert(connection.getSchema == "db1")
        } finally connection.close()
      } finally {
        statement.execute("drop database if exists db1")
      }
    }
  }
}
