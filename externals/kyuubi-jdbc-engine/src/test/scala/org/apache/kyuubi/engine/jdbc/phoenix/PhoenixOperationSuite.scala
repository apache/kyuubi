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

import java.sql.ResultSet

import scala.collection.mutable.ArrayBuffer

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
      assert(resultBuffer.contains(Table(null, null, "CATALOG", "s")))
      assert(resultBuffer.contains(Table(null, null, "LOG", "s")))
      resultBuffer.clear()

      statement.execute("create table db1.test1(id bigint primary key)")
      statement.execute("create table db1.test2(id bigint primary key)")

      tables = meta.getTables(null, null, "test1", Array("u"))
      while (tables.next()) {
        val table = Table(
          null,
          null,
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
        assert(table == Table(null, null, "test1", "u"))
      }

      tables = meta.getTables(null, null, "test1", null)
      while (tables.next()) {
        resultBuffer += Table(
          null,
          null,
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table(null, null, "test1", "u")))
      resultBuffer.clear()

      tables = meta.getTables(null, null, null, Array("u"))
      while (tables.next()) {
        resultBuffer += Table(
          null,
          null,
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table(null, null, "test1", "u")))
      assert(resultBuffer.size == 2)
      resultBuffer.clear()

      tables = meta.getTables(null, null, null, Array("u", "s"))
      while (tables.next()) {
        resultBuffer += Table(
          null,
          null,
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table(null, null, "test1", "u")))
      assert(resultBuffer.contains(Table(null, null, "test2", "u")))
      assert(resultBuffer.contains(Table(null, null, "LOG", "s")))
      assert(resultBuffer.contains(Table(null, null, "CATALOG", "s")))
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
      statement.execute("create table if not exists db1.test1" +
        "(id bigint primary key, str1 varchar, str2 varchar, age integer)")

      statement.execute("create table if not exists db2.test1" +
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
      assert(resultBuffer.contains(Column("test1", "age")))

      resultBuffer.clear()

      val resultSet4 = metadata.getColumns(null, null, "t%1", "str%")
      while (resultSet4.next()) {
        val column = buildColumn(resultSet4)
        resultBuffer += column
      }

      assert(resultBuffer.contains(Column("test1", "str1")))
      assert(resultBuffer.contains(Column("test1", "str2")))

      resultBuffer.clear()

      val resultSet5 = metadata.getColumns(null, null, "t%1", "fake")
      assert(!resultSet5.next())

      statement.execute("drop table db1.test1")
      statement.execute("drop table db2.test1")
    }
  }
}
