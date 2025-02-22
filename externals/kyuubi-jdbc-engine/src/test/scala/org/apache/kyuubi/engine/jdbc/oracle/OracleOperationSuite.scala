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
package org.apache.kyuubi.engine.jdbc.oracle

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant.{COLUMN_NAME, TABLE_NAME, TABLE_TYPE}

abstract class OracleOperationSuite extends WithOracleEngine with HiveJDBCTestHelper {
  test("oracle - get tables") {
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
      assert(resultBuffer.contains(Table(null, null, "DUAL", "TABLE")))
      assert(resultBuffer.contains(Table(null, null, "DUAL", "SYNONYM")))
      assert(resultBuffer.contains(Table(null, null, "NLS_SESSION_PARAMETERS", "VIEW")))
      resultBuffer.clear()

      statement.execute("create table T_PEOPLE (ID INTEGER not null " +
        "constraint \"T_PEOPLE_pk\" primary key, NAME VARCHAR2(64))")

      tables = meta.getTables(null, null, "T_PEOPLE", Array("TABLE"))
      while (tables.next()) {
        val table = Table(
          null,
          null,
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
        assert(table == Table(null, null, "T_PEOPLE", "TABLE"))
      }

      tables = meta.getTables(null, null, "%PEOPLE", Array("TABLE"))
      while (tables.next()) {
        val table = Table(
          null,
          null,
          tables.getString(TABLE_NAME),
          tables.getString(TABLE_TYPE))
        assert(table == Table(null, null, "T_PEOPLE", "TABLE"))
      }

      statement.execute("DROP TABLE T_PEOPLE")
    }
  }

  test("oracle - get columns") {
    case class Column(tableName: String, columnName: String)

    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val resultBuffer = ArrayBuffer[Column]()

      var columns = meta.getColumns(null, null, null, null)
      while (columns.next()) {
        resultBuffer +=
          Column(
            columns.getString(TABLE_NAME),
            columns.getString(COLUMN_NAME))
      }
      assert(resultBuffer.contains(Column("DUAL", "DUMMY")))
      resultBuffer.clear()

      statement.execute("create table T_PEOPLE (ID INTEGER not null " +
        "constraint \"T_PEOPLE_pk\" primary key)")

      columns = meta.getColumns(null, null, "%PEOPLE", null)
      while (columns.next()) {
        val column = Column(
          columns.getString(TABLE_NAME),
          columns.getString(COLUMN_NAME))
        assert(column == Column("T_PEOPLE", "ID"))
      }

      statement.execute("DROP TABLE T_PEOPLE")
    }
  }
}
