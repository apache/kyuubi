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

import java.sql.DriverManager

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.engine.jdbc.MetadataTestHelpers._
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant.{COLUMN_NAME, TABLE_NAME, TABLE_SCHEM, TABLE_TYPE}

abstract class OracleOperationSuite extends WithOracleEngine with HiveJDBCTestHelper {
  test("oracle - get tables") {
    case class Table(catalog: String, schema: String, tableName: String, tableType: String)

    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val resultBuffer = ArrayBuffer[Table]()

      // Narrow the metadata scan with a schema filter so the result stays small enough to be
      // viable over slow links (the standard JDBC path returns the full catalog otherwise,
      // which on Oracle is tens of thousands of rows).
      var tables = meta.getTables(null, "SYS", null, null)
      while (tables.next()) {
        resultBuffer +=
          Table(
            null,
            null,
            tables.getString(TABLE_NAME),
            tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table(null, null, "DUAL", "TABLE")))
      assert(resultBuffer.contains(Table(null, null, "NLS_SESSION_PARAMETERS", "VIEW")))
      resultBuffer.clear()

      tables = meta.getTables(null, "PUBLIC", "DUAL", Array("SYNONYM"))
      while (tables.next()) {
        resultBuffer +=
          Table(
            null,
            null,
            tables.getString(TABLE_NAME),
            tables.getString(TABLE_TYPE))
      }
      assert(resultBuffer.contains(Table(null, null, "DUAL", "SYNONYM")))
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

      // Narrow the metadata scan with a schema/table filter (full-catalog `getColumns` is
      // tens of thousands of rows on Oracle and unacceptably slow over a remote link).
      var columns = meta.getColumns(null, "SYS", "DUAL", null)
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

  test("oracle - additional metadata operations") {
    val schema = externalOracleUser.toUpperCase
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      // Oracle has no JDBC catalogs; Oracle JDBC driver returns no rows here.
      drain(meta.getCatalogs)
      assert(collectCol(meta.getSchemas(null, schema), TABLE_SCHEM).contains(schema))
      assert(collectCol(meta.getTableTypes, "TABLE_TYPE").contains("TABLE"))
      assert(rowCount(meta.getTypeInfo) > 0)
      // Functions in the user schema are typically empty for the bare APP_USER, but the call
      // must not throw and must serialise its result set cleanly.
      drain(meta.getFunctions(null, schema, "%"))
    }
  }

  test("oracle - URL database is applied to backend connection") {
    // Oracle treats schemas as users; we point at the same user the engine connects as, since
    // `setSchema` requires the schema to exist and we cannot create one without DBA privileges.
    // Oracle uppercases unquoted identifiers, so the schema name comes back as the user
    // upper-cased. This still exercises the URL-scoped `setSchema` code path end-to-end.
    val schema = externalOracleUser.toUpperCase
    val connection = DriverManager.getConnection(
      jdbcUrlWithConf(s"jdbc:hive2://$connectionUrl/$schema;"),
      user,
      password)
    try {
      val s = connection.createStatement()
      try {
        val rs = s.executeQuery("select sys_context('USERENV','CURRENT_SCHEMA') from dual")
        try {
          assert(rs.next())
          assert(rs.getString(1) == schema)
        } finally rs.close()
      } finally s.close()
    } finally connection.close()
  }
}
