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
package org.apache.kyuubi.engine.jdbc.starrocks

import java.sql.{DriverManager, ResultSet}

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.engine.jdbc.MetadataTestHelpers._
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._

abstract class StarRocksOperationSuite extends WithStarRocksEngine with HiveJDBCTestHelper {
  test("starrocks - get tables") {
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
      // StarRocks speaks the MySQL wire protocol; under mysql-connector-j default
      // databaseTerm=CATALOG the database lives in TABLE_CAT and TABLE_SCHEM is null.
      // User tables come back as "TABLE" (not "BASE TABLE").
      assert(resultBuffer.contains(Table("information_schema", null, "tables", "SYSTEM VIEW")))
      assert(resultBuffer.contains(Table("information_schema", null, "views", "SYSTEM VIEW")))
      resultBuffer.clear()

      statement.execute("create database if not exists db1")
      statement.execute("create table db1.test1(id bigint)" +
        "ENGINE=OLAP DISTRIBUTED BY HASH(`id`) BUCKETS 32 " +
        "PROPERTIES ('replication_num' = '1')")
      statement.execute("create table db1.test2(id bigint)" +
        "ENGINE=OLAP DISTRIBUTED BY HASH(`id`) BUCKETS 32 " +
        "PROPERTIES ('replication_num' = '1')")

      statement.execute("create database if not exists db2")
      statement.execute("create table db2.test1(id bigint)" +
        "ENGINE=OLAP DISTRIBUTED BY HASH(`id`) BUCKETS 32 " +
        "PROPERTIES ('replication_num' = '1')")
      statement.execute("create table db2.test2(id bigint)" +
        "ENGINE=OLAP DISTRIBUTED BY HASH(`id`) BUCKETS 32 " +
        "PROPERTIES ('replication_num' = '1')")

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
      assert(resultBuffer.contains(Table("db1", null, "test1", "TABLE")))
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

      // NOTE: per JDBC spec the `catalog` argument of getTables is not a pattern; pattern
      // matching across catalogs (the "db%" sub-test in the SQL-builder days) is no longer
      // expressible via the standard API.

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

  test("starrocks - get columns") {
    case class Column(tableSchema: String, tableName: String, columnName: String)

    def buildColumn(resultSet: ResultSet): Column = {
      // Under mysql-connector-j default databaseTerm=CATALOG the database lives in
      // TABLE_CAT, with TABLE_SCHEM null.
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
        "(id bigint, str1 string, str2 string, age int)" +
        "ENGINE=OLAP DISTRIBUTED BY HASH(`id`) BUCKETS 32 " +
        "PROPERTIES ('replication_num' = '1')")
      statement.execute("create table if not exists db1.test2" +
        "(id bigint, str1 string, str2 string, age int)" +
        "ENGINE=OLAP DISTRIBUTED BY HASH(`id`) BUCKETS 32 " +
        "PROPERTIES ('replication_num' = '1')")

      statement.execute("create database if not exists db2")

      statement.execute("create table if not exists db2.test1" +
        "(id bigint, str1 string, str2 string, age int)" +
        "ENGINE=OLAP DISTRIBUTED BY HASH(`id`) BUCKETS 32 " +
        "PROPERTIES ('replication_num' = '1')")

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

      // NOTE: per JDBC spec the `catalog` argument of getColumns is not a pattern; the
      // previous "d%1" pattern-on-database sub-test cannot be expressed via the standard API.

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

  test("starrocks - additional metadata operations") {
    // `getFunctions` is omitted: under mysql-connector-j it issues `SHOW PROCEDURES/FUNCTIONS`
    // with a non-equal predicate, which StarRocks rejects ("Only support equal predicate in
    // show statement"). Driver-level gap, not engine behavior.
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      assert(collectCol(meta.getCatalogs, TABLE_CAT).contains("information_schema"))
      drain(meta.getSchemas)
      assert(collectCol(meta.getTableTypes, "TABLE_TYPE").contains("TABLE"))
      assert(rowCount(meta.getTypeInfo) > 0)
    }
  }

  test("starrocks - URL database is applied to backend connection") {
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
