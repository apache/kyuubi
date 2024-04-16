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

package org.apache.kyuubi.engine.flink.operation

import java.nio.file.Paths
import java.sql.DatabaseMetaData
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.flink.api.common.JobID
import org.apache.flink.configuration.PipelineOptions
import org.apache.flink.table.types.logical.LogicalTypeRoot

import org.apache.kyuubi.Utils
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.flink.FlinkEngineUtils.FLINK_RUNTIME_VERSION
import org.apache.kyuubi.engine.flink.WithFlinkTestResources
import org.apache.kyuubi.engine.flink.result.{CommandStrings, Constants}
import org.apache.kyuubi.engine.flink.util.TestUserClassLoaderJar
import org.apache.kyuubi.jdbc.hive.{KyuubiSQLException, KyuubiStatement}
import org.apache.kyuubi.jdbc.hive.common.TimestampTZ
import org.apache.kyuubi.operation.HiveJDBCTestHelper
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

abstract class FlinkOperationSuite extends HiveJDBCTestHelper with WithFlinkTestResources {

  test("get catalogs") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val catalogs = meta.getCatalogs
      val expected = Set("default_catalog").toIterator
      while (catalogs.next()) {
        assert(catalogs.getString(TABLE_CAT) === expected.next())
      }
      assert(!expected.hasNext)
      assert(!catalogs.next())
    }
  }

  test("get columns") {
    val tableName = "flink_get_col_operation"

    withJdbcStatement(tableName) { statement =>
      statement.execute(
        s"""
           | create table $tableName (
           |  c0 boolean,
           |  c1 tinyint,
           |  c2 smallint,
           |  c3 integer,
           |  c4 bigint,
           |  c5 float,
           |  c6 double,
           |  c7 decimal(38,20),
           |  c8 decimal(10,2),
           |  c9 string,
           |  c10 array<bigint>,
           |  c11 array<string>,
           |  c12 map<smallint, tinyint>,
           |  c13 date,
           |  c14 timestamp,
           |  c15 binary
           | )
           | with (
           |   'connector' = 'filesystem'
           | )
    """.stripMargin)

      val metaData = statement.getConnection.getMetaData

      Seq("%", null, ".*", "c.*") foreach { columnPattern =>
        val rowSet = metaData.getColumns("", "", tableName, columnPattern)

        import java.sql.Types._
        val expectedJavaTypes = Seq(
          BOOLEAN,
          TINYINT,
          SMALLINT,
          INTEGER,
          BIGINT,
          FLOAT,
          DOUBLE,
          DECIMAL,
          DECIMAL,
          VARCHAR,
          ARRAY,
          ARRAY,
          JAVA_OBJECT,
          DATE,
          TIMESTAMP,
          BINARY)

        val expectedSqlType = Seq(
          "BOOLEAN",
          "TINYINT",
          "SMALLINT",
          "INT",
          "BIGINT",
          "FLOAT",
          "DOUBLE",
          "DECIMAL(38, 20)",
          "DECIMAL(10, 2)",
          "STRING",
          "ARRAY<BIGINT>",
          "ARRAY<STRING>",
          "MAP<SMALLINT, TINYINT>",
          "DATE",
          "TIMESTAMP(6)",
          "BINARY(1)")

        var pos = 0

        while (rowSet.next()) {
          assert(rowSet.getString(TABLE_CAT) === "default_catalog")
          assert(rowSet.getString(TABLE_SCHEM) === "default_database")
          assert(rowSet.getString(TABLE_NAME) === tableName)
          assert(rowSet.getString(COLUMN_NAME) === s"c$pos")
          assert(rowSet.getInt(DATA_TYPE) === expectedJavaTypes(pos))
          assert(rowSet.getString(TYPE_NAME) === expectedSqlType(pos))
          assert(rowSet.getInt(BUFFER_LENGTH) === 0)
          assert(rowSet.getInt(NULLABLE) === 1)
          assert(rowSet.getInt(ORDINAL_POSITION) === pos)
          assert(rowSet.getString(IS_NULLABLE) === "YES")
          assert(rowSet.getString(IS_AUTO_INCREMENT) === "NO")
          pos += 1
        }

        assert(pos === expectedSqlType.length, "all columns should have been verified")
      }
      val rowSet = metaData.getColumns(null, "*", "not_exist", "not_exist")
      assert(!rowSet.next())
    }
  }

  test("get primary keys") {
    val tableName1 = "flink_get_primary_keys_operation1"
    val tableName2 = "flink_get_primary_keys_operation2"
    val tableName3 = "flink_get_primary_keys_operation3"

    withJdbcStatement(tableName1, tableName2, tableName3) { statement =>
      statement.execute(
        s"""
           | create table $tableName1 (
           |  id1 int,
           |  c1 tinyint,
           |  c2 smallint,
           |  c3 integer,
           |  CONSTRAINT pk_con primary key(id1) NOT ENFORCED
           | )
           | with (
           |   'connector' = 'filesystem'
           | )
    """.stripMargin)

      statement.execute(
        s"""
           | create table $tableName2 (
           |  id1 int,
           |  id2 int,
           |  c1 tinyint,
           |  c2 smallint,
           |  c3 integer,
           |  CONSTRAINT pk_con primary key(id1,id2) NOT ENFORCED
           | )
           | with (
           |   'connector' = 'filesystem'
           | )
    """.stripMargin)

      statement.execute(
        s"""
           | create table $tableName3 (
           |  id1 int,
           |  id2 int,
           |  c1 tinyint,
           |  c2 smallint,
           |  c3 integer
           | )
           | with (
           |   'connector' = 'filesystem'
           | )
    """.stripMargin)

      val metaData = statement.getConnection.getMetaData

      Seq(tableName1, tableName2, tableName3) foreach { tableName =>
        val rowSet = metaData.getPrimaryKeys("", "", tableName)

        if (tableName.equals(tableName3)) {
          assert(!rowSet.next())
        } else {
          if (tableName.equals(tableName1)) {
            assert(rowSet.next())
            assert(rowSet.getString(TABLE_CAT) === "default_catalog")
            assert(rowSet.getString(TABLE_SCHEM) === "default_database")
            assert(rowSet.getString(TABLE_NAME) === tableName)
            assert(rowSet.getString(COLUMN_NAME) === "id1")
            assert(rowSet.getInt(KEY_SEQ) === 1)
            assert(rowSet.getString(PK_NAME) === "pk_con")
          } else if (tableName.equals(tableName2)) {
            assert(rowSet.next())
            assert(rowSet.getString(TABLE_CAT) === "default_catalog")
            assert(rowSet.getString(TABLE_SCHEM) === "default_database")
            assert(rowSet.getString(TABLE_NAME) === tableName)
            assert(rowSet.getString(COLUMN_NAME) === "id1")
            assert(rowSet.getInt(KEY_SEQ) === 1)
            assert(rowSet.getString(PK_NAME) === "pk_con")

            assert(rowSet.next())
            assert(rowSet.getString(TABLE_CAT) === "default_catalog")
            assert(rowSet.getString(TABLE_SCHEM) === "default_database")
            assert(rowSet.getString(TABLE_NAME) === tableName)
            assert(rowSet.getString(COLUMN_NAME) === "id2")
            assert(rowSet.getInt(KEY_SEQ) === 2)
            assert(rowSet.getString(PK_NAME) === "pk_con")
          }
        }

      }
    }
  }

  test("get type info") {
    withJdbcStatement() { statement =>
      val typeInfo = statement.getConnection.getMetaData.getTypeInfo

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.CHAR.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.CHAR)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.VARCHAR.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.VARCHAR)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.BOOLEAN.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.BOOLEAN)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.BINARY.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.BINARY)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.VARBINARY.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.VARBINARY)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.DECIMAL.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.DECIMAL)
      assert(typeInfo.getInt(PRECISION) === 38)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.TINYINT.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.TINYINT)
      assert(typeInfo.getInt(PRECISION) === 3)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.SMALLINT.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.SMALLINT)
      assert(typeInfo.getInt(PRECISION) === 5)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.INTEGER.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.INTEGER)
      assert(typeInfo.getInt(PRECISION) === 10)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.BIGINT.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.BIGINT)
      assert(typeInfo.getInt(PRECISION) === 19)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.FLOAT.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.FLOAT)
      assert(typeInfo.getInt(PRECISION) === 7)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.DOUBLE.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.DOUBLE)
      assert(typeInfo.getInt(PRECISION) === 15)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.DATE.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.DATE)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.TIME)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.TIMESTAMP)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.TIMESTAMP_WITH_TIMEZONE)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(
        typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.TIMESTAMP_WITH_TIMEZONE)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.INTERVAL_YEAR_MONTH.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.OTHER)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.INTERVAL_DAY_TIME.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.OTHER)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.ARRAY.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.ARRAY)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.MULTISET.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.JAVA_OBJECT)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.MAP.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.JAVA_OBJECT)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.ROW.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.JAVA_OBJECT)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.DISTINCT_TYPE.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.OTHER)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.STRUCTURED_TYPE.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.OTHER)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.NULL.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.NULL)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.RAW.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.OTHER)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.SYMBOL.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.OTHER)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === LogicalTypeRoot.UNRESOLVED.name())
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.OTHER)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)
    }
  }

  test("get schemas") {
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      var resultSet = metaData.getSchemas(null, null)
      val defaultCatalog = "default_catalog"
      val defaultDatabase = "default_database"
      while (resultSet.next()) {
        assert(resultSet.getString(TABLE_SCHEM) === defaultDatabase)
        assert(resultSet.getString(TABLE_CATALOG) === defaultCatalog)
      }
      resultSet = metaData.getSchemas(
        defaultCatalog.split("_").apply(0),
        defaultDatabase.split("_").apply(0))
      while (resultSet.next()) {
        assert(resultSet.getString(TABLE_SCHEM) === defaultDatabase)
        assert(resultSet.getString(TABLE_CATALOG) === defaultCatalog)
      }
    }
  }

  test("get table types") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val types = meta.getTableTypes
      val expected = Constants.SUPPORTED_TABLE_TYPES.toIterator
      while (types.next()) {
        assert(types.getString(TABLE_TYPE) === expected.next())
      }
      assert(!expected.hasNext)
      assert(!types.next())
    }
  }

  test("get tables") {
    val table = "table_1_test"
    val table_view = "table_1_test_view"

    withJdbcStatement(table) { statement =>
      statement.execute(
        s"""
           | create table $table (
           |  id int,
           |  name string,
           |  price double
           | )
           | comment 'table_comment'
           | with (
           |   'connector' = 'filesystem'
           | )
       """.stripMargin)

      statement.execute(
        s"""
           | create view ${table_view}
           | as select 1
       """.stripMargin)

      val metaData = statement.getConnection.getMetaData
      val rs1 = metaData.getTables(null, null, null, null)
      assert(rs1.next())
      assert(rs1.getString(1) == "default_catalog")
      assert(rs1.getString(2) == "default_database")
      assert(rs1.getString(3) == table)
      assert(rs1.getString(4) == "TABLE")
      assert(rs1.getString(5) == "table_comment")
      assert(rs1.next())
      assert(rs1.getString(1) == "default_catalog")
      assert(rs1.getString(2) == "default_database")
      assert(rs1.getString(3) == table_view)
      assert(rs1.getString(4) == "VIEW")
      assert(rs1.getString(5) == "")

      // get table , table name like table%
      val rs2 = metaData.getTables(null, null, "table%", Array("TABLE"))
      assert(rs2.next())
      assert(rs2.getString(1) == "default_catalog")
      assert(rs2.getString(2) == "default_database")
      assert(rs2.getString(3) == table)
      assert(!rs2.next())

      // get view , view name like *
      val rs3 = metaData.getTables(null, "default_database", "*", Array("VIEW"))
      assert(rs3.next())
      assert(rs3.getString(1) == "default_catalog")
      assert(rs3.getString(2) == "default_database")
      assert(rs3.getString(3) == table_view)

      // get view , view name like *, schema pattern like default_%
      val rs4 = metaData.getTables(null, "default_%", "*", Array("VIEW"))
      assert(rs4.next())
      assert(rs4.getString(3) == table_view)

      // get view , view name like *, schema pattern like no_exists_%
      val rs5 = metaData.getTables(null, "no_exists_%", "*", Array("VIEW"))
      assert(!rs5.next())
    }
  }

  test("get functions") {
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      Seq("currentTimestamp", "currentDate", "currentTime", "localTimestamp", "localTime")
        .foreach { func =>
          val resultSet = metaData.getFunctions(null, null, func)
          while (resultSet.next()) {
            assert(resultSet.getString(FUNCTION_CAT) === null)
            assert(resultSet.getString(FUNCTION_SCHEM) === null)
            assert(resultSet.getString(FUNCTION_NAME) === func)
            assert(resultSet.getString(REMARKS) === null)
            assert(resultSet.getInt(FUNCTION_TYPE) === DatabaseMetaData.functionResultUnknown)
            assert(resultSet.getString(SPECIFIC_NAME) === null)
          }
        }
      val expected =
        List("currentTimestamp", "currentDate", "currentTime", "localTimestamp", "localTime")
      Seq("current", "local")
        .foreach { funcPattern =>
          val resultSet = metaData.getFunctions(null, null, funcPattern)
          while (resultSet.next()) {
            assert(resultSet.getString(FUNCTION_CAT) === null)
            assert(resultSet.getString(FUNCTION_SCHEM) === null)
            assert(expected.contains(resultSet.getString(FUNCTION_NAME)))
            assert(resultSet.getString(REMARKS) === null)
            assert(resultSet.getString(FUNCTION_TYPE) === DatabaseMetaData.functionResultUnknown)
            assert(resultSet.getString(SPECIFIC_NAME) === null)
          }
        }
    }
  }

  test("execute statement - show/stop jobs") {
    if (FLINK_RUNTIME_VERSION >= "1.17") {
      // use a bigger value to ensure all tasks of the streaming query run until
      // we explicitly stop the job.
      withSessionConf()(Map(ENGINE_FLINK_MAX_ROWS.key -> "10000"))(Map.empty) {
        withMultipleConnectionJdbcStatement()({ statement =>
          statement.executeQuery(
            "create table tbl_a (a int) with (" +
              "'connector' = 'datagen', " +
              "'rows-per-second'='10')")
          statement.executeQuery("create table tbl_b (a int) with ('connector' = 'blackhole')")
          val insertResult1 = statement.executeQuery("insert into tbl_b select * from tbl_a")
          assert(insertResult1.next())
          val jobId1 = insertResult1.getString(1)

          Thread.sleep(5000)

          val showResult = statement.executeQuery("show jobs")
          val metadata = showResult.getMetaData
          assert(metadata.getColumnName(1) === "job id")
          assert(metadata.getColumnType(1) === java.sql.Types.VARCHAR)
          assert(metadata.getColumnName(2) === "job name")
          assert(metadata.getColumnType(2) === java.sql.Types.VARCHAR)
          assert(metadata.getColumnName(3) === "status")
          assert(metadata.getColumnType(3) === java.sql.Types.VARCHAR)
          assert(metadata.getColumnName(4) === "start time")
          assert(metadata.getColumnType(4) === java.sql.Types.OTHER)

          var isFound = false
          while (showResult.next()) {
            if (showResult.getString(1) === jobId1) {
              isFound = true
              assert(showResult.getString(2) === "test-job")
              assert(showResult.getString(3) === "RUNNING")
              assert(showResult.getObject(4).isInstanceOf[TimestampTZ])
            }
          }
          assert(isFound)

          val stopResult1 = statement.executeQuery(s"stop job '$jobId1'")
          assert(stopResult1.next())
          assert(stopResult1.getString(1) === "OK")

          val insertResult2 = statement.executeQuery("insert into tbl_b select * from tbl_a")
          assert(insertResult2.next())
          val jobId2 = insertResult2.getString(1)

          val stopResult2 = statement.executeQuery(s"stop job '$jobId2' with savepoint")
          assert(stopResult2.getMetaData.getColumnName(1).equals("savepoint path"))
          assert(stopResult2.next())
          assert(Paths.get(stopResult2.getString(1)).getFileName.toString.startsWith("savepoint-"))
        })
      }
    }
  }

  test("execute statement - select column name with dots") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select 'tmp.hello'")
      assert(resultSet.next())
      assert(resultSet.getString(1) === "tmp.hello")
    }
  }

  test("execute statement - select decimal") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT 1.2BD, 1.23BD ")
      assert(resultSet.next())
      assert(resultSet.getBigDecimal(1) === java.math.BigDecimal.valueOf(1.2))
      assert(resultSet.getBigDecimal(2) === java.math.BigDecimal.valueOf(1.23))
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.DECIMAL)
      assert(metaData.getColumnType(2) === java.sql.Types.DECIMAL)
      assert(metaData.getPrecision(1) == 2)
      assert(metaData.getPrecision(2) == 3)
      assert(metaData.getScale(1) == 1)
      assert(metaData.getScale(2) == 2)
    }
  }

  test("execute statement - select varchar/char") {
    withJdbcStatement() { statement =>
      val resultSet =
        statement.executeQuery("select cast('varchar10' as varchar(10)), " +
          "cast('char16' as char(16))")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.VARCHAR)
      assert(metaData.getPrecision(1) === 10)
      assert(metaData.getColumnType(2) === java.sql.Types.CHAR)
      assert(metaData.getPrecision(2) === 16)
      assert(resultSet.next())
      assert(resultSet.getString(1) === "varchar10")
      assert(resultSet.getString(2) === "char16          ")
    }
  }

  test("execute statement - select tinyint") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select cast(1 as tinyint)")
      assert(resultSet.next())
      assert(resultSet.getByte(1) === 1)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.TINYINT)
    }
  }

  test("execute statement - select smallint") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select cast(1 as smallint)")
      assert(resultSet.next())
      assert(resultSet.getShort(1) === 1)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.SMALLINT)
    }
  }

  test("execute statement - select int") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select 1")
      assert(resultSet.next())
      assert(resultSet.getInt(1) === 1)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.INTEGER)
    }
  }

  test("execute statement - select bigint") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select cast(1 as bigint)")
      assert(resultSet.next())
      assert(resultSet.getLong(1) === 1)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.BIGINT)
    }
  }

  test("execute statement - select date") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select date '2022-01-01'")
      assert(resultSet.next())
      assert(resultSet.getDate(1).toLocalDate.toString == "2022-01-01")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.DATE)
    }
  }

  test("execute statement - select timestamp") {
    withJdbcStatement() { statement =>
      val resultSet =
        statement.executeQuery(
          "select timestamp '2022-01-01 00:00:00', timestamp '2022-01-01 00:00:00.123456789'")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.TIMESTAMP)
      assert(metaData.getColumnType(2) === java.sql.Types.TIMESTAMP)
      // 9 digits for fraction of seconds
      assert(metaData.getPrecision(1) == 29)
      assert(metaData.getPrecision(2) == 29)
      assert(resultSet.next())
      assert(resultSet.getTimestamp(1).toString == "2022-01-01 00:00:00.0")
      assert(resultSet.getTimestamp(2).toString == "2022-01-01 00:00:00.123456789")
    }
  }

  test("execute statement - select timestamp with local time zone") {
    withJdbcStatement() { statement =>
      statement.executeQuery("CREATE VIEW T1 AS SELECT TO_TIMESTAMP_LTZ(4001, 3)")
      statement.executeQuery("SET 'table.local-time-zone' = 'UTC'")
      val resultSetUTC = statement.executeQuery("SELECT * FROM T1")
      val metaData = resultSetUTC.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.OTHER)
      assert(resultSetUTC.next())
      assert(resultSetUTC.getString(1) === "1970-01-01 00:00:04.001 UTC")

      statement.executeQuery("SET 'table.local-time-zone' = 'America/Los_Angeles'")
      val resultSetPST = statement.executeQuery("SELECT * FROM T1")
      assert(resultSetPST.next())
      assert(resultSetPST.getString(1) === "1969-12-31 16:00:04.001 America/Los_Angeles")
    }
  }

  test("execute statement - select time") {
    withJdbcStatement() { statement =>
      val resultSet =
        statement.executeQuery(
          "select time '00:00:03', time '00:00:05.123456789'")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.VARCHAR)
      assert(metaData.getColumnType(2) === java.sql.Types.VARCHAR)
      assert(resultSet.next())
      assert(resultSet.getString(1) == "00:00:03")
      assert(resultSet.getString(2) == "00:00:05.123")
    }
  }

  test("execute statement - select array") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select array ['v1', 'v2', 'v3']")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.ARRAY)
      assert(resultSet.next())
      val expected = "[\"v1\",\"v2\",\"v3\"]"
      assert(resultSet.getObject(1).toString == expected)
    }
  }

  test("execute statement - select map") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select map ['k1', 'v1', 'k2', 'v2']")
      assert(resultSet.next())
      assert(List("{k1=v1, k2=v2}", "{k2=v2, k1=v1}")
        .contains(resultSet.getString(1)))
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.JAVA_OBJECT)
    }
  }

  test("execute statement - select row") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select (1, '2', true)")
      assert(resultSet.next())
      val expected = """{INT NOT NULL:1,CHAR(1) NOT NULL:"2",BOOLEAN NOT NULL:true}"""
      assert(resultSet.getString(1) == expected)
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.STRUCT)
    }
  }

  test("execute statement - select binary") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select encode('kyuubi', 'UTF-8')")
      assert(resultSet.next())
      // TODO: validate table results after FLINK-28882 is resolved
      assert(resultSet.getString(1) == "k")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.BINARY)
    }
  }

  test("execute statement - select varbinary") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("select cast('kyuubi' as varbinary)")
      assert(resultSet.next())
      assert(resultSet.getString(1) == "kyuubi")
      val metaData = resultSet.getMetaData
      assert(metaData.getColumnType(1) === java.sql.Types.BINARY)
    }
  }

  test("execute statement - select float") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT cast(0.1 as float)")
      assert(resultSet.next())
      assert(resultSet.getString(1) == "0.1")
      assert(resultSet.getFloat(1) == 0.1f)
    }
  }

  test("execute statement - select count") {
    withJdbcStatement() { statement =>
      statement.execute(
        "create table tbl_src (a int) with ('connector' = 'datagen', 'number-of-rows' = '100')")
      val resultSet = statement.executeQuery(s"select count(a) from tbl_src")
      assert(resultSet.next())
      assert(resultSet.getInt(1) <= 100)
    }
  }

  test("execute statement - show functions") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("show functions")
      val metadata = resultSet.getMetaData
      assert(metadata.getColumnName(1) == "function name")
      assert(resultSet.next())
    }
  }

  test("execute statement - show databases") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("show databases")
      val metadata = resultSet.getMetaData
      assert(metadata.getColumnName(1) == "database name")
      assert(resultSet.next())
      assert(resultSet.getString(1) == "default_database")
    }
  }

  test("execute statement - show tables") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("show tables")
      val metadata = resultSet.getMetaData
      assert(metadata.getColumnName(1) == "table name")
      assert(!resultSet.next())
    }
  }

  test("execute statement - explain query") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("explain select 1")
      val metadata = resultSet.getMetaData
      assert(metadata.getColumnName(1) == "result")
      assert(resultSet.next())
    }
  }

  test("execute statement - create/drop catalog") {
    withJdbcStatement() { statement =>
      val createResult =
        statement.executeQuery("create catalog cat_a with ('type'='generic_in_memory')")
      assert(createResult.next())
      assert(createResult.getString(1) === "OK")
      val dropResult = statement.executeQuery("drop catalog cat_a")
      assert(dropResult.next())
      assert(dropResult.getString(1) === "OK")
    }
  }

  test("execute statement - set/get catalog") {
    withSessionConf()(
      Map(ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED.key -> "true"))(
      Map.empty) {
      withJdbcStatement() { statement =>
        statement.executeQuery("create catalog cat_a with ('type'='generic_in_memory')")
        val catalog = statement.getConnection.getCatalog
        assert(catalog == "default_catalog")
        statement.getConnection.setCatalog("cat_a")
        val changedCatalog = statement.getConnection.getCatalog
        assert(changedCatalog == "cat_a")
        statement.getConnection.setCatalog("default_catalog")
        assert(statement.execute("drop catalog cat_a"))
      }
    }
  }

  test("execute statement - create/alter/drop database") {
    withJdbcStatement() { statement =>
      val createResult = statement.executeQuery("create database db_a")
      assert(createResult.next())
      assert(createResult.getString(1) === "OK")
      val alterResult = statement.executeQuery("alter database db_a set ('k1' = 'v1')")
      assert(alterResult.next())
      assert(alterResult.getString(1) === "OK")
      val dropResult = statement.executeQuery("drop database db_a")
      assert(dropResult.next())
      assert(dropResult.getString(1) === "OK")
    }
  }

  test("execute statement - set/get database") {
    withSessionConf()(
      Map(ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED.key -> "true"))(
      Map.empty) {
      withJdbcStatement() { statement =>
        statement.executeQuery("create database db_a")
        val schema = statement.getConnection.getSchema
        assert(schema == "default_database")
        statement.getConnection.setSchema("db_a")
        val changedSchema = statement.getConnection.getSchema
        assert(changedSchema == "db_a")
        // reset database to default
        statement.getConnection.setSchema("default_database")
        assert(statement.execute("drop database db_a"))
      }
    }
  }

  test("execute statement - create/alter/drop table") {
    withJdbcStatement() { statement =>
      val createResult =
        statement.executeQuery("create table tbl_a (a string) with ('connector' = 'blackhole')")
      assert(createResult.next())
      assert(createResult.getString(1) === "OK")
      val alterResult = statement.executeQuery("alter table tbl_a rename to tbl_b")
      assert(alterResult.next())
      assert(alterResult.getString(1) === "OK")
      val dropResult = statement.executeQuery("drop table tbl_b")
      assert(dropResult.next())
      assert(dropResult.getString(1) === "OK")
    }
  }

  test("execute statement - create/alter/drop view") {
    withMultipleConnectionJdbcStatement() { statement =>
      val createResult = statement.executeQuery("create view view_a as select 1")
      assert(createResult.next())
      assert(createResult.getString(1) === "OK")
      val alterResult = statement.executeQuery("alter view view_a rename to view_b")
      assert(alterResult.next())
      assert(alterResult.getString(1) === "OK")
      val dropResult = statement.executeQuery("drop view view_b")
      assert(dropResult.next())
      assert(dropResult.getString(1) === "OK")
    }
  }

  test("execute statement - batch insert into") {
    withMultipleConnectionJdbcStatement() { statement =>
      statement.executeQuery("create table tbl_a (a int) with ('connector' = 'blackhole')")
      val resultSet = statement.executeQuery("insert into tbl_a select 1")
      val metadata = resultSet.getMetaData
      assert(metadata.getColumnName(1) === "job id")
      assert(metadata.getColumnType(1) === java.sql.Types.VARCHAR)
      assert(resultSet.next())
      assert(resultSet.getString(1).length == 32)
    }
  }

  test("execute statement - streaming insert into") {
    withMultipleConnectionJdbcStatement()({ statement =>
      // Flink currently doesn't support stop job statement, thus use a finite stream
      statement.executeQuery(
        "create table tbl_a (a int) with (" +
          "'connector' = 'datagen', " +
          "'rows-per-second'='10', " +
          "'number-of-rows'='100')")
      statement.executeQuery("create table tbl_b (a int) with ('connector' = 'blackhole')")
      val resultSet = statement.executeQuery("insert into tbl_b select * from tbl_a")
      val metadata = resultSet.getMetaData
      assert(metadata.getColumnName(1) === "job id")
      assert(metadata.getColumnType(1) === java.sql.Types.VARCHAR)
      assert(resultSet.next())
      val jobId = resultSet.getString(1)
      assert(jobId.length == 32)

      if (FLINK_RUNTIME_VERSION >= "1.17") {
        val stopResult = statement.executeQuery(s"stop job '$jobId'")
        assert(stopResult.next())
        assert(stopResult.getString(1) === "OK")
      }
    })
  }

  test("execute statement - set properties") {
    withMultipleConnectionJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("set table.dynamic-table-options.enabled = true")
      val metadata = resultSet.getMetaData
      assert(metadata.getColumnName(1) == "result")
      assert(resultSet.next())
      assert(resultSet.getString(1) == "OK")
    }
  }

  test("execute statement - show properties") {
    withMultipleConnectionJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("set")
      val metadata = resultSet.getMetaData
      assert(metadata.getColumnName(1) == "key")
      assert(metadata.getColumnName(2) == "value")
      assert(resultSet.next())
    }
  }

  test("execute statement - reset property") {
    val originalName = "test-job" // defined in WithFlinkTestResource
    withMultipleConnectionJdbcStatement() { statement =>
      statement.executeQuery(s"set ${PipelineOptions.NAME.key()} = wrong-name")
      statement.executeQuery(s"reset ${PipelineOptions.NAME.key()}")
      val resultSet = statement.executeQuery("set")
      // Flink does not support set key without value currently,
      // thus read all rows to find the desired one
      var success = false
      while (resultSet.next()) {
        if (resultSet.getString(1) == PipelineOptions.NAME.key() &&
          resultSet.getString(2).equals(originalName)) {
          success = true
        }
      }
      assert(success)
    }
  }

  test("execute statement - select udf") {
    withJdbcStatement() { statement =>
      statement.execute(s"create function $GENERATED_UDF_CLASS AS '$GENERATED_UDF_CLASS'")
      val resultSet = statement.executeQuery(s"select $GENERATED_UDF_CLASS('A')")
      assert(resultSet.next())
      assert(resultSet.getString(1) === "a")
    }
  }

  test("async execute statement - select column name with dots") {
    withThriftClient { client =>
      val tOpenSessionReq = new TOpenSessionReq()
      tOpenSessionReq.setUsername("kentyao")
      tOpenSessionReq.setPassword("anonymous")
      val tOpenSessionResp = client.OpenSession(tOpenSessionReq)
      val tExecuteStatementReq = new TExecuteStatementReq()
      tExecuteStatementReq.setSessionHandle(tOpenSessionResp.getSessionHandle)
      tExecuteStatementReq.setRunAsync(true)
      tExecuteStatementReq.setStatement("select 'tmp.hello'")
      val tExecuteStatementResp = client.ExecuteStatement(tExecuteStatementReq)
      val operationHandle = tExecuteStatementResp.getOperationHandle
      waitForOperationToComplete(client, operationHandle)
      val tFetchResultsReq = new TFetchResultsReq()
      tFetchResultsReq.setOperationHandle(operationHandle)
      tFetchResultsReq.setFetchType(2)
      tFetchResultsReq.setMaxRows(1000)
      val tFetchResultsResp = client.FetchResults(tFetchResultsReq)
      assert(tFetchResultsResp.getResults.getColumns.get(0)
        .getStringVal.getValues.get(0) === "tmp.hello")
    }
  }

  test("ensure result max rows") {
    withSessionConf()(Map(ENGINE_FLINK_MAX_ROWS.key -> "200"))(Map.empty) {
      withJdbcStatement() { statement =>
        statement.execute("create table tbl_src (a bigint) with (" +
          "'connector' = 'datagen', 'number-of-rows' = '1000')")
        val resultSet = statement.executeQuery(s"select a from tbl_src")
        var rows = 0
        while (resultSet.next()) {
          rows += 1
        }
        assert(rows === 200)
      }
    }
    if (FLINK_RUNTIME_VERSION >= "1.17") {
      withSessionConf()(Map(ENGINE_FLINK_MAX_ROWS.key -> "10"))(Map.empty) {
        withJdbcStatement() { statement =>
          for (i <- 0 to 10) {
            statement.execute(s"create table tbl_src$i (a bigint) " +
              s"with ('connector' = 'blackhole')")
          }
          val resultSet = statement.executeQuery("show tables")
          var rows = 0
          while (resultSet.next()) {
            rows += 1
          }
          assert(rows === 11)
        }
      }
    }
  }

  test("execute statement - add/show jar") {
    val jarName = s"newly-added-${UUID.randomUUID()}.jar"
    val newJar = TestUserClassLoaderJar.createJarFile(
      Utils.createTempDir("add-jar-test").toFile,
      jarName,
      GENERATED_UDF_CLASS,
      GENERATED_UDF_CODE).toPath

    withMultipleConnectionJdbcStatement()({ statement =>
      statement.execute(s"add jar '$newJar'")

      val showJarsResultAdded = statement.executeQuery("show jars")
      var exists = false
      while (showJarsResultAdded.next()) {
        if (showJarsResultAdded.getString(1).contains(jarName)) {
          exists = true
        }
      }
      assert(exists)
    })
  }

  // ignored because Flink gateway doesn't support remove-jar statements
  // see org.apache.flink.table.gateway.service.operation.OperationExecutor#callRemoveJar(..)
  ignore("execute statement - remove jar") {
    val jarName = s"newly-added-${UUID.randomUUID()}.jar"
    val newJar = TestUserClassLoaderJar.createJarFile(
      Utils.createTempDir("add-jar-test").toFile,
      jarName,
      GENERATED_UDF_CLASS,
      GENERATED_UDF_CODE).toPath

    withMultipleConnectionJdbcStatement()({ statement =>
      statement.execute(s"add jar '$newJar'")

      val showJarsResultAdded = statement.executeQuery("show jars")
      var exists = false
      while (showJarsResultAdded.next()) {
        if (showJarsResultAdded.getString(1).contains(jarName)) {
          exists = true
        }
      }
      assert(exists)

      statement.execute(s"remove jar '$newJar'")
      val showJarsResultRemoved = statement.executeQuery("show jars")
      exists = false
      while (showJarsResultRemoved.next()) {
        if (showJarsResultRemoved.getString(1).contains(jarName)) {
          exists = true
        }
      }
      assert(!exists)
    })
  }

  test("set session conf - default database") {
    def assertDefaultDatabase(
        client: TCLIService.Iface,
        database: String,
        expectSuccess: Boolean): Unit = {
      val req = new TOpenSessionReq()
      req.setUsername("kyuubi")
      req.setPassword("anonymous")
      val conf = Map("use:database" -> database)
      req.setConfiguration(conf.asJava)
      val tOpenSessionResp = client.OpenSession(req)
      val status = tOpenSessionResp.getStatus
      if (expectSuccess) {
        assert(status.getStatusCode === TStatusCode.SUCCESS_STATUS)
      } else {
        assert(status.getStatusCode === TStatusCode.ERROR_STATUS)
        assert(status.getErrorMessage.contains(
          s"A database with name [$database] does not exist"))
      }
    }

    withThriftClient { client =>
      assertDefaultDatabase(client, "default", true)
    }
    withThriftClient { client =>
      assertDefaultDatabase(client, "default2", false)
    }
    withThriftClient { client =>
      assertDefaultDatabase(client, "default_database", true)
    }
  }

  test("get query id") {
    withJdbcStatement("tbl_a") { stmt =>
      stmt.executeQuery("create table tbl_a (a int) with ('connector' = 'blackhole')")
      assert(stmt.asInstanceOf[KyuubiStatement].getQueryId === null)
      stmt.executeQuery("insert into tbl_a values (1)")
      val queryId = stmt.asInstanceOf[KyuubiStatement].getQueryId
      assert(queryId !== null)
      // parse the string to check if it's valid Flink job id
      assert(JobID.fromHexString(queryId) !== null)
    }
  }

  test("test result fetch timeout") {
    val exception = intercept[KyuubiSQLException](
      withSessionConf()(Map(ENGINE_FLINK_FETCH_TIMEOUT.key -> "PT60S"))() {
        withJdbcStatement("tbl_a") { stmt =>
          stmt.executeQuery("create table tbl_a (a int) " +
            "with ('connector' = 'datagen', 'rows-per-second'='0')")
          val resultSet = stmt.executeQuery("select * from tbl_a")
          while (resultSet.next()) {}
        }
      })
    assert(exception.getMessage === "Futures timed out after [60000 milliseconds]")
  }

  test("execute statement - help") {
    withJdbcStatement() { stmt =>
      val resultSet = stmt.executeQuery("help")
      val metadata = resultSet.getMetaData
      assert(metadata.getColumnName(1) === "result")
      assert(resultSet.next())
      assert(resultSet.getString(1).equals(CommandStrings.MESSAGE_HELP.toString))
    }
  }
}
