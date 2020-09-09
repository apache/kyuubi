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

package org.apache.kyuubi.engine.spark.operation

import org.apache.hive.service.rpc.thrift.TOpenSessionReq
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.types._

import org.apache.kyuubi.Utils
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._

class SparkOperationSuite extends WithSparkSQLEngine {
  private val currentCatalog = spark.sessionState.catalogManager.currentCatalog
  private val dftSchema = "default"

  test("get catalogs") {
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      val catalogs = metaData.getCatalogs
      catalogs.next()
      assert(catalogs.getString(TABLE_CAT) === currentCatalog.name())
      assert(!catalogs.next())
    }
  }

  test("get schemas") {
    withDatabases("db1", "db2") { statement =>
      statement.execute("CREATE DATABASE IF NOT EXISTS db1")
      statement.execute("CREATE DATABASE IF NOT EXISTS db2")
      val metaData = statement.getConnection.getMetaData
      val resultSet = metaData.getSchemas(null, null)
      val expected =
        Seq("db1", "db2", "default", spark.sharedState.globalTempViewManager.database).iterator
      while(resultSet.next()) {
        assert(resultSet.getString(TABLE_SCHEM) === expected.next)
        assert(resultSet.getString(TABLE_CATALOG).isEmpty)
      }
    }
  }

  test("get tables") {
    val table_test = "table_1_test"
    val table_external_test = "table_2_test"
    val view_test = "view_1_test"
    val view_global_test = "view_2_test"
    val tables = Seq(table_test, table_external_test, view_test, view_global_test)
    val schemas = Seq("default", "default", "default", "global_temp")
    val tableTypes = Seq("MANAGED", "EXTERNAL", "VIEW", "VIEW")
    withJdbcStatement(view_test, view_global_test, table_test, view_test) { statement =>
      statement.execute(
        s"CREATE TABLE IF NOT EXISTS $table_test(key int) USING parquet COMMENT '$table_test'")
      val loc = Utils.createTempDir()
      statement.execute(s"CREATE EXTERNAL TABLE IF NOT EXISTS $table_external_test(key int)" +
        s" COMMENT '$table_external_test' LOCATION '$loc'")
      statement.execute(s"CREATE VIEW IF NOT EXISTS $view_test COMMENT '$view_test'" +
        s" AS SELECT * FROM $table_test")
      statement.execute(s"CREATE GLOBAL TEMP VIEW $view_global_test" +
        s" COMMENT '$view_global_test' AS SELECT * FROM $table_test")

      val metaData = statement.getConnection.getMetaData
      val rs1 = metaData.getTables(null, null, null, null)
      var i = 0
      while(rs1.next()) {
        assert(rs1.getString(TABLE_CAT).isEmpty)
        assert(rs1.getString(TABLE_SCHEM) === schemas(i))
        assert(rs1.getString(TABLE_NAME) == tables(i))
        assert(rs1.getString(TABLE_TYPE) == tableTypes(i))
        assert(rs1.getString(REMARKS) === tables(i).replace(view_global_test, ""))
        i += 1
      }
      assert(i === 4)

      val rs2 = metaData.getTables(null, null, null, Array("VIEW"))
      i = 2
      while(rs2.next()) {
        assert(rs2.getString(TABLE_NAME) == tables(i))
        i += 1
      }
      assert(i === 4)

      val rs3 = metaData.getTables(null, null, "table%", Array("VIEW"))
      assert(!rs3.next())

      val rs4 = metaData.getTables(null, null, "table%", Array("TABLE"))
      i = 0
      while(rs4.next()) {
        assert(rs4.getString(TABLE_NAME) == tables(i))
        i += 1
      }
      assert(i === 2)

      val rs5 = metaData.getTables(null, "default", "%", Array("VIEW"))
      i = 2
      while(rs5.next()) {
        assert(rs5.getString(TABLE_NAME) == view_test)
      }
    }
  }

  test("get table types") {
    withJdbcStatement() { statement =>
      val meta = statement.getConnection.getMetaData
      val types = meta.getTableTypes
      val expected = CatalogTableType.tableTypes.toIterator
      while (types.next()) {
        assert(types.getString(TABLE_TYPE) === expected.next().name)
      }
      assert(!expected.hasNext)
      assert(!types.next())
    }
  }

  test("get type info") {
    withJdbcStatement() { statement =>
      val typeInfo = statement.getConnection.getMetaData.getTypeInfo
      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "VOID")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.NULL)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "BOOLEAN")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.BOOLEAN)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "TINYINT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.TINYINT)
      assert(typeInfo.getInt(PRECISION) === 3)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "SMALLINT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.SMALLINT)
      assert(typeInfo.getInt(PRECISION) === 5)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "INTEGER")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.INTEGER)
      assert(typeInfo.getInt(PRECISION) === 10)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "BIGINT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.BIGINT)
      assert(typeInfo.getInt(PRECISION) === 19)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "FLOAT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.FLOAT)
      assert(typeInfo.getInt(PRECISION) === 7)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "DOUBLE")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.DOUBLE)
      assert(typeInfo.getInt(PRECISION) === 15)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "VARCHAR")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.VARCHAR)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "BINARY")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.BINARY)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "DECIMAL")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.DECIMAL)
      assert(typeInfo.getInt(PRECISION) === 38)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 10)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "DATE")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.DATE)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "TIMESTAMP")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.TIMESTAMP)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 3)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "ARRAY")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.ARRAY)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "MAP")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.JAVA_OBJECT)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "STRUCT")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.STRUCT)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)

      typeInfo.next()
      assert(typeInfo.getString(TYPE_NAME) === "INTERVAL")
      assert(typeInfo.getInt(DATA_TYPE) === java.sql.Types.OTHER)
      assert(typeInfo.getInt(PRECISION) === 0)
      assert(typeInfo.getShort(NULLABLE) === 1)
      assert(!typeInfo.getBoolean(CASE_SENSITIVE))
      assert(typeInfo.getShort(SEARCHABLE) === 0)
      assert(typeInfo.getInt(NUM_PREC_RADIX) === 0)
    }
  }

  test("get columns operation") {
    val tableName = "spark_get_col_operation"
    val schema = new StructType()
      .add("c0", "boolean", nullable = false, "0")
      .add("c1", "tinyint", nullable = true, "1")
      .add("c2", "smallint", nullable = false, "2")
      .add("c3", "int", nullable = true, "3")
      .add("c4", "long", nullable = false, "4")
      .add("c5", "float", nullable = true, "5")
      .add("c6", "double", nullable = false, "6")
      .add("c7", "decimal(38, 20)", nullable = true, "7")
      .add("c8", "decimal(10, 2)", nullable = false, "8")
      .add("c9", "string", nullable = true, "9")
      .add("c10", "array<long>", nullable = false, "10")
      .add("c11", "array<string>", nullable = true, "11")
      .add("c12", "map<smallint, tinyint>", nullable = false, "12")
      .add("c13", "date", nullable = true, "13")
      .add("c14", "timestamp", nullable = false, "14")
      .add("c15", "struct<X: bigint,Y: double>", nullable = true, "15")
      .add("c16", "binary", nullable = false, "16")

    val ddl =
      s"""
         |CREATE TABLE IF NOT EXISTS $dftSchema.$tableName (
         |  ${schema.toDDL}
         |)
         |using parquet""".stripMargin

    withJdbcStatement(tableName) { statement =>
      statement.execute(ddl)

      val databaseMetaData = statement.getConnection.getMetaData
      val rowSet = databaseMetaData.getColumns("", dftSchema, tableName, null)

      import java.sql.Types._
      val expectedJavaTypes = Seq(BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE,
        DECIMAL, DECIMAL, VARCHAR, ARRAY, ARRAY, JAVA_OBJECT, DATE, TIMESTAMP, STRUCT, BINARY)

      var pos = 0

      while (rowSet.next()) {
        assert(rowSet.getString(TABLE_CAT) === null)
        assert(rowSet.getString(TABLE_SCHEM) === dftSchema)
        assert(rowSet.getString(TABLE_NAME) === tableName)
        assert(rowSet.getString(COLUMN_NAME) === schema(pos).name)
        assert(rowSet.getInt(DATA_TYPE) === expectedJavaTypes(pos))
        assert(rowSet.getString(TYPE_NAME) === schema(pos).dataType.sql)

        val colSize = rowSet.getInt(COLUMN_SIZE)
        schema(pos).dataType match {
          case StringType | BinaryType | _: ArrayType | _: MapType => assert(colSize === 0)
          case o => assert(colSize === o.defaultSize)
        }

        assert(rowSet.getInt(BUFFER_LENGTH) === 0) // not used
        val decimalDigits = rowSet.getInt(DECIMAL_DIGITS)
        schema(pos).dataType match {
          case BooleanType | _: IntegerType => assert(decimalDigits === 0)
          case d: DecimalType => assert(decimalDigits === d.scale)
          case FloatType => assert(decimalDigits === 7)
          case DoubleType => assert(decimalDigits === 15)
          case TimestampType => assert(decimalDigits === 6)
          case _ => assert(decimalDigits === 0) // nulls
        }

        val radix = rowSet.getInt(NUM_PREC_RADIX)
        schema(pos).dataType match {
          case _: NumericType => assert(radix === 10)
          case _ => assert(radix === 0) // nulls
        }

        assert(rowSet.getInt(NULLABLE) === 1)
        assert(rowSet.getString(REMARKS) === pos.toString)
        assert(rowSet.getInt(ORDINAL_POSITION) === pos)
        assert(rowSet.getString(IS_NULLABLE) === "YES")
        assert(rowSet.getString(IS_AUTO_INCREMENT) === "NO")
        pos += 1
      }

      assert(pos === 17, "all columns should have been verified")
    }
  }



  test("get columns operation should handle interval column properly") {
    val viewName = "view_interval"
    val ddl = s"CREATE GLOBAL TEMP VIEW $viewName as select interval 1 day as i"

    withJdbcStatement(viewName) { statement =>
      statement.execute(ddl)
      val data = statement.getConnection.getMetaData
      val rowSet = data.getColumns("", "global_temp", viewName, null)
      while (rowSet.next()) {
        assert(rowSet.getString(TABLE_CAT) === null)
        assert(rowSet.getString(TABLE_SCHEM) === "global_temp")
        assert(rowSet.getString(TABLE_NAME) === viewName)
        assert(rowSet.getString(COLUMN_NAME) === "i")
        assert(rowSet.getInt(DATA_TYPE) === java.sql.Types.OTHER)
        assert(rowSet.getString(TYPE_NAME).equalsIgnoreCase(CalendarIntervalType.sql))
        assert(rowSet.getInt(COLUMN_SIZE) === CalendarIntervalType.defaultSize)
        assert(rowSet.getInt(DECIMAL_DIGITS) === 0)
        assert(rowSet.getInt(NUM_PREC_RADIX) === 0)
        assert(rowSet.getInt(NULLABLE) === 0)
        assert(rowSet.getString(REMARKS) === "")
        assert(rowSet.getInt(ORDINAL_POSITION) === 0)
        assert(rowSet.getString(IS_NULLABLE) === "YES")
        assert(rowSet.getString(IS_AUTO_INCREMENT) === "NO")
      }
    }
  }

  test("handling null in view for get columns operations") {
    val viewName = "view_null"
    val ddl = s"CREATE GLOBAL TEMP VIEW $viewName as select null as n"

    withJdbcStatement(viewName) { statement =>
      statement.execute(ddl)
      val data = statement.getConnection.getMetaData
      val rowSet = data.getColumns("", "global_temp", viewName, "n")
      while (rowSet.next()) {
        assert(rowSet.getString("TABLE_CAT") === null)
        assert(rowSet.getString("TABLE_SCHEM") === "global_temp")
        assert(rowSet.getString("TABLE_NAME") === viewName)
        assert(rowSet.getString("COLUMN_NAME") === "n")
        assert(rowSet.getInt("DATA_TYPE") === java.sql.Types.NULL)
        assert(rowSet.getString("TYPE_NAME").equalsIgnoreCase(NullType.sql))
        assert(rowSet.getInt("COLUMN_SIZE") === 1)
        assert(rowSet.getInt("DECIMAL_DIGITS") === 0)
        assert(rowSet.getInt("NUM_PREC_RADIX") === 0)
        assert(rowSet.getInt("NULLABLE") === 1)
        assert(rowSet.getString("REMARKS") === "")
        assert(rowSet.getInt("ORDINAL_POSITION") === 0)
        assert(rowSet.getString("IS_NULLABLE") === "YES")
        assert(rowSet.getString("IS_AUTO_INCREMENT") === "NO")
      }
    }
  }

  test("get functions") {
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      Seq("to_timestamp", "date_part", "lpad", "date_format", "cos", "sin").foreach { func =>
        val resultSet = metaData.getFunctions("", dftSchema, func)
        while (resultSet.next()) {
          val exprInfo = FunctionRegistry.expressions(func)._1
          assert(resultSet.getString(FUNCTION_CAT).isEmpty)
          assert(resultSet.getString(FUNCTION_SCHEM) === null)
          assert(resultSet.getString(FUNCTION_NAME) === exprInfo.getName)
          assert(resultSet.getString(REMARKS) ===
            s"Usage: ${exprInfo.getUsage}\nExtended Usage:${exprInfo.getExtended}")
          assert(resultSet.getString(SPECIFIC_NAME) === exprInfo.getClassName)
        }
      }
    }
  }

  test("get functions operation") {
    withThriftClient { client =>
      val req = new TOpenSessionReq()
      req.setUsername("kentyao")
      req.setPassword("anonymous")
//      req.setClient_protocol(BackendService.SERVER_VERSION)
      req
      val resp = client.OpenSession(req)
      val sessionHandle = resp.getSessionHandle
    }

  }

}
