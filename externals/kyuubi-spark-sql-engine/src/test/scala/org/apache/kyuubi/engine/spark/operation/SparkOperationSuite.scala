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

import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, DecimalType, DoubleType, FloatType, IntegerType, MapType, NumericType, StringType, StructType, TimestampType}

import org.apache.kyuubi.engine.spark.WithSparkSQLEngine

class SparkOperationSuite extends WithSparkSQLEngine {
  private val currentCatalog = spark.sessionState.catalogManager.currentCatalog

  test("get catalogs") {
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      val catalogs = metaData.getCatalogs
      catalogs.next()
      assert(catalogs.getString("TABLE_CAT") === currentCatalog.name())
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
        assert(resultSet.getString("TABLE_SCHEM") === expected.next)
        assert(resultSet.getString("TABLE_CATALOG").isEmpty)
      }
    }
  }

  test("get columns operation") {
    val schemaName = "default"
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
         |CREATE TABLE IF NOT EXISTS $schemaName.$tableName (
         |  ${schema.toDDL}
         |)
         |using parquet""".stripMargin

    withJdbcStatement(tableName) { statement =>
      statement.execute(ddl)

      val databaseMetaData = statement.getConnection.getMetaData
      val rowSet = databaseMetaData.getColumns("", schemaName, tableName, null)

      import java.sql.Types._
      val expectedJavaTypes = Seq(BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE,
        DECIMAL, DECIMAL, VARCHAR, ARRAY, ARRAY, JAVA_OBJECT, DATE, TIMESTAMP, STRUCT, BINARY)

      var pos = 0

      while (rowSet.next()) {
        assert(rowSet.getString("TABLE_CAT") === null)
        assert(rowSet.getString("TABLE_SCHEM") === schemaName)
        assert(rowSet.getString("TABLE_NAME") === tableName)
        assert(rowSet.getString("COLUMN_NAME") === schema(pos).name)
        assert(rowSet.getInt("DATA_TYPE") === expectedJavaTypes(pos))
        assert(rowSet.getString("TYPE_NAME") === schema(pos).dataType.sql)

        val colSize = rowSet.getInt("COLUMN_SIZE")
        schema(pos).dataType match {
          case StringType | BinaryType | _: ArrayType | _: MapType => assert(colSize === 0)
          case o => assert(colSize === o.defaultSize)
        }

        assert(rowSet.getInt("BUFFER_LENGTH") === 0) // not used
        val decimalDigits = rowSet.getInt("DECIMAL_DIGITS")
        schema(pos).dataType match {
          case BooleanType | _: IntegerType => assert(decimalDigits === 0)
          case d: DecimalType => assert(decimalDigits === d.scale)
          case FloatType => assert(decimalDigits === 7)
          case DoubleType => assert(decimalDigits === 15)
          case TimestampType => assert(decimalDigits === 6)
          case _ => assert(decimalDigits === 0) // nulls
        }

        val radix = rowSet.getInt("NUM_PREC_RADIX")
        schema(pos).dataType match {
          case _: NumericType => assert(radix === 10)
          case _ => assert(radix === 0) // nulls
        }

        assert(rowSet.getInt("NULLABLE") === 1)
        assert(rowSet.getString("REMARKS") === pos.toString)
        assert(rowSet.getInt("ORDINAL_POSITION") === pos)
        assert(rowSet.getString("IS_NULLABLE") === "YES")
        assert(rowSet.getString("IS_AUTO_INCREMENT") === "NO")
        pos += 1
      }

      assert(pos === 17, "all columns should have been verified")
    }
  }

}
