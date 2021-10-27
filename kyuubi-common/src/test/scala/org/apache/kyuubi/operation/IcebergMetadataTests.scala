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

package org.apache.kyuubi.operation

import org.apache.kyuubi.IcebergSuiteMixin
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._

trait IcebergMetadataTests extends HiveJDBCTestHelper with IcebergSuiteMixin {

  test("get catalogs") {
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      val catalogs = metaData.getCatalogs
      catalogs.next()
      assert(catalogs.getString(TABLE_CAT) === "spark_catalog")
      catalogs.next()
      assert(catalogs.getString(TABLE_CAT) === catalog)
    }
  }

  test("get schemas") {
    val dbs = Seq("db1", "db2", "db33", "db44")
    val dbDflts = Seq("default", "global_temp")

    withDatabases(dbs: _*) { statement =>
      dbs.foreach(db => statement.execute(s"CREATE NAMESPACE IF NOT EXISTS $db"))
      val metaData = statement.getConnection.getMetaData


      // The session catalog
      matchAllPatterns foreach { pattern =>
        checkGetSchemas(metaData.getSchemas("spark_catalog", pattern), dbDflts, "spark_catalog")
      }

      Seq(null, catalog).foreach { cg =>
        matchAllPatterns foreach { pattern =>
          checkGetSchemas(
            metaData.getSchemas(cg, pattern), dbs ++ Seq("global_temp"), catalog)
        }
      }

      Seq("db%", "db.*") foreach { pattern =>
        checkGetSchemas(metaData.getSchemas(catalog, pattern), dbs, catalog)
      }

      Seq("db_", "db.") foreach { pattern =>
        checkGetSchemas(metaData.getSchemas(catalog, pattern), dbs.take(2), catalog)
      }

      checkGetSchemas(metaData.getSchemas(catalog, "db1"), Seq("db1"), catalog)
      checkGetSchemas(metaData.getSchemas(catalog, "db_not_exist"), Seq.empty, catalog)
    }
  }

  test("get schemas with multipart namespaces") {
    val dbs = Seq(
      "`a.b`",
      "`a.b`.c",
      "a.`b.c`",
      "`a.b.c`",
      "`a.b``.c`",
      "db1.db2.db3",
      "db4")

    withDatabases(dbs: _*) { statement =>
      dbs.foreach(db => statement.execute(s"CREATE NAMESPACE IF NOT EXISTS $db"))
      val metaData = statement.getConnection.getMetaData

      Seq(null, catalog).foreach { cg =>
        matchAllPatterns foreach { pattern =>
          checkGetSchemas(metaData.getSchemas(cg, pattern),
            dbs ++ Seq("global_temp", "a", "db1", "db1.db2"), catalog)
        }
      }

      checkGetSchemas(metaData.getSchemas(catalog, "db1.db2%"),
        Seq("db1.db2", "db1.db2.db3"), catalog)
    }
  }

  test("get tables") {
    val dbs = Seq(
      "`a.b`",
      "`a.b`.c",
      "a.`b.c`",
      "`a.b.c`",
      "`a.b``.c`",
      "db1.db2.db3",
      "db4")
    withDatabases(dbs: _*) { statement =>
      dbs.foreach(db => statement.execute(s"CREATE NAMESPACE IF NOT EXISTS $db"))
      val metaData = statement.getConnection.getMetaData

      Seq(catalog).foreach { cg =>
        dbs.foreach { db =>
          try {
            statement.execute(
              s"CREATE TABLE IF NOT EXISTS $cg.$db.tbl(c STRING) USING iceberg")

            val rs1 = metaData.getTables(cg, db, "%", null)
            while (rs1.next()) {
              val catalogName = rs1.getString(TABLE_CAT)
              assert(catalogName === cg)
              assert(rs1.getString(TABLE_SCHEM) === db)
              assert(rs1.getString(TABLE_NAME) === "tbl")
              assert(rs1.getString(TABLE_TYPE) == "TABLE")
              assert(rs1.getString(REMARKS) === "")
            }
            assert(!rs1.next())
          } finally {
            statement.execute(s"DROP TABLE IF EXISTS $cg.$db.tbl")
          }
        }
      }
    }

  }

  test("get columns operation") {
    val dataTypes = Seq("boolean", "int", "bigint",
      "float", "double", "decimal(38,20)", "decimal(10,2)",
      "string", "array<bigint>", "array<string>", "map<int, bigint>",
      "date", "timestamp", "struct<`X`: bigint, `Y`: double>", "binary", "struct<`X`: string>")
    val cols = dataTypes.zipWithIndex.map { case (dt, idx) => s"c$idx" -> dt }
    val (colNames, _) = cols.unzip

    val tableName = "iceberg_get_col_operation"

    val ddl =
      s"""
         |CREATE TABLE IF NOT EXISTS $catalog.$defaultSchema.$tableName (
         |  ${cols.map { case (cn, dt) => cn + " " + dt }.mkString(",\n")}
         |)
         |USING iceberg""".stripMargin


    withJdbcStatement(tableName) { statement =>
      statement.execute(ddl)

      val metaData = statement.getConnection.getMetaData

      Seq("%", null, ".*", "c.*") foreach { columnPattern =>
        val rowSet = metaData.getColumns(catalog, defaultSchema, tableName, columnPattern)

        import java.sql.Types._
        val expectedJavaTypes = Seq(BOOLEAN, INTEGER, BIGINT, FLOAT, DOUBLE,
          DECIMAL, DECIMAL, VARCHAR, ARRAY, ARRAY, JAVA_OBJECT, DATE, TIMESTAMP, STRUCT, BINARY,
          STRUCT)

        var pos = 0

        while (rowSet.next()) {
          assert(rowSet.getString(TABLE_CAT) === catalog)
          assert(rowSet.getString(TABLE_SCHEM) === defaultSchema)
          assert(rowSet.getString(TABLE_NAME) === tableName)
          assert(rowSet.getString(COLUMN_NAME) === colNames(pos))
          assert(rowSet.getInt(DATA_TYPE) === expectedJavaTypes(pos))
          assert(rowSet.getString(TYPE_NAME) equalsIgnoreCase dataTypes(pos))
          pos += 1
        }

        assert(pos === dataTypes.size, "all columns should have been verified")
      }

      val rowSet = metaData.getColumns(catalog, "*", "not_exist", "not_exist")
      assert(!rowSet.next())
    }
  }
}
