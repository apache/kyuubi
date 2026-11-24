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

import scala.collection.mutable.ListBuffer

import org.apache.kyuubi.HMSIcebergSuiteMixin
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._
import org.apache.kyuubi.util.AssertionUtils._
import org.apache.kyuubi.util.SparkVersionUtil

trait IcebergMetadataTests extends HiveJDBCTestHelper with SparkVersionUtil {
  self: HMSIcebergSuiteMixin =>

  test("get catalogs") {
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      val catalogs = metaData.getCatalogs
      val results = ListBuffer[String]()
      while (catalogs.next()) {
        results += catalogs.getString(TABLE_CAT)
      }
      assertContains(results, "spark_catalog", catalog)
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
          checkGetSchemas(metaData.getSchemas(cg, pattern), dbs, catalog)
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
      "db1.db2.db3",
      "db4") ++ (if (SPARK_ENGINE_RUNTIME_VERSION < "4.0") Seq("`a.b``.c`") else Nil)

    withDatabases(dbs: _*) { statement =>
      dbs.foreach(db => statement.execute(s"CREATE NAMESPACE IF NOT EXISTS $db"))
      val metaData = statement.getConnection.getMetaData

      Seq(null, catalog).foreach { cg =>
        matchAllPatterns foreach { pattern =>
          checkGetSchemas(
            metaData.getSchemas(cg, pattern),
            dbs ++ Seq("a", "db1", "db1.db2"),
            catalog)
        }
      }

      checkGetSchemas(
        metaData.getSchemas(catalog, "db1.db2%"),
        Seq("db1.db2", "db1.db2.db3"),
        catalog)
    }
  }

  test("get tables") {
    val dbs = Seq(
      "`a.b`",
      "`a.b`.c",
      "a.`b.c`",
      "`a.b.c`",
      "db1.db2.db3",
      "db4") ++ (if (SPARK_ENGINE_RUNTIME_VERSION < "4.0") Seq("`a.b``.c`") else Nil)
    withDatabases(dbs: _*) { statement =>
      Seq("spark_catalog", catalog).foreach { cg =>
        // `spark_catalog` is a hive-backed session catalog with single-level namespaces, while
        // `catalog` (hadoop_prod) is a hadoop-backed v2 catalog supporting dotted namespaces.
        val namespaces = if (cg == catalog) dbs else dbs.filterNot(_.contains("."))
        val metaData = statement.getConnection.getMetaData
        namespaces.foreach { db =>
          statement.execute(s"CREATE NAMESPACE IF NOT EXISTS $cg.$db")
          try {
            statement.execute(
              s"CREATE TABLE IF NOT EXISTS $cg.$db.tbl(c STRING) USING iceberg")

            val rs1 = metaData.getTables(cg, db, "%", null)
            assert(rs1.next())
            val catalogName = rs1.getString(TABLE_CAT)
            assert(catalogName === cg)
            assert(rs1.getString(TABLE_SCHEM) === db)
            assert(rs1.getString(TABLE_NAME) === "tbl")
            assert(rs1.getString(TABLE_TYPE) == "TABLE")
            assert(rs1.getString(REMARKS) === "")
            assert(!rs1.next())
          } finally {
            statement.execute(s"DROP TABLE IF EXISTS $cg.$db.tbl PURGE")
            statement.execute(s"DROP NAMESPACE IF EXISTS $cg.$db")
          }
        }
      }
    }
  }

  test("get tables with ignoreTableProperties") {
    val cg = "spark_catalog"
    val table = "ignore_props_table"
    val view = "ignore_props_view"
    val schema = "ignore_db"
    val comment = "tbl_comment"
    withSessionConf()(
      Map(KyuubiConf.OPERATION_GET_TABLES_IGNORE_TABLE_PROPERTIES.key -> "true"))() {
      withDatabases(schema) { statement =>
        statement.execute(s"CREATE NAMESPACE IF NOT EXISTS $cg.$schema")
        try {
          statement.execute(
            s"CREATE TABLE IF NOT EXISTS $cg.$schema.$table(c STRING) " +
              s"USING iceberg COMMENT '$comment'")
          statement.execute(s"CREATE VIEW $cg.$schema.$view AS SELECT 1 AS a")

          // When ignoreTableProperties is true, skips the view probe and table property loading.
          val metaData = statement.getConnection.getMetaData
          val rsAll = metaData.getTables(cg, schema, "%", null)
          val typeByName = scala.collection.mutable.Map.empty[String, String]
          val remarkByName = scala.collection.mutable.Map.empty[String, String]
          while (rsAll.next()) {
            val name = rsAll.getString(TABLE_NAME)
            if (name == table || name == view) {
              typeByName += name -> rsAll.getString(TABLE_TYPE)
              remarkByName += name -> rsAll.getString(REMARKS)
              assert(rsAll.getString(TABLE_SCHEM) === schema)
              assert(rsAll.getString(TABLE_CAT) === cg)
            }
          }
          assert(typeByName(table) == "TABLE")
          assert(typeByName(view) == "TABLE")
          assert(remarkByName(table) == "")
          assert(remarkByName(view) == "")

          // The session catalog (Iceberg 1.11.0) does not implement listTableSummaries,
          // so tableTypes is not honored: a VIEW query still returns all entries as TABLE.
          val rsView = metaData.getTables(cg, schema, "%", Array("VIEW"))
          val viewNames = scala.collection.mutable.Set.empty[String]
          while (rsView.next()) {
            viewNames += rsView.getString(TABLE_NAME)
          }
          assert(viewNames.contains(table))
          assert(viewNames.contains(view))
        } finally {
          statement.execute(s"DROP VIEW IF EXISTS $cg.$schema.$view")
          statement.execute(s"DROP TABLE IF EXISTS $cg.$schema.$table PURGE")
          statement.execute(s"DROP NAMESPACE IF EXISTS $cg.$schema")
        }
      }
    }
  }

  test("get tables and views") {
    val cg = "spark_catalog"
    val table = "table_type"
    val view = "view_type"
    val schema = "db_type"
    val comment = "tbl_comment"
    withDatabases(schema) { statement =>
      statement.execute(s"CREATE NAMESPACE IF NOT EXISTS $cg.$schema")
      try {
        statement.execute(
          s"CREATE TABLE IF NOT EXISTS $cg.$schema.$table(c STRING) " +
            s"USING iceberg COMMENT '$comment'")
        statement.execute(s"CREATE VIEW $cg.$schema.$view AS SELECT 1 AS a")

        val metaData = statement.getConnection.getMetaData

        // Without a tableTypes filter, both table and view are returned with their true types.
        val rsAll = metaData.getTables(cg, schema, "%", null)
        val typeByName = scala.collection.mutable.Map.empty[String, String]
        val remarkByName = scala.collection.mutable.Map.empty[String, String]
        while (rsAll.next()) {
          val name = rsAll.getString(TABLE_NAME)
          if (name == table || name == view) {
            typeByName += name -> rsAll.getString(TABLE_TYPE)
            remarkByName += name -> rsAll.getString(REMARKS)
            assert(rsAll.getString(TABLE_SCHEM) === schema)
            assert(rsAll.getString(TABLE_CAT) === cg)
          }
        }
        assert(typeByName(table) == "TABLE")
        assert(typeByName(view) == "VIEW")
        assert(remarkByName(table) == comment)
        assert(remarkByName(view) == "")

        // tableTypes = TABLE returns only table.
        val rsTable = metaData.getTables(cg, schema, "%", Array("TABLE"))
        val tableNames = scala.collection.mutable.Set.empty[String]
        while (rsTable.next()) {
          tableNames += rsTable.getString(TABLE_NAME)
        }
        assert(tableNames.contains(table))
        assert(!tableNames.contains(view))

        // tableTypes = VIEW returns only view.
        val rsView = metaData.getTables(cg, schema, "%", Array("VIEW"))
        val viewNames = scala.collection.mutable.Set.empty[String]
        while (rsView.next()) {
          viewNames += rsView.getString(TABLE_NAME)
        }
        assert(viewNames.contains(view))
        assert(!viewNames.contains(table))

        // Session catalog pattern matching in getTables is case-insensitive.
        val rsUpper = metaData.getTables(cg, "DB_TYPE", "TABLE%", null)
        assert(rsUpper.next())
        assert(rsUpper.getString(TABLE_SCHEM) === schema)
        assert(rsUpper.getString(TABLE_NAME) === table)
        assert(!rsUpper.next())

        // Session catalog pattern matching in getColumns is case-insensitive.
        val rsUpperCols = metaData.getColumns(cg, "DB_TYPE", "TABLE%", "%")
        assert(rsUpperCols.next())
        assert(rsUpperCols.getString(TABLE_SCHEM) === schema)
        assert(rsUpperCols.getString(TABLE_NAME) === table)
        assert(!rsUpperCols.next())
      } finally {
        statement.execute(s"DROP VIEW IF EXISTS $cg.$schema.$view")
        statement.execute(s"DROP TABLE IF EXISTS $cg.$schema.$table PURGE")
        statement.execute(s"DROP NAMESPACE IF EXISTS $cg.$schema")
      }
    }
  }

  test("get tables types views on the fallback with wildcard patterns") {
    val cg = "spark_catalog"
    val schema = "db_fallback"
    val view = "v"
    val table = "v_ice"
    withDatabases(schema) { statement =>
      statement.execute(s"CREATE NAMESPACE IF NOT EXISTS $cg.$schema")
      try {
        statement.execute(s"CREATE VIEW IF NOT EXISTS $cg.$schema.$view AS SELECT 1 AS a")
        // HMSIcebergSuiteMixin sets spark.hadoop.iceberg.engine.hive.enabled=true, so the
        // table registers with the Hive storage handler that the Iceberg runtime does not
        // ship: the batched v1 lookup fails and the entries are typed via the view probe.
        statement.execute(
          s"CREATE TABLE IF NOT EXISTS $cg.$schema.$table(c STRING) USING iceberg")

        val metaData = statement.getConnection.getMetaData

        Seq("v", "v%", "v.*") foreach { pattern =>
          val rsView = metaData.getTables(cg, schema, pattern, Array("VIEW"))
          assert(rsView.next())
          assert(rsView.getString(TABLE_SCHEM) === schema)
          assert(rsView.getString(TABLE_NAME) === view)
          assert(rsView.getString(TABLE_TYPE) === "VIEW")
          assert(!rsView.next())
        }

        val rsAll = metaData.getTables(cg, schema, "v%", null)
        val typeByName = scala.collection.mutable.Map.empty[String, String]
        while (rsAll.next()) {
          typeByName += rsAll.getString(TABLE_NAME) -> rsAll.getString(TABLE_TYPE)
        }
        assert(typeByName(view) == "VIEW")
        assert(typeByName(table) == "TABLE")
      } finally {
        statement.execute(s"DROP VIEW IF EXISTS $cg.$schema.$view")
        statement.execute(s"DROP TABLE IF EXISTS $cg.$schema.$table PURGE")
        statement.execute(s"DROP NAMESPACE IF EXISTS $cg.$schema")
      }
    }
  }

  test("get tables types and comments from the batched v1 lookup as on the fallback") {
    val cg = "spark_catalog"
    val schemaBatch = "db_batch"
    val schemaFallback = "db_batch_fallback"
    val table = "batch_table"
    val view = "batch_view"
    val comment = "tbl_comment"
    withDatabases(schemaBatch, schemaFallback) { statement =>
      Seq(schemaBatch, schemaFallback).foreach { schema =>
        statement.execute(s"CREATE NAMESPACE IF NOT EXISTS $cg.$schema")
      }
      try {
        // The table-level engine.hive.enabled=false overrides HMSIcebergSuiteMixin's
        // session-level flag and registers the table without the handler, so the
        // batched v1 lookup succeeds.
        statement.execute(
          s"CREATE TABLE IF NOT EXISTS $cg.$schemaBatch.$table(c STRING) USING iceberg " +
            s"COMMENT '$comment' TBLPROPERTIES ('engine.hive.enabled' = 'false')")
        statement.execute(s"CREATE VIEW IF NOT EXISTS $cg.$schemaBatch.$view AS SELECT 1 AS a")
        // HMSIcebergSuiteMixin's flag applies without the override, forcing the fallback.
        statement.execute(
          s"CREATE TABLE IF NOT EXISTS $cg.$schemaFallback.$table(c STRING) USING iceberg " +
            s"COMMENT '$comment'")
        statement.execute(s"CREATE VIEW IF NOT EXISTS $cg.$schemaFallback.$view AS SELECT 1 AS a")

        val metaData = statement.getConnection.getMetaData
        def typeAndRemark(schema: String): Map[String, (String, String)] = {
          val rs = metaData.getTables(cg, schema, "%", null)
          val result = scala.collection.mutable.Map.empty[String, (String, String)]
          while (rs.next()) {
            result += rs.getString(TABLE_NAME) ->
              (rs.getString(TABLE_TYPE), rs.getString(REMARKS))
          }
          result.toMap
        }
        val byBatch = typeAndRemark(schemaBatch)
        val byFallback = typeAndRemark(schemaFallback)
        assert(byBatch.keySet == Set(table, view))
        assert(byBatch(table)._1 == "TABLE")
        assert(byBatch(view)._1 == "VIEW")
        assert(byBatch == byFallback)
      } finally {
        Seq(schemaBatch, schemaFallback).foreach { schema =>
          statement.execute(s"DROP VIEW IF EXISTS $cg.$schema.$view")
          statement.execute(s"DROP TABLE IF EXISTS $cg.$schema.$table PURGE")
          statement.execute(s"DROP NAMESPACE IF EXISTS $cg.$schema")
        }
      }
    }
  }

  test("get tables labels temp views with the session catalog") {
    val schema = "global_temp"
    val view = "temp_view_type"
    withJdbcStatement() { statement =>
      statement.execute(s"CREATE GLOBAL TEMP VIEW $view AS SELECT 1 AS a")
      try {
        val metaData = statement.getConnection.getMetaData
        val rs = metaData.getTables(null, schema, "%", Array("VIEW"))
        assert(rs.next())
        assert(rs.getString(TABLE_CAT) === "spark_catalog")
        assert(rs.getString(TABLE_SCHEM) === schema)
        assert(rs.getString(TABLE_NAME) === view)
        assert(rs.getString(TABLE_TYPE) === "VIEW")
        assert(!rs.next())
      } finally {
        statement.execute(s"DROP VIEW IF EXISTS $schema.$view")
      }
    }
  }

  test("get tables and columns with names needing quotes") {
    val cg = "spark_catalog"
    val schema = "2026"
    val table = "2026"
    val column = "c"
    withJdbcStatement() { statement =>
      statement.execute(s"CREATE NAMESPACE IF NOT EXISTS $cg.`$schema`")
      try {
        statement.execute(
          s"CREATE TABLE IF NOT EXISTS $cg.`$schema`.`$table`($column STRING) USING iceberg")
        val metaData = statement.getConnection.getMetaData
        // schemaPattern and tablePattern match the raw names, not the backtick-quoted form
        val rs1 = metaData.getTables(cg, schema, "%", null)
        assert(rs1.next())
        assert(rs1.getString(TABLE_SCHEM) === schema)
        assert(rs1.getString(TABLE_NAME) === table)
        assert(!rs1.next())
        val rs2 = metaData.getTables(cg, schema, "20%", null)
        assert(rs2.next())
        assert(!rs2.next())

        // getColumns resolves the same raw names, so TABLE_SCHEM feeds straight back in.
        val rs3 = metaData.getColumns(cg, schema, "%", "%")
        assert(rs3.next())
        assert(rs3.getString(TABLE_SCHEM) === schema)
        assert(rs3.getString(TABLE_NAME) === table)
        assert(rs3.getString(COLUMN_NAME) === column)
        assert(!rs3.next())
      } finally {
        statement.execute(s"DROP TABLE IF EXISTS $cg.`$schema`.`$table` PURGE")
        statement.execute(s"DROP NAMESPACE IF EXISTS $cg.`$schema`")
      }
    }
  }

  test("get columns with a temp view in the session") {
    val cg = "spark_catalog"
    val tempView = "columns_temp_view"
    val column = "c"
    withJdbcStatement() { statement =>
      statement.execute(s"CREATE OR REPLACE TEMPORARY VIEW $tempView($column INT) AS SELECT 1")
      try {
        val metaData = statement.getConnection.getMetaData
        val rs = metaData.getColumns(cg, "default", "%", "%")
        var tempViewColumns = 0
        while (rs.next()) {
          if (rs.getString(TABLE_NAME) == tempView) {
            tempViewColumns += 1
            assert(rs.getString(TABLE_SCHEM) == null)
            assert(rs.getString(COLUMN_NAME) === column)
          }
        }
        assert(tempViewColumns == 1)
      } finally {
        statement.execute(s"DROP VIEW IF EXISTS $tempView")
      }
    }
  }

  test("get columns operation") {
    val dataTypes = Seq(
      "boolean",
      "int",
      "bigint",
      "float",
      "double",
      "decimal(38,20)",
      "decimal(10,2)",
      "string",
      "array<bigint>",
      "array<string>",
      "map<int, bigint>",
      "date",
      "timestamp",
      "struct<X: bigint, Y: double>",
      "binary",
      "struct<X: string>")
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
        val expectedJavaTypes = Seq(
          BOOLEAN,
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
          STRUCT,
          BINARY,
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
