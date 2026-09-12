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

import org.apache.kyuubi.DeltaSuiteMixin
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant._

trait DeltaMetadataTests extends HiveJDBCTestHelper with DeltaSuiteMixin {

  test("get catalogs") {
    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      val catalogs = metaData.getCatalogs
      catalogs.next()
      assert(catalogs.getString(TABLE_CAT) === "spark_catalog")
      assert(!catalogs.next())
    }
  }

  test("get schemas") {
    val dbs = Seq("db1", "db2", "db33", "db44")
    val dbDflts = Seq("default", "global_temp")

    val catalog = "spark_catalog"
    withDatabases(dbs: _*) { statement =>
      dbs.foreach(db => statement.execute(s"CREATE DATABASE IF NOT EXISTS $db"))
      val metaData = statement.getConnection.getMetaData

      Seq("", "*", "%", null, ".*", "_*", "_%", ".%") foreach { pattern =>
        checkGetSchemas(metaData.getSchemas(catalog, pattern), dbs ++ dbDflts, catalog)
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

  test("get tables") {
    val table = "table_1_test"
    val schema = "default"
    val tableType = "TABLE"
    withJdbcStatement(table) { statement =>
      statement.execute(
        s"CREATE TABLE IF NOT EXISTS $table(key int) USING $format COMMENT '$table'")

      val metaData = statement.getConnection.getMetaData
      val rs1 = metaData.getTables(null, null, null, null)

      assert(rs1.next())
      val catalogName = rs1.getString(TABLE_CAT)
      assert(catalogName === "spark_catalog" || catalogName === null)
      assert(rs1.getString(TABLE_SCHEM) === schema)
      assert(rs1.getString(TABLE_NAME) == table)
      assert(rs1.getString(TABLE_TYPE) == tableType)
      assert(rs1.getString(REMARKS) === table)
      assert(!rs1.next())

      val rs2 = metaData.getTables(null, null, "table%", Array("TABLE"))
      assert(rs2.next())
      assert(rs2.getString(TABLE_NAME) == table)
      assert(!rs2.next())

      val rs3 = metaData.getTables(null, "default", "*", Array("VIEW"))
      assert(!rs3.next())
    }
  }

  test("get tables and views") {
    val table = "table_type"
    val view = "view_type"
    val schema = "default"
    withJdbcStatement() { statement =>
      Seq("spark_catalog").foreach { cg =>
        try {
          statement.execute(
            s"CREATE TABLE IF NOT EXISTS $cg.$schema.$table(key int) USING $format")
          statement.execute(s"CREATE VIEW $cg.$schema.$view AS SELECT 1 AS a")

          val metaData = statement.getConnection.getMetaData

          // Without a tableTypes filter, both table and view are returned with their true types.
          val rsAll = metaData.getTables(cg, schema, "%", null)
          val typeByName = scala.collection.mutable.Map.empty[String, String]
          while (rsAll.next()) {
            val name = rsAll.getString(TABLE_NAME)
            if (name == table || name == view) {
              typeByName += name -> rsAll.getString(TABLE_TYPE)
            }
          }
          assert(typeByName(table) == "TABLE")
          assert(typeByName(view) == "VIEW")

          // tableTypes = TABLE returns only the table.
          val rsTable = metaData.getTables(cg, schema, "%", Array("TABLE"))
          val tableNames = scala.collection.mutable.Set.empty[String]
          while (rsTable.next()) {
            tableNames += rsTable.getString(TABLE_NAME)
          }
          assert(tableNames.contains(table))
          assert(!tableNames.contains(view))

          // tableTypes = VIEW returns only the view.
          val rsView = metaData.getTables(cg, schema, "%", Array("VIEW"))
          val viewNames = scala.collection.mutable.Set.empty[String]
          while (rsView.next()) {
            viewNames += rsView.getString(TABLE_NAME)
          }
          assert(viewNames.contains(view))
          assert(!viewNames.contains(table))
        } finally {
          statement.execute(s"DROP VIEW IF EXISTS $cg.$schema.$view")
          statement.execute(s"DROP TABLE IF EXISTS $cg.$schema.$table PURGE")
        }
      }
    }
  }
}
