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

import java.nio.file.Path

import org.apache.kyuubi.Utils
import org.apache.kyuubi.operation.meta.ResultSetSchemaConstant.TABLE_CAT

trait BasicIcebergJDBCTests extends JDBCTestUtils {

  protected def catalog: String = "hadoop_prod"

  protected val iceberg: String = {
    System.getProperty("java.class.path")
      .split(":")
      .filter(_.contains("iceberg-spark")).head
  }

  protected val warehouse: Path = Utils.createTempDir()

  protected val icebergConfigs = Map(
    "spark.sql.defaultCatalog" -> catalog,
    "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog" -> "org.apache.iceberg.spark.SparkSessionCatalog",
    "spark.sql.catalog.spark_catalog.type" -> "hive",
    s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
    s"spark.sql.catalog.$catalog.type" -> "hadoop",
    s"spark.sql.catalog.$catalog.warehouse" -> warehouse.toString,
    "spark.jars" -> iceberg)

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

      val allPattern = Seq("", "*", "%", null, ".*", "_*", "_%", ".%")

      // The session catalog
      allPattern foreach { pattern =>
        checkGetSchemas(metaData.getSchemas("spark_catalog", pattern), dbDflts, "spark_catalog")
      }

      Seq(null, catalog).foreach { cg =>
        allPattern foreach { pattern =>
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
}
