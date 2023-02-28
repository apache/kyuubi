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
package org.apache.kyuubi.plugin.spark.authz.ranger

import java.sql.DriverManager

import scala.util.Try

// scalastyle:off

/**
 * Tests for RangerSparkExtensionSuite
 * on JdbcTableCatalog with DataSource V2 API.
 */
class V2JdbcTableCatalogRangerSparkExtensionSuite extends RangerSparkExtensionSuite {
  override protected val catalogImpl: String = "in-memory"

  val catalogV2 = "testcat"
  val jdbcCatalogV2 = "jdbc2"
  val namespace1 = "ns1"
  val namespace2 = "ns2"
  val table1 = "table1"
  val table2 = "table2"
  val outputTable1 = "outputTable1"
  val cacheTable1 = "cacheTable1"

  val dbUrl = s"jdbc:derby:memory:$catalogV2"
  val jdbcUrl: String = s"$dbUrl;create=true"

  override def beforeAll(): Unit = {
    if (isSparkV31OrGreater) {
      spark.conf.set(
        s"spark.sql.catalog.$catalogV2",
        "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog")
      spark.conf.set(s"spark.sql.catalog.$catalogV2.url", jdbcUrl)
      spark.conf.set(
        s"spark.sql.catalog.$catalogV2.driver",
        "org.apache.derby.jdbc.AutoloadedDriver")

      super.beforeAll()

      runSqlAs("admin")(s"CREATE DATABASE IF NOT EXISTS $catalogV2.$namespace1")
      runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$table1" +
        " (id int, name string, city string)")
      runSqlAs("admin")(s"CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$outputTable1" +
        " (id int, name string, city string)")
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.sessionState.catalog.reset()
    spark.sessionState.conf.clear()

    // cleanup db
    Try {
      DriverManager.getConnection(s"$dbUrl;shutdown=true")
    }
  }

  test("[KYUUBI #3424] CREATE DATABASE") {
    assume(isSparkV31OrGreater)

    // create database
    runSqlAsWithAccessException()(
      s"CREATE DATABASE IF NOT EXISTS $catalogV2.$namespace2",
      isExplain = true,
      contains = s"does not have [create] privilege on [$namespace2]")
  }

  test("[KYUUBI #3424] DROP DATABASE") {
    assume(isSparkV31OrGreater)

    // create database
    runSqlAsWithAccessException()(
      s"DROP DATABASE IF EXISTS $catalogV2.$namespace2",
      isExplain = true,
      contains = s"does not have [drop] privilege on [$namespace2]")
  }

  test("[KYUUBI #3424] SELECT TABLE") {
    assume(isSparkV31OrGreater)

    // select
    runSqlAsWithAccessException()(
      s"select city, id from $catalogV2.$namespace1.$table1",
      isExplain = true,
      contains = s"does not have [select] privilege on [$namespace1/$table1/city]")
  }

  test("[KYUUBI #4255] DESCRIBE TABLE") {
    assume(isSparkV31OrGreater)
    runSqlAsWithAccessException()(
      s"DESCRIBE TABLE $catalogV2.$namespace1.$table1",
      isExplain = true,
      contains = s"does not have [select] privilege on [$namespace1/$table1]")
  }

  test("[KYUUBI #3424] CREATE TABLE") {
    assume(isSparkV31OrGreater)

    // CreateTable
    runSqlAsWithAccessException()(
      s"CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$table2",
      contains = s"does not have [create] privilege on [$namespace1/$table2]")

    // CreateTableAsSelect
    runSqlAsWithAccessException()(
      s"CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$table2" +
        s" AS select * from $catalogV2.$namespace1.$table1",
      contains = s"does not have [select] privilege on [$namespace1/$table1/id]")
  }

  test("[KYUUBI #3424] DROP TABLE") {
    assume(isSparkV31OrGreater)

    // DropTable
    runSqlAsWithAccessException()(
      s"DROP TABLE $catalogV2.$namespace1.$table1",
      contains = s"does not have [drop] privilege on [$namespace1/$table1]")
  }

  test("[KYUUBI #3424] INSERT TABLE") {
    assume(isSparkV31OrGreater)

    // AppendData: Insert Using a VALUES Clause
    runSqlAsWithAccessException()(
      s"INSERT INTO $catalogV2.$namespace1.$outputTable1 (id, name, city)" +
        s" VALUES (1, 'bowenliang123', 'Guangzhou')",
      contains = s"does not have [update] privilege on [$namespace1/$outputTable1]")

    // AppendData: Insert Using a TABLE Statement
    runSqlAsWithAccessException()(
      s"INSERT INTO $catalogV2.$namespace1.$outputTable1 (id, name, city)" +
        s" TABLE $catalogV2.$namespace1.$table1",
      contains = s"does not have [select] privilege on [$namespace1/$table1/id]")

    // AppendData: Insert Using a SELECT Statement
    runSqlAsWithAccessException()(
      s"INSERT INTO $catalogV2.$namespace1.$outputTable1 (id, name, city)" +
        s" SELECT * from $catalogV2.$namespace1.$table1",
      contains = s"does not have [select] privilege on [$namespace1/$table1/id]")

    // OverwriteByExpression: Insert Overwrite
    runSqlAsWithAccessException()(
      s"INSERT OVERWRITE $catalogV2.$namespace1.$outputTable1 (id, name, city)" +
        s" VALUES (1, 'bowenliang123', 'Guangzhou')",
      contains = s"does not have [update] privilege on [$namespace1/$outputTable1]")
  }

  test("[KYUUBI #3424] MERGE INTO") {
    assume(isSparkV31OrGreater)

    val mergeIntoSql =
      s"""
         |MERGE INTO $catalogV2.$namespace1.$outputTable1 AS target
         |USING $catalogV2.$namespace1.$table1  AS source
         |ON target.id = source.id
         |WHEN MATCHED AND (target.name='delete') THEN DELETE
         |WHEN MATCHED AND (target.name='update') THEN UPDATE SET target.city = source.city
      """.stripMargin

    // MergeIntoTable:  Using a MERGE INTO Statement
    runSqlAsWithAccessException()(
      mergeIntoSql,
      contains = s"does not have [select] privilege on [$namespace1/$table1/id]")

    try {
      SparkRangerAdminPlugin.getRangerConf.setBoolean(
        s"ranger.plugin.${SparkRangerAdminPlugin.getServiceType}.authorize.in.single.call",
        true)
      runSqlAsWithAccessException()(
        mergeIntoSql,
        contains = s"does not have" +
          s" [select] privilege" +
          s" on [$namespace1/$table1/id,$namespace1/table1/name,$namespace1/$table1/city]," +
          s" [update] privilege on [$namespace1/$outputTable1]")
    } finally {
      SparkRangerAdminPlugin.getRangerConf.setBoolean(
        s"ranger.plugin.${SparkRangerAdminPlugin.getServiceType}.authorize.in.single.call",
        false)
    }
  }

  test("[KYUUBI #3424] UPDATE TABLE") {
    assume(isSparkV31OrGreater)

    // UpdateTable
    runSqlAsWithAccessException()(
      s"UPDATE $catalogV2.$namespace1.$table1 SET city='Hangzhou' WHERE id=1",
      contains = s"does not have [update] privilege on [$namespace1/$table1]")
  }

  test("[KYUUBI #3424] DELETE FROM TABLE") {
    assume(isSparkV31OrGreater)

    // DeleteFromTable
    runSqlAsWithAccessException()(
      s"DELETE FROM $catalogV2.$namespace1.$table1 WHERE id=1",
      contains = s"does not have [update] privilege on [$namespace1/$table1]")
  }

  test("[KYUUBI #3424] CACHE TABLE") {
    assume(isSparkV31OrGreater)

    // CacheTable
    val errorMsg = if (isSparkV32OrGreater) {
      s"does not have [select] privilege on [$namespace1/$table1/id]"
    } else {
      s"does not have [select] privilege on [$catalogV2.$namespace1/$table1]"
    }
    runSqlAsWithAccessException()(
      s"CACHE TABLE $cacheTable1 AS select * from $catalogV2.$namespace1.$table1",
      contains = errorMsg)
  }

  test("[KYUUBI #3424] TRUNCATE TABLE") {
    assume(isSparkV32OrGreater)

    runSqlAsWithAccessException()(
      s"TRUNCATE TABLE $catalogV2.$namespace1.$table1",
      contains = s"does not have [update] privilege on [$namespace1/$table1]")
  }

  test("[KYUUBI #3424] MSCK REPAIR TABLE") {
    assume(isSparkV32OrGreater)

    runSqlAsWithAccessException()(
      s"MSCK REPAIR TABLE $catalogV2.$namespace1.$table1",
      contains = s"does not have [alter] privilege on [$namespace1/$table1]")
  }

  test("[KYUUBI #3424] ALTER TABLE") {
    assume(isSparkV31OrGreater)

    // AddColumns
    runSqlAsWithAccessException()(
      s"ALTER TABLE $catalogV2.$namespace1.$table1 ADD COLUMNS (age int)",
      isExplain = true,
      contains = s"does not have [alter] privilege on [$namespace1/$table1]")

    // DropColumns
    runSqlAsWithAccessException()(
      s"ALTER TABLE $catalogV2.$namespace1.$table1 DROP COLUMNS city",
      isExplain = true,
      contains = s"does not have [alter] privilege on [$namespace1/$table1]")

    // RenameColumn
    runSqlAsWithAccessException()(
      s"ALTER TABLE $catalogV2.$namespace1.$table1 RENAME COLUMN city TO city2 ",
      isExplain = true,
      contains = s"does not have [alter] privilege on [$namespace1/$table1]")

    // AlterColumn
    runSqlAsWithAccessException()(
      s"ALTER TABLE $catalogV2.$namespace1.$table1 ALTER COLUMN city COMMENT 'city'",
      contains = s"does not have [alter] privilege on [$namespace1/$table1]")
  }

  test("[KYUUBI #3424] COMMENT ON") {
    assume(isSparkV31OrGreater)

    // CommentOnNamespace
    runSqlAsWithAccessException()(
      s"COMMENT ON DATABASE $catalogV2.$namespace1 IS 'xYz'",
      isExplain = true,
      contains = s"does not have [alter] privilege on [$namespace1]")

    // CommentOnNamespace
    runSqlAsWithAccessException()(
      s"COMMENT ON NAMESPACE $catalogV2.$namespace1 IS 'xYz' ",
      isExplain = true,
      contains = s"does not have [alter] privilege on [$namespace1]")

    // CommentOnTable
    runSqlAsWithAccessException()(
      s"COMMENT ON TABLE $catalogV2.$namespace1.$table1 IS 'xYz' ",
      isExplain = true,
      contains = s"does not have [alter] privilege on [$namespace1/$table1]")

  }
}
