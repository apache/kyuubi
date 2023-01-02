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
class MultiV2JdbcTableCatalogRangerSparkExtensionSuite extends RangerSparkExtensionSuite {
  override protected val catalogImpl: String = "in-memory"

  val catalogV2 = "catalog2"
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

      doAs("admin", sql(s"CREATE DATABASE IF NOT EXISTS $catalogV2.$namespace1"))
      doAs(
        "admin",
        sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$table1" +
          " (id int, name string, city string)"))
      doAs(
        "admin",
        sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.$namespace1.$outputTable1" +
          " (id int, name string, city string)"))
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
    val db = "default_tom"
    val table = "table"

    withCleanTmpResources(Seq(
      (s"$catalogV2.$db.${table}_use1", "table"),
      (s"$catalogV2.$db.${table}_use2", "table"),
      (s"$catalogV2.$db.${table}_select1", "table"),
      (s"$catalogV2.$db.${table}_select2", "table"),
      (s"$catalogV2.$db.${table}_select3", "table"))) {
      doAs("admin", sql(s"CREATE DATABASE IF NOT EXISTS $catalogV2.$db"))
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.$db.${table}_use1 (key int)"))
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.$db.${table}_use2 (key int)"))
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.$db.${table}_select1 (key int)"))
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.$db.${table}_select21 (key int)"))
      doAs("admin", sql(s"CREATE TABLE IF NOT EXISTS $catalogV2.$db.${table}_select3 (key int)"))

      doAs(
        "admin",
        assert(sql(s"select * from $catalogV2.$db.${table}_select21").collect().length === 0))

      doAs(
        "tom",
        assert(sql(s"select * from $catalogV2.$db.${table}_select21").collect().length === 0))
    }
  }
}
