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

package org.apache.kyuubi.spark.connector.tpcds

import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.KyuubiFunSuite

class TPCDSCatalogSuite extends KyuubiFunSuite {

  protected lazy val spark: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.catalogImplementation", "in-memory")
      .config("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
      .getOrCreate()
  }

  test("tpcds.sf1 count") {
    assert(spark.table("tpcds.sf1.call_center").count === 6)
    assert(spark.table("tpcds.sf1.catalog_page").count === 11718)
    assert(spark.table("tpcds.sf1.catalog_returns").count === 144067)
    assert(spark.table("tpcds.sf1.catalog_sales").count === 1441548)
    assert(spark.table("tpcds.sf1.customer").count === 100000)
    assert(spark.table("tpcds.sf1.customer_address").count === 50000)
    assert(spark.table("tpcds.sf1.customer_demographics").count === 1920800)
    assert(spark.table("tpcds.sf1.date_dim").count === 73049)
    assert(spark.table("tpcds.sf1.household_demographics").count === 7200)
    assert(spark.table("tpcds.sf1.income_band").count === 20)
    assert(spark.table("tpcds.sf1.inventory").count === 11745000)
    assert(spark.table("tpcds.sf1.item").count === 18000)
    assert(spark.table("tpcds.sf1.promotion").count === 300)
    assert(spark.table("tpcds.sf1.reason").count === 35)
    assert(spark.table("tpcds.sf1.ship_mode").count === 20)
    assert(spark.table("tpcds.sf1.store").count === 12)
    assert(spark.table("tpcds.sf1.store_returns").count === 287514)
    assert(spark.table("tpcds.sf1.store_sales").count === 2880404)
    assert(spark.table("tpcds.sf1.time_dim").count === 86400)
    assert(spark.table("tpcds.sf1.warehouse").count === 5)
    assert(spark.table("tpcds.sf1.web_page").count === 60)
    assert(spark.table("tpcds.sf1.web_returns").count === 71763)
    assert(spark.table("tpcds.sf1.web_sales").count === 719384)
    assert(spark.table("tpcds.sf1.web_site").count === 30)
  }

  test("tpcds.sf1 stats") {
    def assertStats(tableName: String, sizeInBytes: BigInt = 1, rowCount: BigInt = 1): Unit = {
      val stats = spark.table(tableName).queryExecution.analyzed.stats
      assert(stats.sizeInBytes == sizeInBytes)
      assert(stats.rowCount.contains(rowCount))
    }

    assertStats("tpcds.sf1.call_center", 2640, 6)
    assertStats("tpcds.sf1.catalog_page", 1218672, 11718)
    assertStats("tpcds.sf1.catalog_returns", 33920000, 160000)
    assertStats("tpcds.sf1.catalog_sales", 42880000, 160000)
    assertStats("tpcds.sf1.customer", 22800000, 100000)
    assertStats("tpcds.sf1.customer_address", 11800000, 50000)
    assertStats("tpcds.sf1.customer_demographics", 199763200, 1920800)
    assertStats("tpcds.sf1.date_dim", 21330308, 73049)
    assertStats("tpcds.sf1.household_demographics", 316800, 7200)
    assertStats("tpcds.sf1.income_band", 320, 20)
    assertStats("tpcds.sf1.inventory", 328860000, 11745000)
    assertStats("tpcds.sf1.item", 5256000, 18000)
    assertStats("tpcds.sf1.promotion", 91200, 300)
    assertStats("tpcds.sf1.reason", 1680, 35)
    assertStats("tpcds.sf1.ship_mode", 2160, 20)
    assertStats("tpcds.sf1.store", 5040, 12)
    assertStats("tpcds.sf1.store_returns", 37440000, 240000)
    assertStats("tpcds.sf1.store_sales", 43200000, 240000)
    assertStats("tpcds.sf1.time_dim", 10713600, 86400)
    assertStats("tpcds.sf1.warehouse", 1200, 5)
    assertStats("tpcds.sf1.web_page", 8160, 60)
    assertStats("tpcds.sf1.web_returns", 11280000, 60000)
    assertStats("tpcds.sf1.web_sales", 16080000, 60000)
    assertStats("tpcds.sf1.web_site", 11880, 30)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
  }
}
