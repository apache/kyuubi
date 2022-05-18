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

import org.apache.spark.sql.{AnalysisException, SparkSession}

import org.apache.kyuubi.KyuubiFunSuite

class TPCDSCatalogSuite extends KyuubiFunSuite {

  protected lazy val spark: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.catalogImplementation", "in-memory")
      .config("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.planStats.enabled", "true")
      .getOrCreate()
  }

  test("supports namespaces") {
    spark.sql("use tpcds")
    assert(spark.sql(s"SHOW DATABASES").collect().length == 10)
    assert(spark.sql(s"SHOW NAMESPACES IN tpcds.sf1").collect().length == 0)
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
    def assertStats(tableName: String, sizeInBytes: BigInt, rowCount: BigInt): Unit = {
      val stats = spark.table(tableName).queryExecution.analyzed.stats
      assert(stats.sizeInBytes == sizeInBytes)
      // stats.rowCount only has value after SPARK-33954
      if (SparkUtils.isSparkVersionAtLeast("3.2")) {
        assert(stats.rowCount.contains(rowCount), tableName)
      }
    }

    // table *_returns and *_sales have different count in spark.table.count and stats.count
    assertStats("tpcds.sf1.call_center", 1830, 6)
    assertStats("tpcds.sf1.catalog_page", 1628802, 11718)
    assertStats("tpcds.sf1.catalog_returns", 26560000, 160000)
    assertStats("tpcds.sf1.catalog_sales", 36160000, 160000)
    assertStats("tpcds.sf1.customer", 13200000, 100000)
    assertStats("tpcds.sf1.customer_address", 5500000, 50000)
    assertStats("tpcds.sf1.customer_demographics", 80673600, 1920800)
    assertStats("tpcds.sf1.date_dim", 10299909, 73049)
    assertStats("tpcds.sf1.household_demographics", 151200, 7200)
    assertStats("tpcds.sf1.income_band", 320, 20)
    assertStats("tpcds.sf1.inventory", 187920000, 11745000)
    assertStats("tpcds.sf1.item", 5058000, 18000)
    assertStats("tpcds.sf1.promotion", 37200, 300)
    assertStats("tpcds.sf1.reason", 1330, 35)
    assertStats("tpcds.sf1.ship_mode", 1120, 20)
    assertStats("tpcds.sf1.store", 3156, 12)
    assertStats("tpcds.sf1.store_returns", 32160000, 240000)
    assertStats("tpcds.sf1.store_sales", 39360000, 240000)
    assertStats("tpcds.sf1.time_dim", 5097600, 86400)
    assertStats("tpcds.sf1.warehouse", 585, 5)
    assertStats("tpcds.sf1.web_page", 5760, 60)
    assertStats("tpcds.sf1.web_returns", 9720000, 60000)
    assertStats("tpcds.sf1.web_sales", 13560000, 60000)
    assertStats("tpcds.sf1.web_site", 8760, 30)
  }

  test("nonexistent table") {
    val exception = intercept[AnalysisException] {
      spark.table("tpcds.sf1.nonexistent_table")
    }
    assert(exception.message === "Table or view not found: tpcds.sf1.nonexistent_table")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
  }
}
