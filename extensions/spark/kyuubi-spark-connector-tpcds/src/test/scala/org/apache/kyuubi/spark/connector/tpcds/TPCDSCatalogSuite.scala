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

  test("tpcds.sf1 size") {
    def assertStats(tableName: String, sizeInBytes: BigInt = 1): Unit = {
      val stats = spark.table(tableName).queryExecution.analyzed.stats
      assert(stats.sizeInBytes == sizeInBytes)
      // See https://issues.apache.org/jira/browse/SPARK-33954
      // stats.rowCount only has value in Spark3.2 and above
    }

    assertStats("tpcds.sf1.call_center", 1830)
    assertStats("tpcds.sf1.catalog_page", 1628802)
    assertStats("tpcds.sf1.catalog_returns", 26560000)
    assertStats("tpcds.sf1.catalog_sales", 36160000)
    assertStats("tpcds.sf1.customer", 13200000)
    assertStats("tpcds.sf1.customer_address", 5500000)
    assertStats("tpcds.sf1.customer_demographics", 80673600)
    assertStats("tpcds.sf1.date_dim", 10299909)
    assertStats("tpcds.sf1.household_demographics", 151200)
    assertStats("tpcds.sf1.income_band", 320)
    assertStats("tpcds.sf1.inventory", 187920000)
    assertStats("tpcds.sf1.item", 5058000)
    assertStats("tpcds.sf1.promotion", 37200)
    assertStats("tpcds.sf1.reason", 1330)
    assertStats("tpcds.sf1.ship_mode", 1120)
    assertStats("tpcds.sf1.store", 3156)
    assertStats("tpcds.sf1.store_returns", 32160000)
    assertStats("tpcds.sf1.store_sales", 39360000)
    assertStats("tpcds.sf1.time_dim", 5097600)
    assertStats("tpcds.sf1.warehouse", 585)
    assertStats("tpcds.sf1.web_page", 5760)
    assertStats("tpcds.sf1.web_returns", 9720000)
    assertStats("tpcds.sf1.web_sales", 13560000)
    assertStats("tpcds.sf1.web_site", 8760)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
  }
}
