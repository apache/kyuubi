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
import org.apache.spark.sql.util.CaseInsensitiveStringMap

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

  test("get catalog name") {
    val catalog = new TPCDSCatalog
    val catalogName = "test"
    catalog.initialize(catalogName, CaseInsensitiveStringMap.empty())
    assert(catalog._name == catalogName)
  }

  test("supports namespaces") {
    spark.sql("use tpcds")
    assert(spark.sql(s"SHOW DATABASES").collect().length == 11)
    assert(spark.sql(s"SHOW NAMESPACES IN tpcds.sf1").collect().length == 0)
  }

  test("tpcds.tiny count") {
    assert(spark.table("tpcds.tiny.call_center").count === 2)
    assert(spark.table("tpcds.tiny.catalog_page").count === 11718)
    assert(spark.table("tpcds.tiny.catalog_returns").count === 8923)
    assert(spark.table("tpcds.tiny.catalog_sales").count === 89807)
    assert(spark.table("tpcds.tiny.customer").count === 1000)
    assert(spark.table("tpcds.tiny.customer_address").count === 1000)
    assert(spark.table("tpcds.tiny.customer_demographics").count === 1920800)
    assert(spark.table("tpcds.tiny.date_dim").count === 73049)
    assert(spark.table("tpcds.tiny.household_demographics").count === 7200)
    assert(spark.table("tpcds.tiny.income_band").count === 20)
    assert(spark.table("tpcds.tiny.inventory").count === 261261)
    assert(spark.table("tpcds.tiny.item").count === 2000)
    assert(spark.table("tpcds.tiny.promotion").count === 3)
    assert(spark.table("tpcds.tiny.reason").count === 1)
    assert(spark.table("tpcds.tiny.ship_mode").count === 20)
    assert(spark.table("tpcds.tiny.store").count === 2)
    assert(spark.table("tpcds.tiny.store_returns").count === 11925)
    assert(spark.table("tpcds.tiny.store_sales").count === 120527)
    assert(spark.table("tpcds.tiny.time_dim").count === 86400)
    assert(spark.table("tpcds.tiny.warehouse").count === 1)
    assert(spark.table("tpcds.tiny.web_page").count === 2)
    assert(spark.table("tpcds.tiny.web_returns").count === 1152)
    assert(spark.table("tpcds.tiny.web_sales").count === 11876)
    assert(spark.table("tpcds.tiny.web_site").count === 2)
  }

  test("tpcds.tiny stats") {
    def assertStats(tableName: String, sizeInBytes: BigInt, rowCount: BigInt): Unit = {
      val stats = spark.table(tableName).queryExecution.analyzed.stats
      assert(stats.sizeInBytes == sizeInBytes)
      // stats.rowCount only has value after SPARK-33954
      if (SparkUtils.isSparkVersionAtLeast("3.2")) {
        assert(stats.rowCount.contains(rowCount), tableName)
      }
    }

    assertStats("tpcds.tiny.call_center", 610, 2)
    assertStats("tpcds.tiny.catalog_page", 1628802, 11718)
    assertStats("tpcds.tiny.catalog_returns", 1481218, 8923)
    assertStats("tpcds.tiny.catalog_sales", 20296382, 89807)
    assertStats("tpcds.tiny.customer", 132000, 1000)
    assertStats("tpcds.tiny.customer_address", 110000, 1000)
    assertStats("tpcds.tiny.customer_demographics", 80673600, 1920800)
    assertStats("tpcds.tiny.date_dim", 10299909, 73049)
    assertStats("tpcds.tiny.household_demographics", 151200, 7200)
    assertStats("tpcds.tiny.income_band", 320, 20)
    assertStats("tpcds.tiny.inventory", 4180176, 261261)
    assertStats("tpcds.tiny.item", 562000, 2000)
    assertStats("tpcds.tiny.promotion", 372, 3)
    assertStats("tpcds.tiny.reason", 38, 1)
    assertStats("tpcds.tiny.ship_mode", 1120, 20)
    assertStats("tpcds.tiny.store", 526, 2)
    assertStats("tpcds.tiny.store_returns", 1597950, 11925)
    assertStats("tpcds.tiny.store_sales", 19766428, 120527)
    assertStats("tpcds.tiny.time_dim", 5097600, 86400)
    assertStats("tpcds.tiny.warehouse", 117, 1)
    assertStats("tpcds.tiny.web_page", 192, 2)
    assertStats("tpcds.tiny.web_returns", 186624, 1152)
    assertStats("tpcds.tiny.web_sales", 2683976, 11876)
    assertStats("tpcds.tiny.web_site", 584, 2)
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
