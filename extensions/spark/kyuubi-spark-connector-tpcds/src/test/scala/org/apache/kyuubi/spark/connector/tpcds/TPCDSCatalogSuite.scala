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
}
