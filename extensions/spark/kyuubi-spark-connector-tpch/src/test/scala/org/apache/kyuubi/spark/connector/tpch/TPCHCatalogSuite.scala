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

package org.apache.kyuubi.spark.connector.tpch

import org.apache.spark.sql.{AnalysisException, SparkSession}

import org.apache.kyuubi.KyuubiFunSuite

class TPCHCatalogSuite extends KyuubiFunSuite {

  protected lazy val spark: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.catalogImplementation", "in-memory")
      .config("spark.sql.catalog.tpch", classOf[TPCHCatalog].getName)
      .config("spark.sql.cbo.enabled", "true")
      .config("spark.sql.cbo.planStats.enabled", "true")
      .getOrCreate()
  }

  test("supports namespaces") {
    spark.sql("use tpch")
    assert(spark.sql(s"SHOW DATABASES").collect().length == 11)
    assert(spark.sql(s"SHOW NAMESPACES IN tpch.sf1").collect().length == 0)
  }

  test("tpch.sf1 count") {
    assert(spark.table("tpch.sf1.customer").count === 150000)
    assert(spark.table("tpch.sf1.orders").count === 1500000)
    assert(spark.table("tpch.sf1.lineitem").count === 6001215)
    assert(spark.table("tpch.sf1.part").count === 200000)
    assert(spark.table("tpch.sf1.partsupp").count === 800000)
    assert(spark.table("tpch.sf1.supplier").count === 10000)
    assert(spark.table("tpch.sf1.nation").count === 25)
    assert(spark.table("tpch.sf1.region").count === 5)
  }

  test("tpch.sf1 stats") {
    def assertStats(tableName: String, sizeInBytes: BigInt, rowCount: BigInt): Unit = {
      val stats = spark.table(tableName).queryExecution.analyzed.stats
      assert(stats.sizeInBytes == sizeInBytes)
      // stats.rowCount only has value after SPARK-33954
      if (SparkUtils.isSparkVersionAtLeast("3.2")) {
        assert(stats.rowCount.contains(rowCount), tableName)
      }
    }

    assertStats("tpch.sf1.customer", 26850000, 150000)
    assertStats("tpch.sf1.orders", 156000000, 1500000)
    assertStats("tpch.sf1.lineitem", 672136080, 6001215)
    assertStats("tpch.sf1.part", 31000000, 200000)
    assertStats("tpch.sf1.partsupp", 115200000, 800000)
    assertStats("tpch.sf1.supplier", 1590000, 10000)
    assertStats("tpch.sf1.nation", 3200, 25)
    assertStats("tpch.sf1.region", 620, 5)

  }

  test("nonexistent table") {
    val exception = intercept[AnalysisException] {
      spark.table("tpch.sf1.nonexistent_table")
    }
    assert(exception.message === "Table or view not found: tpch.sf1.nonexistent_table")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
  }
}
