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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession

class TPCHCatalogSuite extends KyuubiFunSuite {

  test("get catalog name") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpch", classOf[TPCHCatalog].getName)
      .set("spark.sql.cbo.enabled", "true")
      .set("spark.sql.cbo.planStats.enabled", "true")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { _ =>
      val catalog = new TPCHCatalog
      val catalogName = "test"
      catalog.initialize(catalogName, CaseInsensitiveStringMap.empty())
      assert(catalog._name == catalogName)
    }
  }

  test("supports namespaces") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpch", classOf[TPCHCatalog].getName)
      .set("spark.sql.cbo.enabled", "true")
      .set("spark.sql.cbo.planStats.enabled", "true")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      spark.sql("USE tpch")
      assert(spark.sql(s"SHOW DATABASES").collect().length == 12)
      assert(spark.sql(s"SHOW NAMESPACES IN tpch.sf1").collect().length == 0)
    }
  }

  test("exclude databases") {
    Seq(
      "TINY,sf10" -> Seq("tiny", "sf10"),
      "sf1 , " -> Seq("sf1"),
      "none" -> Seq.empty[String]).foreach { case (confValue, expectedExcludeDatabases) =>
      val sparkConf = new SparkConf().setMaster("local[*]")
        .set("spark.ui.enabled", "false")
        .set("spark.sql.catalogImplementation", "in-memory")
        .set("spark.sql.catalog.tpch", classOf[TPCHCatalog].getName)
        .set("spark.sql.catalog.tpch.excludeDatabases", confValue)
      withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
        spark.sql("USE tpch")
        assert(
          spark.sql(s"SHOW DATABASES").collect.map(_.getString(0)).sorted ===
            (TPCHSchemaUtils.DATABASES diff expectedExcludeDatabases).sorted)
      }
    }
  }

  test("tpch.tiny count") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpch", classOf[TPCHCatalog].getName)
      .set("spark.sql.cbo.enabled", "true")
      .set("spark.sql.cbo.planStats.enabled", "true")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      assert(spark.table("tpch.tiny.customer").count === 1500)
      assert(spark.table("tpch.tiny.orders").count === 15000)
      assert(spark.table("tpch.tiny.lineitem").count === 60175)
      assert(spark.table("tpch.tiny.part").count === 2000)
      assert(spark.table("tpch.tiny.partsupp").count === 8000)
      assert(spark.table("tpch.tiny.supplier").count === 100)
      assert(spark.table("tpch.tiny.nation").count === 25)
      assert(spark.table("tpch.tiny.region").count === 5)
    }
  }

  test("tpch.sf0 count") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpch", classOf[TPCHCatalog].getName)
      .set("spark.sql.cbo.enabled", "true")
      .set("spark.sql.cbo.planStats.enabled", "true")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      assert(spark.table("tpch.sf0.customer").count === 0)
      assert(spark.table("tpch.sf0.orders").count === 0)
      assert(spark.table("tpch.sf0.lineitem").count === 0)
      assert(spark.table("tpch.sf0.part").count === 0)
      assert(spark.table("tpch.sf0.partsupp").count === 0)
      assert(spark.table("tpch.sf0.supplier").count === 0)
      assert(spark.table("tpch.sf0.nation").count === 0)
      assert(spark.table("tpch.sf0.region").count === 0)
    }
  }

  test("tpch.sf1 stats") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpch", classOf[TPCHCatalog].getName)
      .set("spark.sql.cbo.enabled", "true")
      .set("spark.sql.cbo.planStats.enabled", "true")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      def assertStats(tableName: String, sizeInBytes: BigInt, rowCount: BigInt): Unit = {
        val stats = spark.table(tableName).queryExecution.analyzed.stats
        assert(stats.sizeInBytes == sizeInBytes)
        assert(stats.rowCount.contains(rowCount), tableName)
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
  }

  test("nonexistent table") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpch", classOf[TPCHCatalog].getName)
      .set("spark.sql.cbo.enabled", "true")
      .set("spark.sql.cbo.planStats.enabled", "true")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      val exception = intercept[AnalysisException] {
        spark.table("tpch.sf1.nonexistent_table")
      }
      assert(exception.message.contains("Table or view not found")
        || exception.message.contains("TABLE_OR_VIEW_NOT_FOUND"))
    }
  }
}
