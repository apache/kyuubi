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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession
import org.apache.kyuubi.spark.connector.common.SparkUtils.SPARK_RUNTIME_VERSION

class TPCDSCatalogSuite extends KyuubiFunSuite {

  test("get catalog name") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
      .set("spark.sql.cbo.enabled", "true")
      .set("spark.sql.cbo.planStats.enabled", "true")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { _ =>
      val catalog = new TPCDSCatalog
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
      .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
      .set("spark.sql.cbo.enabled", "true")
      .set("spark.sql.cbo.planStats.enabled", "true")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      spark.sql("USE tpcds")
      assert(spark.sql(s"SHOW DATABASES").collect().length == 11)
      assert(spark.sql(s"SHOW NAMESPACES IN tpcds.sf1").collect().length == 0)
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
        .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
        .set("spark.sql.catalog.tpcds.excludeDatabases", confValue)
      withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
        spark.sql("USE tpcds")
        assert(
          spark.sql(s"SHOW DATABASES").collect.map(_.getString(0)).sorted ===
            (TPCDSSchemaUtils.DATABASES diff expectedExcludeDatabases).sorted)
      }
    }
  }

  test("tpcds.tiny count") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
      .set("spark.sql.cbo.enabled", "true")
      .set("spark.sql.cbo.planStats.enabled", "true")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
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
  }

  test("tpcds.tiny count and checksum") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
      .set("spark.sql.cbo.enabled", "true")
      .set("spark.sql.cbo.planStats.enabled", "true")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      tableInfo.foreach {
        case (table, (expectCount, expectChecksum)) =>
          val (count, checksum) = countAndchecksum(spark, table)
          assert(count == expectCount)
          assert(checksum == expectChecksum, s"table $table")
      }
    }
  }

  test("debug") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
      .set("spark.sql.cbo.enabled", "true")
      .set("spark.sql.cbo.planStats.enabled", "true")
    val table = "store"
//    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
//      val schema = spark.table(s"tpcds.tiny.$table").schema.map(_.name)
//      checksum(spark, s"tpcds.tiny.$table", schema).foreach(println)
//    }
//    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
//      spark.table("tpcds.tiny.customer").schema.map {f =>
//        spark.sql(s"select ${f.name} from tpcds.tiny.customer").show(truncate = false)
//      }
//    }
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      spark.sql(s"select * from tpcds.tiny.$table").printSchema()
      spark.sql(s"select s_rec_start_date from tpcds.tiny.$table").show(truncate = false)
    }
  }

  test("tpcds.sf1 stats") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
      .set("spark.sql.cbo.enabled", "true")
      .set("spark.sql.cbo.planStats.enabled", "true")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      def assertStats(tableName: String, sizeInBytes: BigInt, rowCount: BigInt): Unit = {
        val stats = spark.table(tableName).queryExecution.analyzed.stats
        assert(stats.sizeInBytes == sizeInBytes)
        // stats.rowCount only has value after SPARK-33954
        if (SPARK_RUNTIME_VERSION >= "3.2") {
          assert(stats.rowCount.contains(rowCount), tableName)
        }
      }

      assertStats("tpcds.sf1.call_center", 1830, 6)
      assertStats("tpcds.sf1.catalog_page", 1628802, 11718)
      assertStats("tpcds.sf1.catalog_returns", 23915122, 144067)
      assertStats("tpcds.sf1.catalog_sales", 325789848, 1441548)
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
      assertStats("tpcds.sf1.store_returns", 38526876, 287514)
      assertStats("tpcds.sf1.store_sales", 472386256, 2880404)
      assertStats("tpcds.sf1.time_dim", 5097600, 86400)
      assertStats("tpcds.sf1.warehouse", 585, 5)
      assertStats("tpcds.sf1.web_page", 5760, 60)
      assertStats("tpcds.sf1.web_returns", 11625606, 71763)
      assertStats("tpcds.sf1.web_sales", 162580784, 719384)
      assertStats("tpcds.sf1.web_site", 8760, 30)
    }
  }

  test("nonexistent table") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
      .set("spark.sql.cbo.enabled", "true")
      .set("spark.sql.cbo.planStats.enabled", "true")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      val exception = intercept[AnalysisException] {
        spark.table("tpcds.sf1.nonexistent_table")
      }
      assert(exception.message.contains("Table or view not found")
        || exception.message.contains("TABLE_OR_VIEW_NOT_FOUND"))
    }
  }

  test("aa") {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
      .set("spark.sql.cbo.enabled", "true")
      .set("spark.sql.cbo.planStats.enabled", "true")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
    }
  }

  def countAndchecksum(spark: SparkSession, tableName: String): (String, String) = {
    val df = spark.table(tableName)
    val cols = df.schema.map { field =>
      concat(
        when(col(field.name).isNull, lit('\u0000').cast("string"))
          .otherwise(col(field.name).cast("string")),
        lit('\u0001').cast("string"))
    }

    df.select(
      crc32(concat(cols: _*))
        // 每行的crc32值之和不会超过10^19*10^19=10^38，也就是说不会超过Decimal(38, 0)可表示的范围
        .cast(DataTypes.createDecimalType(38, 0))
        .as("row_checksum"))
      .agg(
        count("*").cast("string").as("count"),
        sum("row_checksum").cast("string").as("checksum"))
      .collect()
      .map(r => (r.getString(0), r.getString(1)))
      .head
  }

  def checksum(
      spark: SparkSession,
      tableName: String,
      schema: Seq[String]): Seq[(String, String)] = {
    val df = spark.table(tableName)
    schema.map { name =>
      val expression = concat(
        when(col(name).isNull, lit('\u0000').cast("string"))
          .otherwise(col(name).cast("string")),
        lit('\u0001').cast("string"))

      val colCheckSum = df.select(
        crc32(expression)
          // 每行的crc32值之和不会超过10^19*10^19=10^38，也就是说不会超过Decimal(38, 0)可表示的范围
          .cast(DataTypes.createDecimalType(38, 0))
          .as("row_checksum"))
        .agg(
          sum("row_checksum").cast("string").as("checksum"))
        .collect()
        .map(r => (r.getString(0)))
        .head
      (name, colCheckSum)
    }
  }

  private val tableInfo = Seq(
    ("tpcds.tiny.call_center", ("2", "4584365911")),
    ("tpcds.tiny.catalog_page", ("11718", "25416854987711")),
    ("tpcds.tiny.catalog_returns", ("8923", "19045021547122")),
    ("tpcds.tiny.catalog_sales", ("89807", "192355655243815")),
    ("tpcds.tiny.customer", ("1000", "2120827330356")),
    ("tpcds.tiny.customer_address", ("1000", "2161077976693")),
    ("tpcds.tiny.customer_demographics", ("1920800", "4124183189708148")),
    ("tpcds.tiny.date_dim", ("73049", "156926081012862")),
    ("tpcds.tiny.household_demographics", ("7200", "15494873325812")),
    ("tpcds.tiny.income_band", ("20", "41180951007")),
    ("tpcds.tiny.inventory", ("261261", "561290989772724")),
    ("tpcds.tiny.item", ("2000", "4254103006936")),
    ("tpcds.tiny.promotion", ("3", "4984911899")),
    ("tpcds.tiny.reason", ("1", "365440741")),
    ("tpcds.tiny.ship_mode", ("20", "52349078860")),
    ("tpcds.tiny.store", ("2", "2964682289")),
    ("tpcds.tiny.store_returns", ("11925", "25400972943896")),
    ("tpcds.tiny.store_sales", ("120527", "259296406856838")),
    ("tpcds.tiny.time_dim", ("86400", "186045071019485")),
    ("tpcds.tiny.warehouse", ("1", "2956768503")),
    ("tpcds.tiny.web_page", ("2", "3215766118")),
    ("tpcds.tiny.web_returns", ("1152", "2464383243098")),
    ("tpcds.tiny.web_sales", ("11876", "25458905770096")),
    ("tpcds.tiny.web_site", ("2", "3798438288")))
}
