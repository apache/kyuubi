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

package org.apache.spark.sql

import java.io.File

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

import org.apache.kyuubi.sql.{KyuubiSQLConf, KyuubiSQLExtensionException}
import org.apache.kyuubi.sql.watchdog.{MaxFileSizeExceedException, MaxPartitionExceedException}

class WatchDogSuite extends KyuubiSparkSQLExtensionTest {
  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setupData()
  }

  case class LimitAndExpected(limit: Int, expected: Int)

  val limitAndExpecteds = List(LimitAndExpected(1, 1), LimitAndExpected(11, 10))

  private def checkMaxPartition: Unit = {
    withSQLConf(KyuubiSQLConf.WATCHDOG_MAX_PARTITIONS.key -> "100") {
      checkAnswer(sql("SELECT count(distinct(p)) FROM test"), Row(10) :: Nil)
    }
    withSQLConf(KyuubiSQLConf.WATCHDOG_MAX_PARTITIONS.key -> "5") {
      sql("SELECT * FROM test where p=1").queryExecution.sparkPlan

      sql(s"SELECT * FROM test WHERE p in (${Range(0, 5).toList.mkString(",")})")
        .queryExecution.sparkPlan

      intercept[MaxPartitionExceedException](
        sql("SELECT * FROM test where p != 1").queryExecution.sparkPlan)

      intercept[MaxPartitionExceedException](
        sql("SELECT * FROM test").queryExecution.sparkPlan)

      intercept[MaxPartitionExceedException](sql(
        s"SELECT * FROM test WHERE p in (${Range(0, 6).toList.mkString(",")})")
        .queryExecution.sparkPlan)
    }
  }

  test("watchdog with scan maxPartitions -- hive") {
    Seq("textfile", "parquet").foreach { format =>
      withTable("test", "temp") {
        sql(
          s"""
             |CREATE TABLE test(i int)
             |PARTITIONED BY (p int)
             |STORED AS $format""".stripMargin)
        spark.range(0, 10, 1).selectExpr("id as col")
          .createOrReplaceTempView("temp")

        for (part <- Range(0, 10)) {
          sql(
            s"""
               |INSERT OVERWRITE TABLE test PARTITION (p='$part')
               |select col from temp""".stripMargin)
        }
        checkMaxPartition
      }
    }
  }

  test("watchdog with scan maxPartitions -- data source") {
    withTempDir { dir =>
      withTempView("test") {
        spark.range(10).selectExpr("id", "id as p")
          .write
          .partitionBy("p")
          .mode("overwrite")
          .save(dir.getCanonicalPath)
        spark.read.load(dir.getCanonicalPath).createOrReplaceTempView("test")
        checkMaxPartition
      }
    }
  }

  private def checkMaxFileSize(tableSize: Long, nonPartTableSize: Long): Unit = {
    withSQLConf(KyuubiSQLConf.WATCHDOG_MAX_FILE_SIZE.key -> tableSize.toString) {
      checkAnswer(sql("SELECT count(distinct(p)) FROM test"), Row(10) :: Nil)
    }

    withSQLConf(KyuubiSQLConf.WATCHDOG_MAX_FILE_SIZE.key -> (tableSize / 2).toString) {
      sql("SELECT * FROM test where p=1").queryExecution.sparkPlan

      sql(s"SELECT * FROM test WHERE p in (${Range(0, 3).toList.mkString(",")})")
        .queryExecution.sparkPlan

      intercept[MaxFileSizeExceedException](
        sql("SELECT * FROM test where p != 1").queryExecution.sparkPlan)

      intercept[MaxFileSizeExceedException](
        sql("SELECT * FROM test").queryExecution.sparkPlan)

      intercept[MaxFileSizeExceedException](sql(
        s"SELECT * FROM test WHERE p in (${Range(0, 6).toList.mkString(",")})")
        .queryExecution.sparkPlan)
    }

    withSQLConf(KyuubiSQLConf.WATCHDOG_MAX_FILE_SIZE.key -> nonPartTableSize.toString) {
      checkAnswer(sql("SELECT count(*) FROM test_non_part"), Row(10000) :: Nil)
    }

    withSQLConf(KyuubiSQLConf.WATCHDOG_MAX_FILE_SIZE.key -> (nonPartTableSize - 1).toString) {
      intercept[MaxFileSizeExceedException](
        sql("SELECT * FROM test_non_part").queryExecution.sparkPlan)
    }
  }

  test("watchdog with scan maxFileSize -- hive") {
    Seq(false).foreach { convertMetastoreParquet =>
      withTable("test", "test_non_part", "temp") {
        spark.range(10000).selectExpr("id as col")
          .createOrReplaceTempView("temp")

        // partitioned table
        sql(
          s"""
             |CREATE TABLE test(i int)
             |PARTITIONED BY (p int)
             |STORED AS parquet""".stripMargin)
        for (part <- Range(0, 10)) {
          sql(
            s"""
               |INSERT OVERWRITE TABLE test PARTITION (p='$part')
               |select col from temp""".stripMargin)
        }

        val tablePath = new File(spark.sessionState.catalog.externalCatalog
          .getTable("default", "test").location)
        val tableSize = FileUtils.listFiles(tablePath, Array("parquet"), true).asScala
          .map(_.length()).sum
        assert(tableSize > 0)

        // non-partitioned table
        sql(
          s"""
             |CREATE TABLE test_non_part(i int)
             |STORED AS parquet""".stripMargin)
        sql(
          s"""
             |INSERT OVERWRITE TABLE test_non_part
             |select col from temp""".stripMargin)
        sql("ANALYZE TABLE test_non_part COMPUTE STATISTICS")

        val nonPartTablePath = new File(spark.sessionState.catalog.externalCatalog
          .getTable("default", "test_non_part").location)
        val nonPartTableSize = FileUtils.listFiles(nonPartTablePath, Array("parquet"), true).asScala
          .map(_.length()).sum
        assert(nonPartTableSize > 0)

        // check
        withSQLConf("spark.sql.hive.convertMetastoreParquet" -> convertMetastoreParquet.toString) {
          checkMaxFileSize(tableSize, nonPartTableSize)
        }
      }
    }
  }

  test("watchdog with scan maxFileSize -- data source") {
    withTempDir { dir =>
      withTempView("test", "test_non_part") {
        // partitioned table
        val tablePath = new File(dir, "test")
        spark.range(10).selectExpr("id", "id as p")
          .write
          .partitionBy("p")
          .mode("overwrite")
          .parquet(tablePath.getCanonicalPath)
        spark.read.load(tablePath.getCanonicalPath).createOrReplaceTempView("test")

        val tableSize = FileUtils.listFiles(tablePath, Array("parquet"), true).asScala
          .map(_.length()).sum
        assert(tableSize > 0)

        // non-partitioned table
        val nonPartTablePath = new File(dir, "test_non_part")
        spark.range(10000).selectExpr("id", "id as p")
          .write
          .mode("overwrite")
          .parquet(nonPartTablePath.getCanonicalPath)
        spark.read.load(nonPartTablePath.getCanonicalPath).createOrReplaceTempView("test_non_part")

        val nonPartTableSize = FileUtils.listFiles(nonPartTablePath, Array("parquet"), true).asScala
          .map(_.length()).sum
        assert(tableSize > 0)

        // check
        checkMaxFileSize(tableSize, nonPartTableSize)
      }
    }
  }

  test("disable script transformation") {
    withSQLConf(KyuubiSQLConf.SCRIPT_TRANSFORMATION_ENABLED.key -> "false") {
      val e = intercept[KyuubiSQLExtensionException] {
        sql("SELECT TRANSFORM('') USING 'ls /'")
      }
      assert(e.getMessage == "Script transformation is not allowed")
    }
  }

  test("watchdog with scan maxFileSize -- data source v2") {
    val df = spark.read.format(classOf[ReportStatisticsAndPartitionAwareDataSource].getName).load()
    df.createOrReplaceTempView("test")
    val logical = df.queryExecution.optimizedPlan.collect {
      case d: DataSourceV2ScanRelation => d
    }.head
    val tableSize = logical.computeStats().sizeInBytes.toLong
    withSQLConf(KyuubiSQLConf.WATCHDOG_MAX_FILE_SIZE.key -> tableSize.toString) {
      sql("SELECT * FROM test").queryExecution.sparkPlan
    }
    withSQLConf(KyuubiSQLConf.WATCHDOG_MAX_FILE_SIZE.key -> (tableSize / 2).toString) {
      intercept[MaxFileSizeExceedException](
        sql("SELECT * FROM test").queryExecution.sparkPlan)
    }

    val nonPartDf = spark.read.format(classOf[ReportStatisticsDataSource].getName).load()
    nonPartDf.createOrReplaceTempView("test_non_part")
    val nonPartLogical = nonPartDf.queryExecution.optimizedPlan.collect {
      case d: DataSourceV2ScanRelation => d
    }.head
    val nonPartTableSize = nonPartLogical.computeStats().sizeInBytes.toLong

    withSQLConf(KyuubiSQLConf.WATCHDOG_MAX_FILE_SIZE.key -> nonPartTableSize.toString) {
      sql("SELECT * FROM test_non_part").queryExecution.sparkPlan
    }

    withSQLConf(KyuubiSQLConf.WATCHDOG_MAX_FILE_SIZE.key -> (nonPartTableSize / 2).toString) {
      intercept[MaxFileSizeExceedException](
        sql("SELECT * FROM test_non_part").queryExecution.sparkPlan)
    }
  }
}
