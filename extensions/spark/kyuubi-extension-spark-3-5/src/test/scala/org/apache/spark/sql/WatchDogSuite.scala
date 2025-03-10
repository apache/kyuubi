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
import org.apache.spark.sql.catalyst.plans.logical.{GlobalLimit, LogicalPlan}
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

  test("test watchdog: simple SELECT STATEMENT") {

    withSQLConf(KyuubiSQLConf.WATCHDOG_FORCED_MAXOUTPUTROWS.key -> "10") {

      List("", "ORDER BY c1", "ORDER BY c2").foreach { sort =>
        List("", " DISTINCT").foreach { distinct =>
          assert(sql(
            s"""
               |SELECT $distinct *
               |FROM t1
               |$sort
               |""".stripMargin).queryExecution.optimizedPlan.isInstanceOf[GlobalLimit])
        }
      }

      limitAndExpecteds.foreach { case LimitAndExpected(limit, expected) =>
        List("", "ORDER BY c1", "ORDER BY c2").foreach { sort =>
          List("", "DISTINCT").foreach { distinct =>
            assert(sql(
              s"""
                 |SELECT $distinct *
                 |FROM t1
                 |$sort
                 |LIMIT $limit
                 |""".stripMargin).queryExecution.optimizedPlan.maxRows.contains(expected))
          }
        }
      }
    }
  }

  test("test watchdog: SELECT ... WITH AGGREGATE STATEMENT ") {

    withSQLConf(KyuubiSQLConf.WATCHDOG_FORCED_MAXOUTPUTROWS.key -> "10") {

      assert(!sql("SELECT count(*) FROM t1")
        .queryExecution.optimizedPlan.isInstanceOf[GlobalLimit])

      val sorts = List("", "ORDER BY cnt", "ORDER BY c1", "ORDER BY cnt, c1", "ORDER BY c1, cnt")
      val havingConditions = List("", "HAVING cnt > 1")

      havingConditions.foreach { having =>
        sorts.foreach { sort =>
          assert(sql(
            s"""
               |SELECT c1, COUNT(*) as cnt
               |FROM t1
               |GROUP BY c1
               |$having
               |$sort
               |""".stripMargin).queryExecution.optimizedPlan.isInstanceOf[GlobalLimit])
        }
      }

      limitAndExpecteds.foreach { case LimitAndExpected(limit, expected) =>
        havingConditions.foreach { having =>
          sorts.foreach { sort =>
            assert(sql(
              s"""
                 |SELECT c1, COUNT(*) as cnt
                 |FROM t1
                 |GROUP BY c1
                 |$having
                 |$sort
                 |LIMIT $limit
                 |""".stripMargin).queryExecution.optimizedPlan.maxRows.contains(expected))
          }
        }
      }
    }
  }

  test("test watchdog: SELECT with CTE forceMaxOutputRows") {
    // simple CTE
    val q1 =
      """
        |WITH t2 AS (
        |    SELECT * FROM t1
        |)
        |""".stripMargin

    // nested CTE
    val q2 =
      """
        |WITH
        |    t AS (SELECT * FROM t1),
        |    t2 AS (
        |        WITH t3 AS (SELECT * FROM t1)
        |        SELECT * FROM t3
        |    )
        |""".stripMargin
    withSQLConf(KyuubiSQLConf.WATCHDOG_FORCED_MAXOUTPUTROWS.key -> "10") {

      val sorts = List("", "ORDER BY c1", "ORDER BY c2")

      sorts.foreach { sort =>
        Seq(q1, q2).foreach { withQuery =>
          assert(sql(
            s"""
               |$withQuery
               |SELECT * FROM t2
               |$sort
               |""".stripMargin).queryExecution.optimizedPlan.isInstanceOf[GlobalLimit])
        }
      }

      limitAndExpecteds.foreach { case LimitAndExpected(limit, expected) =>
        sorts.foreach { sort =>
          Seq(q1, q2).foreach { withQuery =>
            assert(sql(
              s"""
                 |$withQuery
                 |SELECT * FROM t2
                 |$sort
                 |LIMIT $limit
                 |""".stripMargin).queryExecution.optimizedPlan.maxRows.contains(expected))
          }
        }
      }
    }
  }

  test("test watchdog: SELECT AGGREGATE WITH CTE forceMaxOutputRows") {

    withSQLConf(KyuubiSQLConf.WATCHDOG_FORCED_MAXOUTPUTROWS.key -> "10") {

      assert(!sql(
        """
          |WITH custom_cte AS (
          |SELECT * FROM t1
          |)
          |
          |SELECT COUNT(*)
          |FROM custom_cte
          |""".stripMargin).queryExecution
        .analyzed.isInstanceOf[GlobalLimit])

      val sorts = List("", "ORDER BY cnt", "ORDER BY c1", "ORDER BY cnt, c1", "ORDER BY c1, cnt")
      val havingConditions = List("", "HAVING cnt > 1")

      havingConditions.foreach { having =>
        sorts.foreach { sort =>
          assert(sql(
            s"""
               |WITH custom_cte AS (
               |SELECT * FROM t1
               |)
               |
               |SELECT c1, COUNT(*) as cnt
               |FROM custom_cte
               |GROUP BY c1
               |$having
               |$sort
               |""".stripMargin).queryExecution.optimizedPlan.isInstanceOf[GlobalLimit])
        }
      }

      limitAndExpecteds.foreach { case LimitAndExpected(limit, expected) =>
        havingConditions.foreach { having =>
          sorts.foreach { sort =>
            assert(sql(
              s"""
                 |WITH custom_cte AS (
                 |SELECT * FROM t1
                 |)
                 |
                 |SELECT c1, COUNT(*) as cnt
                 |FROM custom_cte
                 |GROUP BY c1
                 |$having
                 |$sort
                 |LIMIT $limit
                 |""".stripMargin).queryExecution.optimizedPlan.maxRows.contains(expected))
          }
        }
      }
    }
  }

  test("test watchdog: UNION Statement for forceMaxOutputRows") {

    withSQLConf(KyuubiSQLConf.WATCHDOG_FORCED_MAXOUTPUTROWS.key -> "10") {

      List("", "ALL").foreach { x =>
        assert(sql(
          s"""
             |SELECT c1, c2 FROM t1
             |UNION $x
             |SELECT c1, c2 FROM t2
             |UNION $x
             |SELECT c1, c2 FROM t3
             |""".stripMargin)
          .queryExecution.optimizedPlan.isInstanceOf[GlobalLimit])
      }

      val sorts = List("", "ORDER BY cnt", "ORDER BY c1", "ORDER BY cnt, c1", "ORDER BY c1, cnt")
      val havingConditions = List("", "HAVING cnt > 1")

      List("", "ALL").foreach { x =>
        havingConditions.foreach { having =>
          sorts.foreach { sort =>
            assert(sql(
              s"""
                 |SELECT c1, count(c2) as cnt
                 |FROM t1
                 |GROUP BY c1
                 |$having
                 |UNION $x
                 |SELECT c1, COUNT(c2) as cnt
                 |FROM t2
                 |GROUP BY c1
                 |$having
                 |UNION $x
                 |SELECT c1, COUNT(c2) as cnt
                 |FROM t3
                 |GROUP BY c1
                 |$having
                 |$sort
                 |""".stripMargin)
              .queryExecution.optimizedPlan.isInstanceOf[GlobalLimit])
          }
        }
      }

      limitAndExpecteds.foreach { case LimitAndExpected(limit, expected) =>
        assert(sql(
          s"""
             |SELECT c1, c2 FROM t1
             |UNION
             |SELECT c1, c2 FROM t2
             |UNION
             |SELECT c1, c2 FROM t3
             |LIMIT $limit
             |""".stripMargin)
          .queryExecution.optimizedPlan.maxRows.contains(expected))
      }
    }
  }

  test("test watchdog: Select View Statement for forceMaxOutputRows") {
    withSQLConf(KyuubiSQLConf.WATCHDOG_FORCED_MAXOUTPUTROWS.key -> "3") {
      withTable("tmp_table", "tmp_union") {
        withView("tmp_view", "tmp_view2") {
          sql(s"create table tmp_table (a int, b int)")
          sql(s"insert into tmp_table values (1,10),(2,20),(3,30),(4,40),(5,50)")
          sql(s"create table tmp_union (a int, b int)")
          sql(s"insert into tmp_union values (6,60),(7,70),(8,80),(9,90),(10,100)")
          sql(s"create view tmp_view2 as select * from tmp_union")
          assert(!sql(
            s"""
               |CREATE VIEW tmp_view
               |as
               |SELECT * FROM
               |tmp_table
               |""".stripMargin)
            .queryExecution.optimizedPlan.isInstanceOf[GlobalLimit])

          assert(sql(
            s"""
               |SELECT * FROM
               |tmp_view
               |""".stripMargin)
            .queryExecution.optimizedPlan.maxRows.contains(3))

          assert(sql(
            s"""
               |SELECT * FROM
               |tmp_view
               |limit 11
               |""".stripMargin)
            .queryExecution.optimizedPlan.maxRows.contains(3))

          assert(sql(
            s"""
               |SELECT * FROM
               |(select * from tmp_view
               |UNION
               |select * from tmp_view2)
               |ORDER BY a
               |DESC
               |""".stripMargin)
            .collect().head.get(0) === 10)
        }
      }
    }
  }

  test("test watchdog: Insert Statement for forceMaxOutputRows") {

    withSQLConf(KyuubiSQLConf.WATCHDOG_FORCED_MAXOUTPUTROWS.key -> "10") {
      withTable("tmp_table", "tmp_insert") {
        spark.sql(s"create table tmp_table (a int, b int)")
        spark.sql(s"insert into tmp_table values (1,10),(2,20),(3,30),(4,40),(5,50)")
        val multiInsertTableName1: String = "tmp_tbl1"
        val multiInsertTableName2: String = "tmp_tbl2"
        sql(s"drop table if exists $multiInsertTableName1")
        sql(s"drop table if exists $multiInsertTableName2")
        sql(s"create table $multiInsertTableName1 like tmp_table")
        sql(s"create table $multiInsertTableName2 like tmp_table")
        assert(!sql(
          s"""
             |FROM tmp_table
             |insert into $multiInsertTableName1 select * limit 2
             |insert into $multiInsertTableName2 select *
             |""".stripMargin)
          .queryExecution.optimizedPlan.isInstanceOf[GlobalLimit])
      }
    }
  }

  test("test watchdog: Distribute by for forceMaxOutputRows") {

    withSQLConf(KyuubiSQLConf.WATCHDOG_FORCED_MAXOUTPUTROWS.key -> "10") {
      withTable("tmp_table") {
        spark.sql(s"create table tmp_table (a int, b int)")
        spark.sql(s"insert into tmp_table values (1,10),(2,20),(3,30),(4,40),(5,50)")
        assert(sql(
          s"""
             |SELECT *
             |FROM tmp_table
             |DISTRIBUTE BY a
             |""".stripMargin)
          .queryExecution.optimizedPlan.isInstanceOf[GlobalLimit])
      }
    }
  }

  test("test watchdog: Subquery for forceMaxOutputRows") {
    withSQLConf(KyuubiSQLConf.WATCHDOG_FORCED_MAXOUTPUTROWS.key -> "1") {
      withTable("tmp_table1") {
        sql("CREATE TABLE spark_catalog.`default`.tmp_table1(KEY INT, VALUE STRING) USING PARQUET")
        sql("INSERT INTO TABLE spark_catalog.`default`.tmp_table1 " +
          "VALUES (1, 'aa'),(2,'bb'),(3, 'cc'),(4,'aa'),(5,'cc'),(6, 'aa')")
        assert(
          sql("select * from tmp_table1").queryExecution.optimizedPlan.isInstanceOf[GlobalLimit])
        val testSqlText =
          """
            |select count(*)
            |from tmp_table1
            |where tmp_table1.key in (
            |select distinct tmp_table1.key
            |from tmp_table1
            |where tmp_table1.value = "aa"
            |)
            |""".stripMargin
        val plan = sql(testSqlText).queryExecution.optimizedPlan
        assert(!findGlobalLimit(plan))
        checkAnswer(sql(testSqlText), Row(3) :: Nil)
      }

      def findGlobalLimit(plan: LogicalPlan): Boolean = plan match {
        case _: GlobalLimit => true
        case p if p.children.isEmpty => false
        case p => p.children.exists(findGlobalLimit)
      }

    }
  }

  test("test watchdog: Join for forceMaxOutputRows") {
    withSQLConf(KyuubiSQLConf.WATCHDOG_FORCED_MAXOUTPUTROWS.key -> "1") {
      withTable("tmp_table1", "tmp_table2") {
        sql("CREATE TABLE spark_catalog.`default`.tmp_table1(KEY INT, VALUE STRING) USING PARQUET")
        sql("INSERT INTO TABLE spark_catalog.`default`.tmp_table1 " +
          "VALUES (1, 'aa'),(2,'bb'),(3, 'cc'),(4,'aa'),(5,'cc'),(6, 'aa')")
        sql("CREATE TABLE spark_catalog.`default`.tmp_table2(KEY INT, VALUE STRING) USING PARQUET")
        sql("INSERT INTO TABLE spark_catalog.`default`.tmp_table2 " +
          "VALUES (1, 'aa'),(2,'bb'),(3, 'cc'),(4,'aa'),(5,'cc'),(6, 'aa')")
        val testSqlText =
          """
            |select a.*,b.*
            |from tmp_table1 a
            |join
            |tmp_table2 b
            |on a.KEY = b.KEY
            |""".stripMargin
        val plan = sql(testSqlText).queryExecution.optimizedPlan
        assert(findGlobalLimit(plan))
      }

      def findGlobalLimit(plan: LogicalPlan): Boolean = plan match {
        case _: GlobalLimit => true
        case p if p.children.isEmpty => false
        case p => p.children.exists(findGlobalLimit)
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
