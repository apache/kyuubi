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

import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit
import org.apache.kyuubi.sql.KyuubiSQLConf
import org.apache.kyuubi.sql.watchdog.MaxPartitionExceedException
//import org.apache.spark.sql.catalyst.planning.ScanOperation
//import org.apache.kyuubi.sql.watchdog.MaxHivePartitionExceedException

class WatchDogSuite extends KyuubiSparkSQLExtensionTest {
  val factData = Seq[(Int, Int, Int, Int)](
    (1000, 1, 1, 10),
    (1010, 2, 1, 10),
    (1020, 2, 1, 10),
    (1030, 3, 2, 10),
    (1040, 3, 2, 50),
    (1050, 3, 2, 50),
    (1060, 3, 2, 50),
    (1070, 4, 2, 10),
    (1080, 4, 3, 20),
    (1090, 4, 3, 10),
    (1100, 4, 3, 10),
    (1110, 5, 3, 10),
    (1120, 6, 4, 10),
    (1130, 7, 4, 50),
    (1140, 8, 4, 50),
    (1150, 9, 1, 20),
    (1160, 10, 1, 20),
    (1170, 11, 1, 30),
    (1180, 12, 2, 20),
    (1190, 13, 2, 20),
    (1200, 14, 3, 40),
    (1200, 15, 3, 70),
    (1210, 16, 4, 10),
    (1220, 17, 4, 20),
    (1230, 18, 4, 20),
    (1240, 19, 5, 40),
    (1250, 20, 5, 40),
    (1260, 21, 5, 40),
    (1270, 22, 5, 50),
    (1280, 23, 1, 50),
    (1290, 24, 1, 50),
    (1300, 25, 1, 50)
  )

  val storeData = Seq[(Int, String, String)](
    (1, "North-Holland", "NL"),
    (2, "South-Holland", "NL"),
    (3, "Bavaria", "DE"),
    (4, "California", "US"),
    (5, "Texas", "US"),
    (6, "Texas", "US")
  )
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

  test("watchdog with scan maxPartitions -- dynamic partition pruning") {
    val s = spark
    import s.implicits._
    withTempDir { dir =>
      factData.toDF("date_id", "store_id", "product_id", "units_sold")
        .write
        .partitionBy("store_id")
        .save(s"${dir.getCanonicalPath}/fact_sk")
      spark.read.load(s"${dir.getCanonicalPath}/fact_sk").createOrReplaceTempView("fact_sk")

      storeData.toDF("store_id", "state_province", "country")
        .write
        .save(s"${dir.getCanonicalPath}/dim_store")
      spark.read.load(s"${dir.getCanonicalPath}/dim_store").createOrReplaceTempView("dim_store")

      withSQLConf(KyuubiSQLConf.WATCHDOG_MAX_PARTITIONS.key -> "5") {
        val query =
          """
            |SELECT f.date_id, f.store_id FROM fact_sk f
            |JOIN dim_store s ON f.store_id = s.store_id AND s.country = 'NL'
            |""".stripMargin
        intercept[MaxPartitionExceedException](sql(query).queryExecution.sparkPlan)
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
               |""".stripMargin).queryExecution.analyzed.isInstanceOf[GlobalLimit])
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
                 |""".stripMargin).queryExecution.analyzed.maxRows.contains(expected))
          }
        }
      }
    }
  }

  test("test watchdog: SELECT ... WITH AGGREGATE STATEMENT ") {

    withSQLConf(KyuubiSQLConf.WATCHDOG_FORCED_MAXOUTPUTROWS.key -> "10") {

      assert(!sql("SELECT count(*) FROM t1")
        .queryExecution.analyzed.isInstanceOf[GlobalLimit])

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
               |""".stripMargin).queryExecution.analyzed.isInstanceOf[GlobalLimit])
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
                 |""".stripMargin).queryExecution.analyzed.maxRows.contains(expected))
          }
        }
      }
    }
  }

  test("test watchdog: SELECT with CTE forceMaxOutputRows") {

    withSQLConf(KyuubiSQLConf.WATCHDOG_FORCED_MAXOUTPUTROWS.key -> "10") {

      val sorts = List("", "ORDER BY c1", "ORDER BY c2")

      sorts.foreach { sort =>
        assert(sql(
          s"""
             |WITH custom_cte AS (
             |SELECT * FROM t1
             |)
             |SELECT *
             |FROM custom_cte
             |$sort
             |""".stripMargin).queryExecution.analyzed.isInstanceOf[GlobalLimit])
      }

      limitAndExpecteds.foreach { case LimitAndExpected(limit, expected) =>
        sorts.foreach { sort =>
          assert(sql(
            s"""
               |WITH custom_cte AS (
               |SELECT * FROM t1
               |)
               |SELECT *
               |FROM custom_cte
               |$sort
               |LIMIT $limit
               |""".stripMargin).queryExecution.analyzed.maxRows.contains(expected))
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
               |""".stripMargin).queryExecution.analyzed.isInstanceOf[GlobalLimit])
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
                 |""".stripMargin).queryExecution.analyzed.maxRows.contains(expected))
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
          .queryExecution.analyzed.isInstanceOf[GlobalLimit])
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
              .queryExecution.analyzed.isInstanceOf[GlobalLimit])
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
          .queryExecution.analyzed.maxRows.contains(expected))
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
            .queryExecution.analyzed.isInstanceOf[GlobalLimit])

          assert(sql(
            s"""
               |SELECT * FROM
               |tmp_view
               |""".stripMargin)
            .queryExecution.analyzed.maxRows.contains(3))

          assert(sql(
            s"""
               |SELECT * FROM
               |tmp_view
               |limit 11
               |""".stripMargin)
            .queryExecution.analyzed.maxRows.contains(3))

          assert(sql(
            s"""
               |SELECT * FROM
               |(select * from tmp_view
               |UNION
               |select * from tmp_view2)
               |ORDER BY a
               |DESC
               |""".stripMargin)
            .collect().head.get(0).equals(10))
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
          .queryExecution.analyzed.isInstanceOf[GlobalLimit])
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
          .queryExecution.analyzed.isInstanceOf[GlobalLimit])
      }
    }
  }
}
