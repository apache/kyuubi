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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, CustomShuffleReaderExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeLike}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SQLTestData.TestData
import org.apache.spark.sql.test.SQLTestUtils

import org.apache.kyuubi.sql.{FinalStageConfigIsolation, KyuubiSQLConf}

class KyuubiExtensionSuite extends QueryTest with SQLTestUtils with AdaptiveSparkPlanHelper {

  var _spark: SparkSession = _
  override def spark: SparkSession = _spark

  protected override def beforeAll(): Unit = {
    _spark = SparkSession.builder()
      .master("local[1]")
      .config(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        "org.apache.kyuubi.sql.KyuubiSparkSQLExtension")
      .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.ui.enabled", "false")
      .enableHiveSupport()
      .getOrCreate()
    setupData()
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    cleanupData()
    if (_spark != null) {
      _spark.stop()
    }
  }

  private def setupData(): Unit = {
    val self = _spark
    import self.implicits._
    spark.sparkContext.parallelize(
      (1 to 100).map(i => TestData(i, i.toString)), 10)
      .toDF("c1", "c2").createOrReplaceTempView("t1")
    spark.sparkContext.parallelize(
      (1 to 10).map(i => TestData(i, i.toString)), 5)
      .toDF("c1", "c2").createOrReplaceTempView("t2")
    spark.sparkContext.parallelize(
      (1 to 50).map(i => TestData(i, i.toString)), 2)
      .toDF("c1", "c2").createOrReplaceTempView("t3")
  }

  private def cleanupData(): Unit = {
    spark.sql("DROP VIEW IF EXISTS t1")
    spark.sql("DROP VIEW IF EXISTS t2")
    spark.sql("DROP VIEW IF EXISTS t3")
  }

  test("check repartition exists") {
    def check(df: DataFrame): Unit = {
      assert(
        df.queryExecution.analyzed.collect {
          case r: RepartitionByExpression =>
            assert(r.optNumPartitions ===
              spark.sessionState.conf.getConf(KyuubiSQLConf.INSERT_REPARTITION_NUM))
            r
        }.size == 1
      )
    }

    // It's better to set config explicitly in case of we change the default value.
    withSQLConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true") {
      Seq("USING PARQUET", "").foreach { storage =>
        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 (c1 int) $storage PARTITIONED BY (c2 string)")
          check(sql("INSERT INTO TABLE tmp1 PARTITION(c2='a') " +
            "SELECT * FROM VALUES(1),(2) AS t(c1)"))
        }

        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 (c1 int) $storage")
          check(sql("INSERT INTO TABLE tmp1 SELECT * FROM VALUES(1),(2),(3) AS t(c1)"))
          check(sql("INSERT INTO TABLE tmp1 " +
            "SELECT * FROM VALUES(1),(2),(3) AS t(c1) DISTRIBUTE BY c1"))
        }

        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 $storage AS SELECT * FROM VALUES(1),(2),(3) AS t(c1)")
        }

        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 $storage PARTITIONED BY(c2) AS " +
            s"SELECT * FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2)")
        }
      }
    }
  }

  test("check repartition does not exists") {
    def check(df: DataFrame): Unit = {
      assert(
        df.queryExecution.analyzed.collect {
          case r: RepartitionByExpression => r
        }.isEmpty
      )
    }

    withSQLConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true") {
      // test no write command
      check(sql("SELECT * FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2)"))
      check(sql("SELECT count(*) FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2)"))

      // test not supported plan
      withTable("tmp1") {
        sql(s"CREATE TABLE tmp1 (c1 int) PARTITIONED BY (c2 string)")
        check(sql("INSERT INTO TABLE tmp1 PARTITION(c2) " +
          "SELECT /*+ repartition(10) */ * FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2)"))
        check(sql("INSERT INTO TABLE tmp1 PARTITION(c2) " +
          "SELECT * FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2) ORDER BY c1"))
        check(sql("INSERT INTO TABLE tmp1 PARTITION(c2) " +
          "SELECT * FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2) LIMIT 10"))
      }
    }

    withSQLConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "false") {
      Seq("USING PARQUET", "").foreach { storage =>
        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 (c1 int) $storage PARTITIONED BY (c2 string)")
          check(sql("INSERT INTO TABLE tmp1 PARTITION(c2) " +
            "SELECT * FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2)"))
        }

        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 (c1 int) $storage")
          check(sql("INSERT INTO TABLE tmp1 SELECT * FROM VALUES(1),(2),(3) AS t(c1)"))
        }

        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 $storage AS SELECT * FROM VALUES(1),(2),(3) AS t(c1)")
        }

        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 $storage PARTITIONED BY(c2) AS " +
            s"SELECT * FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2)")
        }
      }
    }
  }

  test("test dynamic partition write") {
    def checkRepartitionExpression(df: DataFrame): Unit = {
      assert(df.queryExecution.analyzed.collect {
        case r: RepartitionByExpression if r.partitionExpressions.size == 2 =>
          assert(r.partitionExpressions.head.asInstanceOf[Attribute].name === "c2")
          assert(r.partitionExpressions(1).asInstanceOf[Cast].child.asInstanceOf[Multiply]
            .left.asInstanceOf[Attribute].name.startsWith("_nondeterministic"))
          r
      }.size == 1)
    }

    withSQLConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true",
      KyuubiSQLConf.DYNAMIC_PARTITION_INSERTION_REPARTITION_NUM.key -> "2") {
      Seq("USING PARQUET", "").foreach { storage =>
        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 (c1 int) $storage PARTITIONED BY (c2 string)")
          checkRepartitionExpression(sql("INSERT INTO TABLE tmp1 SELECT 1 as c1, 'a' as c2 "))
        }

        withTable("tmp1") {
          checkRepartitionExpression(
            sql("CREATE TABLE tmp1 PARTITIONED BY(C2) SELECT 1 as c1, 'a' as c2 "))
        }
      }
    }
  }

  test("force shuffle before join") {
    def checkShuffleNodeNum(sqlString: String, num: Int): Unit = {
      var expectedResult: Seq[Row] = Seq.empty
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        expectedResult = sql(sqlString).collect()
      }
      val df = sql(sqlString)
      checkAnswer(df, expectedResult)
      assert(
        collect(df.queryExecution.executedPlan) {
          case shuffle: ShuffleExchangeLike
            if shuffle.shuffleOrigin == ENSURE_REQUIREMENTS => shuffle
        }.size == num)
    }

    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      KyuubiSQLConf.FORCE_SHUFFLE_BEFORE_JOIN.key -> "true") {
      Seq("SHUFFLE_HASH", "MERGE").foreach { joinHint =>
        // positive case
        checkShuffleNodeNum(
          s"""
            |SELECT /*+ $joinHint(t2, t3) */ t1.c1, t1.c2, t2.c1, t3.c1 from t1
            | JOIN t2 ON t1.c1 = t2.c1
            | JOIN t3 ON t1.c1 = t3.c1
            | """.stripMargin, 4)

        // negative case
        checkShuffleNodeNum(
          s"""
             |SELECT /*+ $joinHint(t2, t3) */ t1.c1, t1.c2, t2.c1, t3.c1 from t1
             | JOIN t2 ON t1.c1 = t2.c1
             | JOIN t3 ON t1.c2 = t3.c2
             | """.stripMargin, 4)
      }

      checkShuffleNodeNum(
        """
          |SELECT t1.c1, t2.c1, t3.c2 from t1
          | JOIN t2 ON t1.c1 = t2.c1
          | JOIN (
          |  SELECT c2, count(*) FROM t1 GROUP BY c2
          | ) t3 ON t1.c1 = t3.c2
          | """.stripMargin, 5)

      checkShuffleNodeNum(
        """
          |SELECT t1.c1, t2.c1, t3.c1 from t1
          | JOIN t2 ON t1.c1 = t2.c1
          | JOIN (
          |  SELECT c1, count(*) FROM t1 GROUP BY c1
          | ) t3 ON t1.c1 = t3.c1
          | """.stripMargin, 5)
    }
  }

  test("final stage config set reset check") {
    withSQLConf(KyuubiSQLConf.FINAL_STAGE_CONFIG_ISOLATION.key -> "true",
      "spark.sql.finalStage.adaptive.coalescePartitions.minPartitionNum" -> "1",
      "spark.sql.finalStage.adaptive.advisoryPartitionSizeInBytes" -> "100") {
      // use loop to double check final stage config doesn't affect the sql query each other
      (1 to 3).foreach { _ =>
        sql("SELECT COUNT(*) FROM VALUES(1) as t(c)").collect()
        assert(spark.sessionState.conf.getConfString(
          "spark.sql.previousStage.adaptive.coalescePartitions.minPartitionNum") ===
          FinalStageConfigIsolation.INTERNAL_UNSET_CONFIG_TAG)
        assert(spark.sessionState.conf.getConfString(
          "spark.sql.adaptive.coalescePartitions.minPartitionNum") ===
          "1")
        assert(spark.sessionState.conf.getConfString(
          "spark.sql.finalStage.adaptive.coalescePartitions.minPartitionNum") ===
          "1")

        // 64MB
        assert(spark.sessionState.conf.getConfString(
          "spark.sql.previousStage.adaptive.advisoryPartitionSizeInBytes") ===
          "67108864b")
        assert(spark.sessionState.conf.getConfString(
          "spark.sql.adaptive.advisoryPartitionSizeInBytes") ===
          "100")
        assert(spark.sessionState.conf.getConfString(
          "spark.sql.finalStage.adaptive.advisoryPartitionSizeInBytes") ===
          "100")
      }

      sql("SET spark.sql.adaptive.advisoryPartitionSizeInBytes=1")
      assert(spark.sessionState.conf.getConfString(
        "spark.sql.adaptive.advisoryPartitionSizeInBytes") ===
        "1")
      assert(!spark.sessionState.conf.contains(
        "spark.sql.previousStage.adaptive.advisoryPartitionSizeInBytes"))

      sql("SET a=1")
      assert(spark.sessionState.conf.getConfString("a") === "1")

      sql("RESET spark.sql.adaptive.coalescePartitions.minPartitionNum")
      assert(!spark.sessionState.conf.contains(
        "spark.sql.adaptive.coalescePartitions.minPartitionNum"))
      assert(!spark.sessionState.conf.contains(
        "spark.sql.previousStage.adaptive.coalescePartitions.minPartitionNum"))

      sql("RESET a")
      assert(!spark.sessionState.conf.contains("a"))
    }
  }

  test("final stage config isolation") {
    def checkPartitionNum(
        sqlString: String, previousPartitionNum: Int, finalPartitionNum: Int): Unit = {
      val df = sql(sqlString)
      df.collect()
      val shuffleReaders = collect(df.queryExecution.executedPlan) {
        case customShuffleReader: CustomShuffleReaderExec => customShuffleReader
      }
      assert(shuffleReaders.nonEmpty)
      // reorder stage by stage id to ensure we get the right stage
      val sortedShuffleReaders = shuffleReaders.sortWith {
        case (s1, s2) =>
          s1.child.asInstanceOf[QueryStageExec].id < s2.child.asInstanceOf[QueryStageExec].id
      }
      if (sortedShuffleReaders.length > 1) {
        assert(sortedShuffleReaders.head.partitionSpecs.length === previousPartitionNum)
      }
      assert(sortedShuffleReaders.last.partitionSpecs.length === finalPartitionNum)
      assert(df.rdd.partitions.length === finalPartitionNum)
    }

    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1",
      SQLConf.SHUFFLE_PARTITIONS.key -> "3",
      KyuubiSQLConf.FINAL_STAGE_CONFIG_ISOLATION.key -> "true",
      "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "1",
      "spark.sql.finalStage.adaptive.advisoryPartitionSizeInBytes" -> "10000000") {

      // use loop to double check final stage config doesn't affect the sql query each other
      (1 to 3).foreach { _ =>
        checkPartitionNum(
          "SELECT c1, count(*) FROM t1 GROUP BY c1",
          1,
          1)

        checkPartitionNum(
          "SELECT c2, count(*) FROM (SELECT c1, count(*) as c2 FROM t1 GROUP BY c1) GROUP BY c2",
          3,
          1)

        checkPartitionNum(
          "SELECT t1.c1, count(*) FROM t1 JOIN t2 ON t1.c2 = t2.c2 GROUP BY t1.c1",
          3,
          1)

        checkPartitionNum(
          """
            | SELECT /*+ REPARTITION */
            | t1.c1, count(*) FROM t1
            | JOIN t2 ON t1.c2 = t2.c2
            | JOIN t3 ON t1.c1 = t3.c1
            | GROUP BY t1.c1
            |""".stripMargin,
          3,
          1)

        // one shuffle reader
        checkPartitionNum(
          """
            | SELECT /*+ BROADCAST(t1) */
            | t1.c1, t2.c2 FROM t1
            | JOIN t2 ON t1.c2 = t2.c2
            | DISTRIBUTE BY c1
            |""".stripMargin,
          1,
          1)

        // test ReusedExchange
        checkPartitionNum(
          """
            |SELECT t0.c2 FROM (
            |SELECT t1.c1, count(*) as c2 FROM t1 GROUP BY t1.c1
            |) t0 JOIN (
            |SELECT t1.c1, count(*) as c2 FROM t1 GROUP BY t1.c1
            |) t1 ON t0.c2 = t1.c2
            |""".stripMargin,
          3,
          1
        )

        // one shuffle reader
        checkPartitionNum(
          """
            |SELECT t0.c1 FROM (
            |SELECT t1.c1 FROM t1 GROUP BY t1.c1
            |) t0 JOIN (
            |SELECT t1.c1 FROM t1 GROUP BY t1.c1
            |) t1 ON t0.c1 = t1.c1
            |""".stripMargin,
          1,
          1
        )
      }
    }
  }
}
