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
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.execution.OptimizedCreateHiveTableAsSelectCommand
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SQLTestData.TestData
import org.apache.spark.sql.test.SQLTestUtils

import org.apache.kyuubi.sql.{FinalStageConfigIsolation, KyuubiSQLConf}
import org.apache.kyuubi.sql.zorder.ZorderException

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
      .config("spark.hadoop.hive.metastore.client.capability.check", "false")
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

        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 $storage PARTITIONED BY(c2) AS " +
            s"SELECT * FROM VALUES(1, 'a'),(2, 'b') AS t(c1, c2) DISTRIBUTE BY rand()")
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
      "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "1",
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
            |SELECT /*+ REPARTITION */ t0.c2 FROM (
            |SELECT t1.c1, (count(*) + c1) as c2 FROM t1 GROUP BY t1.c1
            |) t0 JOIN (
            |SELECT t1.c1, (count(*) + c1) as c2 FROM t1 GROUP BY t1.c1
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

  test("OptimizedCreateHiveTableAsSelectCommand") {
    withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> "true",
      HiveUtils.CONVERT_METASTORE_CTAS.key -> "true") {
      withTable("t") {
        val df = sql(s"CREATE TABLE t STORED AS parquet AS SELECT 1 as a")
        val ctas = df.queryExecution.analyzed.collect {
          case _: OptimizedCreateHiveTableAsSelectCommand => true
        }
        assert(ctas.size == 1)
        val repartition = df.queryExecution.analyzed.collect {
          case _: RepartitionByExpression => true
        }
        assert(repartition.size == 1)
      }
    }
  }

  test("Sql classification for ddl") {
    withSQLConf(KyuubiSQLConf.SQL_CLASSIFICATION_ENABLED.key -> "true") {
      withDatabase("inventory") {
        val df = sql("CREATE DATABASE inventory;")
        assert(df.sparkSession.conf.get("kyuubi.spark.sql.classification") === "ddl")
      }
      val df = sql("select timestamp'2021-06-01'")
      assert(df.sparkSession.conf.get("kyuubi.spark.sql.classification") !== "ddl")
    }
  }

  test("Sql classification for dml") {
    withSQLConf(KyuubiSQLConf.SQL_CLASSIFICATION_ENABLED.key -> "true") {
      val df01 = sql("CREATE TABLE IF NOT EXISTS students " +
        "(name VARCHAR(64), address VARCHAR(64)) " +
        "USING PARQUET PARTITIONED BY (student_id INT);")
      assert(df01.sparkSession.conf.get("kyuubi.spark.sql.classification") === "ddl")

      val sql02 = "INSERT INTO students VALUES ('Amy Smith', '123 Park Ave, San Jose', 111111);"
      val df02 = sql(sql02)

      // scalastyle:off println
      println("the query execution is :" + spark.sessionState.executePlan(
        spark.sessionState.sqlParser.parsePlan(sql02)).toString())
      // scalastyle:on println

      assert(df02.sparkSession.conf.get("kyuubi.spark.sql.classification") === "dml")
    }
  }

  test("get simple name for DDL") {

    import scala.collection.mutable.Set

    val ddlSimpleName: Set[String] = Set()

    // Notice: When we get Analyzed Logical Plan, the field of LogicalPlan.analyzed.analyzed is true

    // ALTER DATABASE
    val sql = "CREATE DATABASE inventory;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql)
      ).getClass.getSimpleName
    )
    val sql02 = "ALTER DATABASE inventory SET DBPROPERTIES " +
      "('Edited-by' = 'John', 'Edit-date' = '01/01/2001');"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql02)
      ).getClass.getSimpleName
    )

    // ALTER TABLE RENAME
    val sql03 = "CREATE TABLE student (name VARCHAR(64), rollno INT, age INT) " +
      "USING PARQUET PARTITIONED BY (age);"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql03)
      ).getClass.getSimpleName
    )
    val sql04 = "INSERT INTO student VALUES " +
      "('zhang', 1, 10),('yu', 2, 11),('xiang', 3, 12),('zhangyuxiang', 4, 17);"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql04)
      ).getClass.getSimpleName
    )
    val sql05 = "ALTER TABLE Student RENAME TO StudentInfo;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql05)
      ).getClass.getSimpleName
    )

    // ALTER TABLE RENAME PARTITION
    val sql06 = "ALTER TABLE default.StudentInfo PARTITION (age='10') " +
      "RENAME TO PARTITION (age='15');"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql06)
      ).getClass.getSimpleName
    )

    var pre_sql = "CREATE TABLE IF NOT EXISTS StudentInfo " +
      "(name VARCHAR(64), rollno INT, age INT) USING PARQUET PARTITIONED BY (age);"
    spark.sql(pre_sql)
    // ALTER TABLE ADD COLUMNS
    val sql07 = "ALTER TABLE StudentInfo ADD columns (LastName string, DOB timestamp);"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql07)
      ).getClass.getSimpleName
    )

    // ALTER TABLE ALTER COLUMN
    val sql08 = "ALTER TABLE StudentInfo ALTER COLUMN age COMMENT \"new comment\";"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql08)
      ).getClass.getSimpleName
    )

    // ALTER TABLE CHANGE COLUMN
    val sql09 = "ALTER TABLE StudentInfo CHANGE COLUMN age COMMENT \"new comment123\";"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql09)
      ).getClass.getSimpleName
    )

    // ALTER TABLE ADD PARTITION
    val sql10 = "ALTER TABLE StudentInfo ADD IF NOT EXISTS PARTITION (age=18);"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql10)
      ).getClass.getSimpleName
    )

    // ALTER TABLE DROP PARTITION
    val sql11 = "ALTER TABLE StudentInfo DROP IF EXISTS PARTITION (age=18);"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql11)
      ).getClass.getSimpleName
    )

    // CREAT VIEW
    val sql12 = "CREATE OR REPLACE VIEW studentinfo_view " +
      "AS SELECT name, rollno FROM studentinfo;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql12)
      ).getClass.getSimpleName
    )

    // ALTER VIEW RENAME TO
    val sql13 = "ALTER VIEW studentinfo_view RENAME TO studentinfo_view2;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql13)
      ).getClass.getSimpleName
    )

    // ALTER VIEW SET TBLPROPERTIES
    val sql14 = "ALTER VIEW studentinfo_view2 SET TBLPROPERTIES " +
      "('created.by.user' = \"zhangyuxiang\", 'created.date' = '08-20-2021' );"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql14)
      ).getClass.getSimpleName
    )

    // ALTER VIEW UNSET TBLPROPERTIES
    val sql15 = "ALTER VIEW studentinfo_view2 UNSET TBLPROPERTIES " +
      "('created.by.user', 'created.date');"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql15)
      ).getClass.getSimpleName
    )

    // ALTER VIEW AS SELECT
    val sql16 = "ALTER VIEW studentinfo_view2 AS SELECT * FROM studentinfo;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql16)
      ).getClass.getSimpleName
    )

    // CREATE DATASOURCE TABLE AS SELECT
    val sql17 = "CREATE TABLE student_copy USING CSV AS SELECT * FROM studentinfo;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql17)
      ).getClass.getSimpleName
    )

    // CREATE DATASOURCE TABLE AS LIKE
    val sql18 = "CREATE TABLE Student_Dupli like studentinfo;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql18)
      ).getClass.getSimpleName
    )

    // USE DATABASE
    val sql26 = "USE default;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql26)
      ).getClass.getSimpleName
    )

    // DROP DATABASE
    val sql19 = "DROP DATABASE IF EXISTS inventory_db CASCADE;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql19)
      ).getClass.getSimpleName
    )

    // CREATE FUNCTION
    val sql20 = "CREATE FUNCTION test_avg AS " +
      "'org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage';"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql20)
      ).getClass.getSimpleName
    )

    // DROP FUNCTION
    val sql21 = "DROP FUNCTION test_avg;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql21)
      ).getClass.getSimpleName
    )

    spark.sql("CREATE TABLE IF NOT EXISTS studentabc (name VARCHAR(64), rollno INT, age INT) " +
      "USING PARQUET PARTITIONED BY (age);")
    // DROP TABLE
    val sql22 = "DROP TABLE IF EXISTS studentabc;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql22)
      ).getClass.getSimpleName
    )

    // DROP VIEW
    val sql23 = "DROP VIEW studentinfo_view2;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql23)
      ).getClass.getSimpleName
    )

    // TRUNCATE TABLE
    val sql24 = "TRUNCATE TABLE StudentInfo;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql24)
      ).getClass.getSimpleName
    )

    // REPAIR TABLE
    val sql25 = "MSCK REPAIR TABLE StudentInfo;"
    ddlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql25)
      ).getClass.getSimpleName
    )
    // scalastyle:off println
    println("ddl simple name is :" + ddlSimpleName)
    // scalastyle:on println
  }

  test("get simple name for DML") {
    import scala.collection.mutable.Set
    val dmlSimpleName: Set[String] = Set()

    var pre_sql = "CREATE TABLE IF NOT EXISTS students (name VARCHAR(64), address VARCHAR(64)) " +
      "USING PARQUET PARTITIONED BY (student_id INT);"
    spark.sql(pre_sql)
    pre_sql = "CREATE TABLE IF NOT EXISTS PERSONS (name VARCHAR(64), address VARCHAR(64)) " +
      "USING PARQUET PARTITIONED BY (ssn INT);"
    spark.sql(pre_sql)
    pre_sql = "INSERT INTO persons VALUES " +
      "('Dora Williams', '134 Forest Ave, Menlo Park', 123456789), " +
      "('Eddie Davis', '245 Market St, Milpitas', 345678901);"
    spark.sql(pre_sql)
    pre_sql = "CREATE TABLE IF NOT EXISTS visiting_students " +
      "(name VARCHAR(64), address VARCHAR(64)) USING PARQUET PARTITIONED BY (student_id INT);"
    spark.sql(pre_sql)
    pre_sql = "CREATE TABLE IF NOT EXISTS applicants " +
      "(name VARCHAR(64), address VARCHAR(64), qualified BOOLEAN) " +
      "USING PARQUET PARTITIONED BY (student_id INT);"
    spark.sql(pre_sql)
    pre_sql = "INSERT INTO applicants VALUES " +
      "('Helen Davis', '469 Mission St, San Diego', true, 999999), " +
      "('Ivy King', '367 Leigh Ave, Santa Clara', false, 101010), " +
      "('Jason Wang', '908 Bird St, Saratoga', true, 121212);"
    spark.sql(pre_sql)

    val sql01 = "INSERT INTO students VALUES ('Amy Smith', '123 Park Ave, San Jose', 111111);"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql01)
      ).getClass.getSimpleName
    )

    val sql02 = "INSERT INTO students VALUES " +
      "('Bob Brown', '456 Taylor St, Cupertino', 222222), " +
      "('Cathy Johnson', '789 Race Ave, Palo Alto', 333333);"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql02)
      ).getClass.getSimpleName
    )

    val sql03 = "INSERT INTO students PARTITION (student_id = 444444) " +
      "SELECT name, address FROM persons WHERE name = \"Dora Williams\";"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql03)
      ).getClass.getSimpleName
    )

    val sql04 = "INSERT INTO students TABLE visiting_students;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql04)
      ).getClass.getSimpleName
    )

    val sql05 = "INSERT INTO students FROM applicants " +
      "SELECT name, address, student_id WHERE qualified = true;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql05)
      ).getClass.getSimpleName
    )

    val sql06 = "INSERT INTO students (address, name, student_id) " +
      "VALUES ('Hangzhou, China', 'Kent Yao', 11215016);"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql06)
      ).getClass.getSimpleName
    )

    val sql07 = "INSERT INTO students PARTITION (student_id = 11215017) " +
      "(address, name) VALUES ('Hangzhou, China', 'Kent Yao Jr.');"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql07)
      ).getClass.getSimpleName
    )

    val sql08 = "INSERT OVERWRITE students VALUES " +
      "('Ashua Hill', '456 Erica Ct, Cupertino', 111111), " +
      "('Brian Reed', '723 Kern Ave, Palo Alto', 222222);"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql08)
      ).getClass.getSimpleName
    )

    val sql09 = "INSERT OVERWRITE students PARTITION (student_id = 222222) " +
      "SELECT name, address FROM persons WHERE name = \"Dora Williams\";"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql09)
      ).getClass.getSimpleName
    )

    val sql10 = "INSERT OVERWRITE students TABLE visiting_students;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql10)
      ).getClass.getSimpleName
    )

    val sql11 = "INSERT OVERWRITE students FROM applicants " +
      "SELECT name, address, student_id WHERE qualified = true;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql11)
      ).getClass.getSimpleName
    )

    val sql12 = "INSERT OVERWRITE students (address, name, student_id) VALUES " +
      "('Hangzhou, China', 'Kent Yao', 11215016);"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql12)
      ).getClass.getSimpleName
    )

    val sql13 = "INSERT OVERWRITE students PARTITION (student_id = 11215016) " +
      "(address, name) VALUES ('Hangzhou, China', 'Kent Yao Jr.');"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql13)
      ).getClass.getSimpleName
    )

    val sql14 = "INSERT OVERWRITE DIRECTORY '/tmp/destination' " +
      "USING parquet OPTIONS (col1 1, col2 2, col3 'test') " +
      "SELECT * FROM students;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql14)
      ).getClass.getSimpleName
    )

    val sql15 = "INSERT OVERWRITE DIRECTORY " +
      "USING parquet " +
      "OPTIONS ('path' '/tmp/destination', col1 1, col2 2, col3 'test') " +
      "SELECT * FROM students;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql15)
      ).getClass.getSimpleName
    )

    val sql016 = "INSERT OVERWRITE LOCAL DIRECTORY '/tmp/destination' " +
      "STORED AS orc " +
      "SELECT * FROM students;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql016)
      ).getClass.getSimpleName
    )

    val sql017 = "INSERT OVERWRITE LOCAL DIRECTORY '/tmp/destination' " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
      "SELECT * FROM students;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql017)
      ).getClass.getSimpleName
    )

    pre_sql = "CREATE TABLE IF NOT EXISTS students_test " +
      "(name VARCHAR(64), address VARCHAR(64)) " +
      "USING PARQUET PARTITIONED BY (student_id INT) " +
      "LOCATION '/tmp/destination/';"
    spark.sql(pre_sql)
    pre_sql = "INSERT INTO students_test VALUES " +
      "('Bob Brown', '456 Taylor St, Cupertino', 222222), " +
      "('Cathy Johnson', '789 Race Ave, Palo Alto', 333333);"
    spark.sql(pre_sql)
    pre_sql = "CREATE TABLE IF NOT EXISTS test_load " +
      "(name VARCHAR(64), address VARCHAR(64), student_id INT) " +
      "USING HIVE;"
    spark.sql(pre_sql)

    val sql018 = "LOAD DATA LOCAL INPATH " +
      "'/tmp/destination/students_test' OVERWRITE INTO TABLE test_load;"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql018)
      ).getClass.getSimpleName
    )

    pre_sql = "CREATE TABLE IF NOT EXISTS test_partition " +
      "(c1 INT, c2 INT, c3 INT) PARTITIONED BY (c2, c3) " +
      "LOCATION '/tmp/destination/';"
    spark.sql(pre_sql)
    pre_sql = "INSERT INTO test_partition PARTITION (c2 = 2, c3 = 3) VALUES (1);"
    spark.sql(pre_sql)
    pre_sql = "INSERT INTO test_partition PARTITION (c2 = 5, c3 = 6) VALUES (4);"
    spark.sql(pre_sql)
    pre_sql = "INSERT INTO test_partition PARTITION (c2 = 8, c3 = 9) VALUES (7);"
    spark.sql(pre_sql)
    pre_sql = "CREATE TABLE IF NOT EXISTS test_load_partition " +
      "(c1 INT, c2 INT, c3 INT) USING HIVE PARTITIONED BY (c2, c3);"
    spark.sql(pre_sql)

    val sql019 = "LOAD DATA LOCAL INPATH '/tmp/destination/test_partition/c2=2/c3=3' " +
      "OVERWRITE INTO TABLE test_load_partition PARTITION (c2=2, c3=3);"
    dmlSimpleName.add(
      spark.sessionState.analyzer.execute(
        spark.sessionState.sqlParser.parsePlan(sql019)
      ).getClass.getSimpleName
    )
    // scalastyle:off println
    println("dml simple name is :" + dmlSimpleName)
    // scalastyle:on println
  }

  test("optimize unpartitioned table") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTable("up") {
        sql(s"DROP TABLE IF EXISTS up")

        val target = Seq(Seq(0, 0), Seq(1, 0), Seq(0, 1), Seq(1, 1),
          Seq(2, 0), Seq(3, 0), Seq(2, 1), Seq(3, 1),
          Seq(0, 2), Seq(1, 2), Seq(0, 3), Seq(1, 3),
          Seq(2, 2), Seq(3, 2), Seq(2, 3), Seq(3, 3))
        sql(s"CREATE TABLE up (c1 INT, c2 INT, c3 INT)")
        sql(s"INSERT INTO TABLE up VALUES" +
          "(0,0,2),(0,1,2),(0,2,1),(0,3,3)," +
          "(1,0,4),(1,1,2),(1,2,1),(1,3,3)," +
          "(2,0,2),(2,1,1),(2,2,5),(2,3,5)," +
          "(3,0,3),(3,1,4),(3,2,9),(3,3,0)")

        val e = intercept[ZorderException](sql("OPTIMIZE up WHERE c1 > 1 ZORDER BY c1, c2"))
        assert(e.getMessage == "Filters are only supported for partitioned table")

        sql("OPTIMIZE up ZORDER BY c1, c2")
        val res = sql("SELECT c1, c2 FROM up").collect()

        assert(res.length == 16)

        for (i <- target.indices) {
          val t = target(i)
          val r = res(i)
          assert(t(0) == r.getInt(0))
          assert(t(1) == r.getInt(1))
        }
      }
    }
  }

  test("optimize partitioned table") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTable("p") {
        sql("DROP TABLE IF EXISTS p")

        val target = Seq(Seq(0, 0), Seq(1, 0), Seq(0, 1), Seq(1, 1),
          Seq(2, 0), Seq(3, 0), Seq(2, 1), Seq(3, 1),
          Seq(0, 2), Seq(1, 2), Seq(0, 3), Seq(1, 3),
          Seq(2, 2), Seq(3, 2), Seq(2, 3), Seq(3, 3))

        sql(s"CREATE TABLE p (c1 INT, c2 INT, c3 INT) PARTITIONED BY (id INT)")
        sql(s"ALTER TABLE p ADD PARTITION (id = 1)")
        sql(s"ALTER TABLE p ADD PARTITION (id = 2)")
        sql(s"INSERT INTO TABLE p PARTITION (id = 1) VALUES" +
          "(0,0,2),(0,1,2),(0,2,1),(0,3,3)," +
          "(1,0,4),(1,1,2),(1,2,1),(1,3,3)," +
          "(2,0,2),(2,1,1),(2,2,5),(2,3,5)," +
          "(3,0,3),(3,1,4),(3,2,9),(3,3,0)")
        sql(s"INSERT INTO TABLE p PARTITION (id = 2) VALUES" +
          "(0,0,2),(0,1,2),(0,2,1),(0,3,3)," +
          "(1,0,4),(1,1,2),(1,2,1),(1,3,3)," +
          "(2,0,2),(2,1,1),(2,2,5),(2,3,5)," +
          "(3,0,3),(3,1,4),(3,2,9),(3,3,0)")

        sql(s"OPTIMIZE p ZORDER BY c1, c2")

        val res1 = sql(s"SELECT c1, c2 FROM p WHERE id = 1").collect()
        val res2 = sql(s"SELECT c1, c2 FROM p WHERE id = 2").collect()

        assert(res1.length == 16)
        assert(res2.length == 16)

        for (i <- target.indices) {
          val t = target(i)
          val r1 = res1(i)
          assert(t(0) == r1.getInt(0))
          assert(t(1) == r1.getInt(1))

          val r2 = res2(i)
          assert(t(0) == r2.getInt(0))
          assert(t(1) == r2.getInt(1))
        }
      }
    }
  }

  test("optimize partitioned table with filters") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      withTable("p") {
        sql("DROP TABLE IF EXISTS p")

        val target1 = Seq(Seq(0, 0), Seq(1, 0), Seq(0, 1), Seq(1, 1),
          Seq(2, 0), Seq(3, 0), Seq(2, 1), Seq(3, 1),
          Seq(0, 2), Seq(1, 2), Seq(0, 3), Seq(1, 3),
          Seq(2, 2), Seq(3, 2), Seq(2, 3), Seq(3, 3))
        val target2 = Seq(Seq(0, 0), Seq(0, 1), Seq(0, 2), Seq(0, 3),
          Seq(1, 0), Seq(1, 1), Seq(1, 2), Seq(1, 3),
          Seq(2, 0), Seq(2, 1), Seq(2, 2), Seq(2, 3),
          Seq(3, 0), Seq(3, 1), Seq(3, 2), Seq(3, 3))
        sql(s"CREATE TABLE p (c1 INT, c2 INT, c3 INT) PARTITIONED BY (id INT)")
        sql(s"ALTER TABLE p ADD PARTITION (id = 1)")
        sql(s"ALTER TABLE p ADD PARTITION (id = 2)")
        sql(s"INSERT INTO TABLE p PARTITION (id = 1) VALUES" +
          "(0,0,2),(0,1,2),(0,2,1),(0,3,3)," +
          "(1,0,4),(1,1,2),(1,2,1),(1,3,3)," +
          "(2,0,2),(2,1,1),(2,2,5),(2,3,5)," +
          "(3,0,3),(3,1,4),(3,2,9),(3,3,0)")
        sql(s"INSERT INTO TABLE p PARTITION (id = 2) VALUES" +
          "(0,0,2),(0,1,2),(0,2,1),(0,3,3)," +
          "(1,0,4),(1,1,2),(1,2,1),(1,3,3)," +
          "(2,0,2),(2,1,1),(2,2,5),(2,3,5)," +
          "(3,0,3),(3,1,4),(3,2,9),(3,3,0)")

        val e = intercept[ZorderException](
          sql(s"OPTIMIZE p WHERE id = 1 AND c1 > 1 ZORDER BY c1, c2")
        )
        assert(e.getMessage == "Only partition column filters are allowed")

        sql(s"OPTIMIZE p WHERE id = 1 ZORDER BY c1, c2")

        val res1 = sql(s"SELECT c1, c2 FROM p WHERE id = 1").collect()
        val res2 = sql(s"SELECT c1, c2 FROM p WHERE id = 2").collect()

        assert(res1.length == 16)
        assert(res2.length == 16)

        for (i <- target1.indices) {
          val t1 = target1(i)
          val r1 = res1(i)
          assert(t1(0) == r1.getInt(0))
          assert(t1(1) == r1.getInt(1))

          val t2 = target2(i)
          val r2 = res2(i)
          assert(t2(0) == r2.getInt(0))
          assert(t2(1) == r2.getInt(1))
        }
      }
    }
  }
}
