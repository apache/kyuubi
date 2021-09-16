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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Expression, ExpressionEvalHelper, Literal, NullsLast, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project, Sort}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable, OptimizedCreateHiveTableAsSelectCommand}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._

import org.apache.kyuubi.sql.{KyuubiSQLConf, KyuubiSQLExtensionException}
import org.apache.kyuubi.sql.zorder.Zorder

trait ZorderSuite extends QueryTest
  with SQLTestUtils
  with AdaptiveSparkPlanHelper
  with ExpressionEvalHelper {

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
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    if (_spark != null) {
      _spark.stop()
    }
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

        val e = intercept[KyuubiSQLExtensionException] {
          sql("OPTIMIZE up WHERE c1 > 1 ZORDER BY c1, c2")
        }
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

        val e = intercept[KyuubiSQLExtensionException](
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

  test("optimize zorder with datasource table") {
    // TODO remove this if we support datasource table
    withTable("t") {
      sql("CREATE TABLE t (c1 int, c2 int) USING PARQUET")
      val msg = intercept[KyuubiSQLExtensionException] {
        sql("OPTIMIZE t ZORDER BY c1, c2")
      }.getMessage
      assert(msg.contains("only support hive table"))
    }
  }

  private def checkZorderTable(
      enabled: Boolean,
      cols: String,
      planHasRepartition: Boolean,
      resHasSort: Boolean): Unit = {
    def checkSort(plan: LogicalPlan): Unit = {
      assert(plan.isInstanceOf[Sort] === resHasSort)
      if (plan.isInstanceOf[Sort]) {
        val refs = plan.asInstanceOf[Sort].order.head
          .child.asInstanceOf[Zorder].children.map(_.references.head)
        val colArr = cols.split(",")
        assert(refs.size === colArr.size)
        refs.zip(colArr).foreach { case (ref, col) =>
          assert(ref.name === col.trim)
        }
      }
    }

    val repartition = if (planHasRepartition) {
      "/*+ repartition */"
    } else {
      ""
    }
    withSQLConf("spark.sql.shuffle.partitions" -> "1") {
      // hive
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "false") {
        withTable("zorder_t1", "zorder_t2_true", "zorder_t2_false") {
          sql(
            s"""
               |CREATE TABLE zorder_t1 (c1 int, c2 string, c3 long, c4 double) STORED AS PARQUET
               |TBLPROPERTIES (
               | 'kyuubi.zorder.enabled' = '$enabled',
               | 'kyuubi.zorder.cols' = '$cols')
               |""".stripMargin)
          val df1 = sql(s"""
                           |INSERT INTO TABLE zorder_t1
                           |SELECT $repartition * FROM VALUES(1,'a',2,4D),(2,'b',3,6D)
                           |""".stripMargin)
          assert(df1.queryExecution.analyzed.isInstanceOf[InsertIntoHiveTable])
          checkSort(df1.queryExecution.analyzed.children.head)

          Seq("true", "false").foreach { optimized =>
            withSQLConf("spark.sql.hive.convertMetastoreCtas" -> optimized,
              "spark.sql.hive.convertMetastoreParquet" -> optimized) {
              val df2 =
                sql(
                  s"""
                     |CREATE TABLE zorder_t2_$optimized STORED AS PARQUET
                     |TBLPROPERTIES (
                     | 'kyuubi.zorder.enabled' = '$enabled',
                     | 'kyuubi.zorder.cols' = '$cols')
                     |
                     |SELECT $repartition * FROM
                     |VALUES(1,'a',2,4D),(2,'b',3,6D) AS t(c1 ,c2 , c3, c4)
                     |""".stripMargin)
              if (optimized.toBoolean) {
                assert(df2.queryExecution.analyzed
                  .isInstanceOf[OptimizedCreateHiveTableAsSelectCommand])
              } else {
                assert(df2.queryExecution.analyzed.isInstanceOf[CreateHiveTableAsSelectCommand])
              }
              checkSort(df2.queryExecution.analyzed.children.head)
            }
          }
        }
      }

      // datasource
      withTable("zorder_t3", "zorder_t4") {
        sql(
          s"""
             |CREATE TABLE zorder_t3 (c1 int, c2 string, c3 long, c4 double) USING PARQUET
             |TBLPROPERTIES (
             | 'kyuubi.zorder.enabled' = '$enabled',
             | 'kyuubi.zorder.cols' = '$cols')
             |""".stripMargin)
        val df1 = sql(s"""
                         |INSERT INTO TABLE zorder_t3
                         |SELECT $repartition * FROM VALUES(1,'a',2,4D),(2,'b',3,6D)
                         |""".stripMargin)
        assert(df1.queryExecution.analyzed.isInstanceOf[InsertIntoHadoopFsRelationCommand])
        checkSort(df1.queryExecution.analyzed.children.head)

        val df2 =
          sql(
            s"""
               |CREATE TABLE zorder_t4 USING PARQUET
               |TBLPROPERTIES (
               | 'kyuubi.zorder.enabled' = '$enabled',
               | 'kyuubi.zorder.cols' = '$cols')
               |
               |SELECT $repartition * FROM
               |VALUES(1,'a',2,4D),(2,'b',3,6D) AS t(c1 ,c2 , c3, c4)
               |""".stripMargin)
        assert(df2.queryExecution.analyzed.isInstanceOf[CreateDataSourceTableAsSelectCommand])
        checkSort(df2.queryExecution.analyzed.children.head)
      }
    }
  }

  test("Support insert zorder by table properties") {
    withSQLConf(KyuubiSQLConf.INSERT_ZORDER_BEFORE_WRITING.key -> "false") {
      checkZorderTable(true, "c1", false, false)
      checkZorderTable(false, "c1", false, false)
    }
    withSQLConf(KyuubiSQLConf.INSERT_ZORDER_BEFORE_WRITING.key -> "true") {
      checkZorderTable(true, "", false, false)
      checkZorderTable(true, "c5", false, false)
      checkZorderTable(true, "c1,c5", false, false)
      checkZorderTable(false, "c3", false, false)
      checkZorderTable(true, "c3", true, false)
      checkZorderTable(true, "c3", false, true)
      checkZorderTable(true, "c2,c4", false, true)
      checkZorderTable(true, "c4, c2, c1, c3", false, true)
    }
  }

  test("zorder: check unsupported data type") {
    def checkZorderPlan(zorder: Expression): Unit = {
      val msg = intercept[AnalysisException] {
        val plan = Project(Seq(Alias(zorder, "c")()), OneRowRelation())
        spark.sessionState.analyzer.checkAnalysis(plan)
      }.getMessage
      assert(msg.contains("Unsupported z-order type: null"))
    }

    checkZorderPlan(Zorder(Seq(Literal(null, NullType))))
    checkZorderPlan(Zorder(Seq(Literal(1, IntegerType), Literal(null, NullType))))
  }

  test("zorder: check supported data type") {
    val children = Seq(
      Literal.create(false, BooleanType),
      Literal.create(null, BooleanType),
      Literal.create(1.toByte, ByteType),
      Literal.create(null, ByteType),
      Literal.create(1.toShort, ShortType),
      Literal.create(null, ShortType),
      Literal.create(1, IntegerType),
      Literal.create(null, IntegerType),
      Literal.create(1L, LongType),
      Literal.create(null, LongType),
      Literal.create(1f, FloatType),
      Literal.create(null, FloatType),
      Literal.create(1d, DoubleType),
      Literal.create(null, DoubleType),
      Literal.create("1", StringType),
      Literal.create(null, StringType),
      Literal.create(1L, TimestampType),
      Literal.create(null, TimestampType),
      Literal.create(1, DateType),
      Literal.create(null, DateType),
      Literal.create(BigDecimal(1, 1), DecimalType(1, 1)),
      Literal.create(null, DecimalType(1, 1))
    )
    val zorder = Zorder(children)
    val plan = Project(Seq(Alias(zorder, "c")()), OneRowRelation())
    spark.sessionState.analyzer.checkAnalysis(plan)
    assert(zorder.foldable)
    val expected = Array(
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x50, 0x54, 0x15, 0x05, 0x49, 0x51, 0x54, 0x55, 0x15, 0x45,
      0x51, 0x54, 0x55, 0x15, 0x45, 0x51, 0x54, 0x55, 0x15, 0x45,
      0x51, 0x54, 0x55, 0x15, 0x45, 0x51, 0x54, 0x55, 0x15, 0x45,
      0x44, 0x44, 0x22, 0x22, 0x08, 0x88, 0x82, 0x22, 0x20, 0x88,
      0x88, 0x22, 0x22, 0x08, 0x88, 0x82, 0x22, 0x20, 0xa8, 0x9a,
      0x2a, 0x2a, 0x8a, 0x8a, 0xa2, 0xa2, 0xa8, 0xa8, 0xaa, 0x2a,
      0x2a, 0x8a, 0x8a, 0xa2, 0xa2, 0xa8, 0xa8, 0xaa, 0xca, 0x8a,
      0xa8, 0xa8, 0xaa, 0x8a, 0x8a, 0xa8, 0xa8, 0xaa, 0x8a, 0x8a,
      0xa8, 0xa8, 0xaa, 0x8a, 0x8a, 0xa8, 0xa8, 0xaa, 0xb2, 0xa2,
      0xaa, 0x8a, 0x9a, 0xaa, 0x2a, 0x6a, 0xa8, 0xa8, 0xaa, 0xa2,
      0xa2, 0xaa, 0x8a, 0x8a, 0xaa, 0x2f, 0x6b, 0xfc)
      .map(_.toByte)
    checkEvaluation(zorder, expected, InternalRow.fromSeq(children))
  }

  test("sort with zorder -- int column") {
    // TODO: add more datatype unit test
    def checkSort(input: DataFrame, expected: Seq[Row]): Unit = {
      withTempDir { dir =>
        input.write.mode("overwrite").format("json").save(dir.getCanonicalPath)
        val df = spark.read.format("json")
          .load(dir.getCanonicalPath)
          .repartition(1)
        val exprs = Seq("c1", "c2").map(col).map(_.expr)
        val sortOrder = SortOrder(Zorder(exprs), Ascending, NullsLast, Seq.empty)
        val zorderSort = Sort(Seq(sortOrder), true, df.logicalPlan)
        val result = Dataset.ofRows(spark, zorderSort)
        checkAnswer(result, expected)
      }
    }
    // generate 4 * 4 matrix
    val len = 3
    val input = spark.range(len + 1)
      .selectExpr("id as c1", s"explode(sequence(0, $len)) as c2")
      .repartition(3)
    val expected =
      (Row(0, 3), 10) :: (Row(1, 3), 11) :: (Row(2, 3), 14) :: (Row(3, 3), 15) ::
        (Row(0, 2), 8) :: (Row(1, 2), 9) :: (Row(2, 2), 12) :: (Row(3, 2), 13) ::
        (Row(0, 1), 2) :: (Row(1, 1), 3) :: (Row(2, 1), 6) :: (Row(3, 1), 7) ::
        (Row(0, 0), 0) :: (Row(1, 0), 1) :: (Row(2, 0), 4) :: (Row(3, 0), 5) :: Nil
    val sortedExpected = expected.sortBy {
      case (_, index) => index
    }.map {
      case (row, _) => row
    }
    checkSort(input, sortedExpected)

    // contains null value case.
    val session = spark
    import session.implicits._
    val input2 = spark.range(len)
      .union(sql("select null").as[java.lang.Long])
      .selectExpr("id as c1", s"explode(concat(sequence(0, $len - 1), array(null))) as c2")
      .repartition(3)
    val expected2 = Row(null, null) :: Row(0, null) :: Row(1, null) :: Row(2, null) ::
      Row(null, 0) :: Row(null, 1) :: Row(null, 2) :: Row(0, 0) ::
      Row(1, 0) :: Row(0, 1) :: Row(1, 1) :: Row(2, 0) ::
      Row(2, 1) :: Row(0, 2) :: Row(1, 2) :: Row(2, 2) :: Nil
    checkSort(input2, expected2)
  }

  def sparkConf(): SparkConf
}

class ZorderWithCodegenEnabledSuite extends ZorderSuite {
  override def sparkConf(): SparkConf = {
    val conf = new SparkConf()
    conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    conf
  }
}

class ZorderWithCodegenDisabledSuite extends ZorderSuite {
  override def sparkConf(): SparkConf = {
    val conf = new SparkConf()
    conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
    conf.set(SQLConf.CODEGEN_FACTORY_MODE.key, "NO_CODEGEN")
    conf
  }
}
