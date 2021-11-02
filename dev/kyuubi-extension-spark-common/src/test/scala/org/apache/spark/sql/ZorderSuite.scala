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
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, AttributeReference, Expression, ExpressionEvalHelper, Literal, NullsLast, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation, Project, Sort}
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, InsertIntoHiveTable, OptimizedCreateHiveTableAsSelectCommand}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types._

import org.apache.kyuubi.sql.{KyuubiSQLConf, KyuubiSQLExtensionException}
import org.apache.kyuubi.sql.zorder.{OptimizeZorderCommandBase, Zorder}

trait ZorderSuite extends KyuubiSparkSQLExtensionTest with ExpressionEvalHelper {
  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        "org.apache.kyuubi.sql.KyuubiSparkSQLCommonExtension")
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
        val colArr = cols.split(",")
        val refs = if (colArr.length == 1) {
          plan.asInstanceOf[Sort].order.head
            .child.asInstanceOf[AttributeReference] :: Nil
        } else {
          plan.asInstanceOf[Sort].order.head
            .child.asInstanceOf[Zorder].children.map(_.references.head)
        }
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
      // before Spark 3.2.0 the null type catalog string is null, after Spark 3.2.0 it's void
      // see https://github.com/apache/spark/pull/33437
      assert(msg.contains("Unsupported z-order type:") &&
        (msg.contains("null") || msg.contains("void")))
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

//    // scalastyle:off
//    val resultGen = org.apache.commons.codec.binary.Hex.encodeHex(
//      zorder.eval(InternalRow.fromSeq(children)).asInstanceOf[Array[Byte]], false)
//    resultGen.grouped(2).zipWithIndex.foreach { case (char, i) =>
//      print("0x" + char(0) + char(1) + ", ")
//      if ((i + 1) % 10 == 0) {
//        println()
//      }
//    }
//    // scalastyle:on

    val expected = Array(
      0xFB, 0xEA, 0xAA, 0xBA, 0xAE, 0xAB, 0xAA, 0xEA, 0xBA, 0xAE,
      0xAB, 0xAA, 0xEA, 0xBA, 0xA6, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
      0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
      0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
      0xBA, 0xBB, 0xAA, 0xAA, 0xAA, 0xBA, 0xAA, 0xBA, 0xAA, 0xBA,
      0xAA, 0xBA, 0xAA, 0xBA, 0xAA, 0xBA, 0xAA, 0x9A, 0xAA, 0xAA,
      0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
      0xAA, 0xAA, 0xAA, 0xAA, 0xEA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
      0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
      0xAA, 0xAA, 0xBE, 0xAA, 0xAA, 0x8A, 0xBA, 0xAA, 0x2A, 0xEA,
      0xA8, 0xAA, 0xAA, 0xA2, 0xAA, 0xAA, 0x8A, 0xAA, 0xAA, 0x2F,
      0xEB, 0xFE)
      .map(_.toByte)
    checkEvaluation(zorder, expected, InternalRow.fromSeq(children))
  }

  private def checkSort(input: DataFrame, expected: Seq[Row], dataType: Array[DataType]): Unit = {
    withTempDir { dir =>
      input.repartition(3).write.mode("overwrite").format("parquet").save(dir.getCanonicalPath)
      val df = spark.read.format("parquet")
        .load(dir.getCanonicalPath)
        .repartition(1)
      assert(df.schema.fields.map(_.dataType).sameElements(dataType))
      val exprs = Seq("c1", "c2").map(col).map(_.expr)
      val sortOrder = SortOrder(Zorder(exprs), Ascending, NullsLast, Seq.empty)
      val zorderSort = Sort(Seq(sortOrder), true, df.logicalPlan)
      val result = Dataset.ofRows(spark, zorderSort)
      checkAnswer(result, expected)
    }
  }

  test("sort with zorder -- boolean column") {
    val schema = StructType(StructField("c1", BooleanType) :: StructField("c2", BooleanType) :: Nil)
    val nonNullDF = spark.createDataFrame(spark.sparkContext.parallelize(
      Seq(Row(false, false), Row(false, true), Row(true, false), Row(true, true))
    ), schema)
    val expected =
      Row(false, false) :: Row(true, false) :: Row(false, true) :: Row(true, true) :: Nil
    checkSort(nonNullDF, expected, Array(BooleanType, BooleanType))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(
      Seq(Row(false, false), Row(false, null), Row(null, false), Row(null, null))
    ), schema)
    val expected2 =
      Row(false, false) :: Row(null, false) :: Row(false, null) :: Row(null, null) :: Nil
    checkSort(df, expected2, Array(BooleanType, BooleanType))
  }

  test("sort with zorder -- int column") {
    // TODO: add more datatype unit test
    val session = spark
    import session.implicits._
    // generate 4 * 4 matrix
    val len = 3
    val input = spark.range(len + 1).selectExpr("cast(id as int) as c1")
      .select($"c1", explode(sequence(lit(0), lit(len))) as "c2")
    val expected =
      Row(0, 0) :: Row(1, 0) :: Row(0, 1) :: Row(1, 1) ::
        Row(2, 0) :: Row(3, 0) :: Row(2, 1) :: Row(3, 1) ::
        Row(0, 2) :: Row(1, 2) :: Row(0, 3) :: Row(1, 3) ::
        Row(2, 2) :: Row(3, 2) :: Row(2, 3) :: Row(3, 3) :: Nil
    checkSort(input, expected, Array(IntegerType, IntegerType))

    // contains null value case.
    val nullDF = spark.range(1).selectExpr("cast(null as int) as c1")
    val input2 = spark.range(len).selectExpr("cast(id as int) as c1")
      .union(nullDF)
      .select(
        $"c1",
        explode(concat(sequence(lit(0), lit(len - 1)), array(lit(null)))) as "c2")
    val expected2 = Row(0, 0) :: Row(1, 0) :: Row(0, 1) :: Row(1, 1) ::
      Row(2, 0) :: Row(2, 1) :: Row(0, 2) :: Row(1, 2) ::
      Row(2, 2) :: Row(null, 0) :: Row(null, 1) :: Row(null, 2) ::
      Row(0, null) :: Row(1, null) :: Row(2, null) :: Row(null, null) :: Nil
    checkSort(input2, expected2, Array(IntegerType, IntegerType))
  }

  test("sort with zorder -- string column") {
    val schema = StructType(StructField("c1", StringType) :: StructField("c2", StringType) :: Nil)
    val rdd = spark.sparkContext.parallelize(Seq(
      Row("a", "a"), Row("a", "b"), Row("a", "c"), Row("a", "d"),
      Row("b", "a"), Row("b", "b"), Row("b", "c"), Row("b", "d"),
      Row("c", "a"), Row("c", "b"), Row("c", "c"), Row("c", "d"),
      Row("d", "a"), Row("d", "b"), Row("d", "c"), Row("d", "d")))
    val input = spark.createDataFrame(rdd, schema)
    val expected = Row("a", "a") :: Row("b", "a") :: Row("c", "a") :: Row("a", "b") ::
        Row("a", "c") :: Row("b", "b") :: Row("c", "b") :: Row("b", "c") ::
        Row("c", "c") :: Row("d", "a") :: Row("d", "b") :: Row("d", "c") ::
        Row("a", "d") :: Row("b", "d") :: Row("c", "d") :: Row("d", "d") :: Nil
    checkSort(input, expected, Array(StringType, StringType))

    val rdd2 = spark.sparkContext.parallelize(Seq(
      Row(null, "a"), Row("a", "b"), Row("a", "c"), Row("a", null),
      Row("b", "a"), Row(null, "b"), Row("b", null), Row("b", "d"),
      Row("c", "a"), Row("c", null), Row(null, "c"), Row("c", "d"),
      Row("d", null), Row("d", "b"), Row("d", "c"), Row(null, "d"), Row(null, null)))
    val input2 = spark.createDataFrame(rdd2, schema)
    val expected2 = Row("b", "a") :: Row("c", "a") :: Row("a", "b") :: Row("a", "c") ::
      Row("d", "b") :: Row("d", "c") :: Row("b", "d") :: Row("c", "d") ::
      Row(null, "a") :: Row(null, "b") :: Row(null, "c") :: Row(null, "d") ::
      Row("a", null) :: Row("b", null) :: Row("c", null) :: Row("d", null) ::
      Row(null, null) :: Nil
    checkSort(input2, expected2, Array(StringType, StringType))
  }

  test("test special value of short int long type") {
    val df1 = spark.createDataFrame(Seq(
      (-1, -1L),
      (Int.MinValue, Int.MinValue.toLong),
      (1, 1L),
      (Int.MaxValue - 1, Int.MaxValue.toLong),
      (Int.MaxValue - 1, Int.MaxValue.toLong - 1),
      (Int.MaxValue, Int.MaxValue.toLong + 1),
      (Int.MaxValue, Int.MaxValue.toLong))).toDF("c1", "c2")
    val expected1 =
      Row(Int.MinValue, Int.MinValue.toLong) ::
        Row(-1, -1L) ::
        Row(1, 1L) ::
        Row(Int.MaxValue - 1, Int.MaxValue.toLong - 1) ::
        Row(Int.MaxValue - 1, Int.MaxValue.toLong) ::
        Row(Int.MaxValue, Int.MaxValue.toLong) ::
        Row(Int.MaxValue, Int.MaxValue.toLong + 1) :: Nil
    checkSort(df1, expected1, Array(IntegerType, LongType))

    val df2 = spark.createDataFrame(Seq(
      (-1, -1.toShort),
      (Short.MinValue.toInt, Short.MinValue),
      (1, 1.toShort),
      (Short.MaxValue.toInt, (Short.MaxValue - 1).toShort),
      (Short.MaxValue.toInt + 1, (Short.MaxValue - 1).toShort),
      (Short.MaxValue.toInt, Short.MaxValue),
      (Short.MaxValue.toInt + 1, Short.MaxValue))).toDF("c1", "c2")
    val expected2 =
      Row(Short.MinValue.toInt, Short.MinValue) ::
        Row(-1, -1.toShort) ::
        Row(1, 1.toShort) ::
        Row(Short.MaxValue.toInt, Short.MaxValue - 1) ::
        Row(Short.MaxValue.toInt, Short.MaxValue) ::
        Row(Short.MaxValue.toInt + 1, Short.MaxValue - 1) ::
        Row(Short.MaxValue.toInt + 1, Short.MaxValue) :: Nil
    checkSort(df2, expected2, Array(IntegerType, ShortType))

    val df3 = spark.createDataFrame(Seq(
      (-1L, -1.toShort),
      (Short.MinValue.toLong, Short.MinValue),
      (1L, 1.toShort),
      (Short.MaxValue.toLong, (Short.MaxValue - 1).toShort),
      (Short.MaxValue.toLong + 1, (Short.MaxValue - 1).toShort),
      (Short.MaxValue.toLong, Short.MaxValue),
      (Short.MaxValue.toLong + 1, Short.MaxValue))).toDF("c1", "c2")
    val expected3 =
      Row(Short.MinValue.toLong, Short.MinValue) ::
        Row(-1L, -1.toShort) ::
        Row(1L, 1.toShort) ::
        Row(Short.MaxValue.toLong, Short.MaxValue - 1) ::
        Row(Short.MaxValue.toLong, Short.MaxValue) ::
        Row(Short.MaxValue.toLong + 1, Short.MaxValue - 1) ::
        Row(Short.MaxValue.toLong + 1, Short.MaxValue) :: Nil
    checkSort(df3, expected3, Array(LongType, ShortType))
  }

  test("skip zorder if only requires one column") {
    withTable("t") {
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "false") {
        sql("CREATE TABLE t (c1 int, c2 string) stored as parquet")
        val order1 = sql("OPTIMIZE t ZORDER BY c1").queryExecution.analyzed
          .asInstanceOf[OptimizeZorderCommandBase].query.asInstanceOf[Sort].order.head.child
        assert(!order1.isInstanceOf[Zorder])
        assert(order1.isInstanceOf[AttributeReference])
      }
    }
  }

  test("Add config to control if zorder using global sort") {
    withTable("t") {
      withSQLConf(KyuubiSQLConf.ZORDER_GLOBAL_SORT_ENABLED.key -> "false") {
        sql(
          """
            |CREATE TABLE t (c1 int, c2 string) TBLPROPERTIES (
            |'kyuubi.zorder.enabled'= 'true',
            |'kyuubi.zorder.cols'= 'c1,c2')
            |""".stripMargin)
        val p1 = sql("OPTIMIZE t ZORDER BY c1, c2").queryExecution.analyzed
        assert(p1.collect {
          case shuffle: Sort if !shuffle.global => shuffle
        }.size == 1)

        val p2 = sql("INSERT INTO TABLE t SELECT * FROM VALUES(1,'a')").queryExecution.analyzed
        assert(p2.collect {
          case shuffle: Sort if !shuffle.global => shuffle
        }.size == 1)
      }
    }
  }
}

class ZorderWithCodegenEnabledSuite extends ZorderSuite {
  override def sparkConf(): SparkConf = {
    val conf = super.sparkConf
    conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    conf
  }
}

class ZorderWithCodegenDisabledSuite extends ZorderSuite {
  override def sparkConf(): SparkConf = {
    val conf = super.sparkConf
    conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
    conf.set(SQLConf.CODEGEN_FACTORY_MODE.key, "NO_CODEGEN")
    conf
  }
}
