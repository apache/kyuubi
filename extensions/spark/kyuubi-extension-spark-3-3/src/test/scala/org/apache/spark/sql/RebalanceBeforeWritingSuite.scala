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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, RebalancePartitions, Sort}
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.execution.OptimizedCreateHiveTableAsSelectCommand

import org.apache.kyuubi.sql.KyuubiSQLConf

class RebalanceBeforeWritingSuite extends KyuubiSparkSQLExtensionTest {

  test("check rebalance exists") {
    def check(
        df: => DataFrame,
        expectedRebalanceNumEnabled: Int = 1,
        expectedRebalanceNumDisabled: Int = 0): Unit = {
      withSQLConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE_IF_NO_SHUFFLE.key -> "true") {
        assert(
          df.queryExecution.analyzed.collect {
            case r: RebalancePartitions => r
          }.size == expectedRebalanceNumEnabled)
      }
      withSQLConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE_IF_NO_SHUFFLE.key -> "false") {
        assert(
          df.queryExecution.analyzed.collect {
            case r: RebalancePartitions => r
          }.size == expectedRebalanceNumDisabled)
      }
    }

    // It's better to set config explicitly in case of we change the default value.
    withSQLConf(KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true") {
      Seq("USING PARQUET", "").foreach { storage =>
        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 (c1 int) $storage PARTITIONED BY (c2 string)")
          check(sql("INSERT INTO TABLE tmp1 PARTITION(c2='a') " +
            "SELECT * FROM VALUES(1),(2) AS t(c1)"))
        }

        withTable("tmp1", "tmp2") {
          sql(s"CREATE TABLE tmp1 (c1 int) $storage PARTITIONED BY (c2 string)")
          sql(s"CREATE TABLE tmp2 (c1 int) $storage PARTITIONED BY (c2 string)")
          check(
            sql(
              """FROM VALUES(1),(2)
                |INSERT INTO TABLE tmp1 PARTITION(c2='a') SELECT *
                |INSERT INTO TABLE tmp2 PARTITION(c2='a') SELECT *
                |""".stripMargin),
            2)
        }

        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 (c1 int) $storage")
          check(sql("INSERT INTO TABLE tmp1 SELECT * FROM VALUES(1),(2),(3) AS t(c1)"))
        }

        withTable("tmp1") {
          sql(s"CREATE TABLE tmp1 (c1 int) $storage")
          check(
            sql("INSERT INTO TABLE tmp1 SELECT /*+ REBALANCE */ * FROM VALUES(1),(2),(3) AS t(c1)"),
            1,
            1)
        }

        withTable("tmp1", "tmp2") {
          sql(s"CREATE TABLE tmp1 (c1 int) $storage")
          sql(s"CREATE TABLE tmp2 (c1 int) $storage")
          check(
            sql(
              """FROM VALUES(1),(2),(3)
                |INSERT INTO TABLE tmp1 SELECT *
                |INSERT INTO TABLE tmp2 SELECT *
                |""".stripMargin),
            2)
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

  test("check rebalance does not exists") {
    def check(df: DataFrame): Unit = {
      assert(
        df.queryExecution.analyzed.collect {
          case r: RebalancePartitions => r
        }.isEmpty)
    }

    withSQLConf(
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true",
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE_IF_NO_SHUFFLE.key -> "true") {
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
        case r: RebalancePartitions if r.partitionExpressions.size == 1 =>
          assert(r.partitionExpressions.head.asInstanceOf[Attribute].name === "c2")
          r
      }.size == 1)
    }

    withSQLConf(
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true",
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE_IF_NO_SHUFFLE.key -> "true") {
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

  test("OptimizedCreateHiveTableAsSelectCommand") {
    withSQLConf(
      HiveUtils.CONVERT_METASTORE_PARQUET.key -> "true",
      HiveUtils.CONVERT_METASTORE_CTAS.key -> "true",
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE_IF_NO_SHUFFLE.key -> "true") {
      withTable("t") {
        val df = sql(s"CREATE TABLE t STORED AS parquet AS SELECT 1 as a")
        val ctas = df.queryExecution.analyzed.collect {
          case _: OptimizedCreateHiveTableAsSelectCommand => true
        }
        assert(ctas.size == 1)
        val repartition = df.queryExecution.analyzed.collect {
          case _: RebalancePartitions => true
        }
        assert(repartition.size == 1)
      }
    }
  }

  test("Infer rebalance and sorder orders") {
    def checkShuffleAndSort(dataWritingCommand: LogicalPlan, sSize: Int, rSize: Int): Unit = {
      assert(dataWritingCommand.isInstanceOf[DataWritingCommand])
      val plan = dataWritingCommand.asInstanceOf[DataWritingCommand].query
      assert(plan.collect {
        case s: Sort => s
      }.size == sSize)
      assert(plan.collect {
        case r: RebalancePartitions if r.partitionExpressions.size == rSize => r
      }.nonEmpty || rSize == 0)
    }

    withView("v") {
      withTable("t", "input1", "input2") {
        withSQLConf(KyuubiSQLConf.INFER_REBALANCE_AND_SORT_ORDERS.key -> "true") {
          sql(s"CREATE TABLE t (c1 int, c2 long) USING PARQUET PARTITIONED BY (p string)")
          sql(s"CREATE TABLE input1 USING PARQUET AS SELECT * FROM VALUES(1,2),(1,3)")
          sql(s"CREATE TABLE input2 USING PARQUET AS SELECT * FROM VALUES(1,3),(1,3)")
          sql(s"CREATE VIEW v as SELECT col1, count(*) as col2 FROM input1 GROUP BY col1")

          val df0 = sql(
            s"""
               |INSERT INTO TABLE t PARTITION(p='a')
               |SELECT /*+ broadcast(input2) */ input1.col1, input2.col1
               |FROM input1
               |JOIN input2
               |ON input1.col1 = input2.col1
               |""".stripMargin)
          checkShuffleAndSort(df0.queryExecution.analyzed, 1, 1)

          val df1 = sql(
            s"""
               |INSERT INTO TABLE t PARTITION(p='a')
               |SELECT /*+ broadcast(input2) */ input1.col1, input1.col2
               |FROM input1
               |LEFT JOIN input2
               |ON input1.col1 = input2.col1 and input1.col2 = input2.col2
               |""".stripMargin)
          checkShuffleAndSort(df1.queryExecution.analyzed, 1, 2)

          val df2 = sql(
            s"""
               |INSERT INTO TABLE t PARTITION(p='a')
               |SELECT col1 as c1, count(*) as c2
               |FROM input1
               |GROUP BY col1
               |HAVING count(*) > 0
               |""".stripMargin)
          checkShuffleAndSort(df2.queryExecution.analyzed, 1, 1)

          // dynamic partition
          val df3 = sql(
            s"""
               |INSERT INTO TABLE t PARTITION(p)
               |SELECT /*+ broadcast(input2) */ input1.col1, input1.col2, input1.col2
               |FROM input1
               |JOIN input2
               |ON input1.col1 = input2.col1
               |""".stripMargin)
          checkShuffleAndSort(df3.queryExecution.analyzed, 0, 1)

          // non-deterministic
          val df4 = sql(
            s"""
               |INSERT INTO TABLE t PARTITION(p='a')
               |SELECT col1 + rand(), count(*) as c2
               |FROM input1
               |GROUP BY col1
               |""".stripMargin)
          checkShuffleAndSort(df4.queryExecution.analyzed, 0, 0)

          // view
          val df5 = sql(
            s"""
               |INSERT INTO TABLE t PARTITION(p='a')
               |SELECT * FROM v
               |""".stripMargin)
          checkShuffleAndSort(df5.queryExecution.analyzed, 1, 1)
        }
      }
    }
  }
}
