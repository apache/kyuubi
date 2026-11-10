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

import org.apache.spark.sql.execution.{CommandResultExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{REBALANCE_PARTITIONS_BY_COL, REBALANCE_PARTITIONS_BY_NONE, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf._

import org.apache.kyuubi.sql.KyuubiSQLConf

class RemoveRebalanceShuffleSuite extends KyuubiSparkSQLExtensionTest {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setupData()
  }

  private def collectExchanges(plan: SparkPlan): Seq[ShuffleExchangeExec] = plan match {
    case p: CommandResultExec => collectExchanges(p.commandPhysicalPlan)
    case p: AdaptiveSparkPlanExec => collectExchanges(p.finalPhysicalPlan)
    case p: ShuffleQueryStageExec => collectExchanges(p.plan)
    case p: ShuffleExchangeExec => p +: collectExchanges(p.child)
    case p => p.children.flatMap(collectExchanges)
  }

  /**
   * Collects all rebalance shuffle exchanges from the physical plan. Both
   * [[REBALANCE_PARTITIONS_BY_NONE]] (empty partition expressions, e.g. inserted by
   * `InsertAdaptor` for non-partitioned tables) and [[REBALANCE_PARTITIONS_BY_COL]]
   * (non-empty partition expressions, e.g. inferred by `InferRebalanceAndSortOrders`
   * for partitioned tables) are counted.
   */
  private def rebalanceExchanges(plan: SparkPlan): Seq[ShuffleExchangeExec] = {
    collectExchanges(plan).filter(e =>
      e.shuffleOrigin == REBALANCE_PARTITIONS_BY_NONE ||
        e.shuffleOrigin == REBALANCE_PARTITIONS_BY_COL)
  }

  /**
   * Collects local [[SortExec]] nodes from the plan that are NOT part of a
   * [[SortMergeJoinExec]] (whose child Sort nodes are join-required sorts, not the
   * local Sort injected by `InferRebalanceAndSortOrders`). Used to verify that
   * [[org.apache.spark.sql.execution.RemoveRedundantSorts]] cleaned up the local Sort
   * after the rebalance was removed.
   */
  private def collectSorts(plan: SparkPlan): Seq[SortExec] = plan match {
    case p: CommandResultExec => collectSorts(p.commandPhysicalPlan)
    case p: AdaptiveSparkPlanExec => collectSorts(p.finalPhysicalPlan)
    case p: ShuffleQueryStageExec => collectSorts(p.plan)
    // Don't recurse into SortMergeJoinExec, its child Sort nodes are join-required sorts,
    // not the local Sort injected by InferRebalanceAndSortOrders.
    case _: SortMergeJoinExec => Seq.empty
    case p: SortExec => p +: p.children.flatMap(collectSorts).toSeq
    case p => p.children.flatMap(collectSorts)
  }

  private def assertRebalanceRemoved(df: DataFrame, removed: Boolean): Unit = {
    df.collect()
    val rebalances = rebalanceExchanges(df.queryExecution.executedPlan)
    val expected = if (removed) 0 else 1
    assert(
      rebalances.size == expected,
      s"Expected $expected rebalance shuffle, got ${rebalances.size}:\n" +
        s"${df.queryExecution.executedPlan}")
  }

  test("remove rebalance shuffle - small-data scenario with non-expanding semi join") {
    // A left-semi join is reducing (not expanding), so the small-data condition applies: the
    // materialized stage size is far below `advisoryPartitionSize * tolerableSmallFileNum`, so the
    // rebalance shuffle is removed.
    withSQLConf(
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true",
      KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_ENABLED.key -> "true",
      ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB",
      KyuubiSQLConf.REBALANCE_PARTITIONS_ADVISORY_PARTITION_SIZE.key -> "128MB",
      COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "2",
      AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("tmp1") {
        sql("CREATE TABLE tmp1 USING PARQUET as select * from range(10)")
        assertRebalanceRemoved(
          sql("INSERT INTO TABLE tmp1 SELECT a.c1 FROM t1 a LEFT SEMI JOIN t2 b ON a.c1 = b.c1"),
          removed = true)
      }
    }
  }

  test("large-data scenario with non-reducing outer join") {
    // A left-outer join is expanding (not reducing), so the large-data condition applies. With a
    // tiny `smallPartitionSize`, the stage size exceeds
    // `numShufflePartitions * smallPartitionSize`, so the rebalance shuffle is removed.
    withSQLConf(
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true",
      KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_ENABLED.key -> "true",
      ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB",
      KyuubiSQLConf.REBALANCE_PARTITIONS_ADVISORY_PARTITION_SIZE.key -> "128MB",
      SHUFFLE_PARTITIONS.key -> "2",
      AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {

      withTable("tmp1") {
        sql("CREATE TABLE tmp1 USING PARQUET as select * from range(10)")

        withSQLConf(KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_SMALL_PARTITION_SIZE.key -> "1b") {
          assertRebalanceRemoved(
            sql("INSERT INTO TABLE tmp1 SELECT a.c1 FROM t1 a LEFT OUTER JOIN t2 b ON a.c1 = b.c1"),
            removed = true)
        }

        withSQLConf(KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_SMALL_PARTITION_SIZE.key -> "1tb") {
          assertRebalanceRemoved(
            sql("INSERT INTO TABLE tmp1 SELECT a.c1 FROM t1 a LEFT OUTER JOIN t2 b ON a.c1 = b.c1"),
            removed = false)
        }
      }
    }
  }

  test("keep rebalance shuffle - advisory size not larger than session advisory size") {
    // The rebalance advisory size equals the session advisory size, which suggests the user
    // intentionally wants smaller partitions, so the rebalance shuffle is kept.
    withSQLConf(
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true",
      KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_ENABLED.key -> "true",
      ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB",
      KyuubiSQLConf.REBALANCE_PARTITIONS_ADVISORY_PARTITION_SIZE.key -> "64MB",
      COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "2",
      AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("tmp1") {
        sql("CREATE TABLE tmp1 USING PARQUET as select * from range(10)")
        assertRebalanceRemoved(
          sql("INSERT INTO TABLE tmp1 SELECT a.c1 FROM t1 a LEFT SEMI JOIN t2 b ON a.c1 = b.c1"),
          removed = false)
      }
    }
  }

  test("keep rebalance shuffle - inner join is both reducing and expanding") {
    // An inner join is classified as both reducing and expanding, so neither the large-data nor
    // the small-data condition can be satisfied and the rebalance shuffle is kept.
    withSQLConf(
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true",
      KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_ENABLED.key -> "true",
      ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB",
      KyuubiSQLConf.REBALANCE_PARTITIONS_ADVISORY_PARTITION_SIZE.key -> "128MB",
      COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "2",
      AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("tmp1") {
        sql("CREATE TABLE tmp1 USING PARQUET as select * from range(10)")
        assertRebalanceRemoved(
          sql("INSERT INTO TABLE tmp1 SELECT a.c1 FROM t1 a JOIN t2 b ON a.c1 = b.c1"),
          removed = false)
      }
    }
  }

  test("keep rebalance shuffle - reducing semi join fails the small-data condition") {
    // A left-semi join is reducing, so the large-data condition never applies. Setting
    // `tolerableSmallFileNum` to 0 also disables the small-data condition, so the shuffle is kept.
    withSQLConf(
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true",
      KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_ENABLED.key -> "true",
      ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB",
      KyuubiSQLConf.REBALANCE_PARTITIONS_ADVISORY_PARTITION_SIZE.key -> "128MB",
      KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_TOLERABLE_SMALL_FILE_NUM.key -> "0",
      COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "2",
      AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("tmp1") {
        sql("CREATE TABLE tmp1 USING PARQUET as select * from range(10)")
        assertRebalanceRemoved(
          sql("INSERT INTO TABLE tmp1 SELECT a.c1 FROM t1 a LEFT SEMI JOIN t2 b ON a.c1 = b.c1"),
          removed = false)
      }
    }
  }

  test("keep rebalance shuffle - switch off") {
    withSQLConf(
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true",
      KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_ENABLED.key -> "false",
      ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB",
      KyuubiSQLConf.REBALANCE_PARTITIONS_ADVISORY_PARTITION_SIZE.key -> "128MB",
      COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "2",
      AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("tmp1") {
        sql("CREATE TABLE tmp1 USING PARQUET as select * from range(10)")
        assertRebalanceRemoved(
          sql("INSERT INTO TABLE tmp1 SELECT a.c1 FROM t1 a LEFT SEMI JOIN t2 b ON a.c1 = b.c1"),
          removed = false)
      }
    }
  }

  test("remove rebalance shuffle - union of non-expanding branches") {
    // A union-all of two left-semi joins: every branch is reducing (not expanding) and each forms
    // its own materialized query stage group. All groups satisfy the small-data condition, so the
    // rebalance shuffle is removed.
    withSQLConf(
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true",
      KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_ENABLED.key -> "true",
      ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB",
      KyuubiSQLConf.REBALANCE_PARTITIONS_ADVISORY_PARTITION_SIZE.key -> "128MB",
      COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "2",
      AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("tmp1") {
        sql("CREATE TABLE tmp1 USING PARQUET as select * from range(10)")
        assertRebalanceRemoved(
          sql(
            "INSERT INTO TABLE tmp1 " +
              "SELECT a.c1 FROM t1 a LEFT SEMI JOIN t2 b ON a.c1 = b.c1 " +
              "UNION ALL " +
              "SELECT a.c1 FROM t1 a LEFT SEMI JOIN t3 b ON a.c1 = b.c1"),
          removed = true)
      }
    }
  }

  test("keep rebalance shuffle - union with an expanding branch") {
    // A union-all of a left-semi join and an inner join. The inner join makes the whole rebalance
    // input both reducing and expanding, so neither the large-data nor the small-data condition can
    // be satisfied for any group and the rebalance shuffle is kept.
    withSQLConf(
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true",
      KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_ENABLED.key -> "true",
      ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB",
      KyuubiSQLConf.REBALANCE_PARTITIONS_ADVISORY_PARTITION_SIZE.key -> "128MB",
      COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "2",
      AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("tmp1") {
        sql("CREATE TABLE tmp1 USING PARQUET as select * from range(10)")
        assertRebalanceRemoved(
          sql(
            "INSERT INTO TABLE tmp1 " +
              "SELECT a.c1 FROM t1 a LEFT SEMI JOIN t2 b ON a.c1 = b.c1 " +
              "UNION ALL " +
              "SELECT a.c1 FROM t1 a JOIN t3 b ON a.c1 = b.c1"),
          removed = false)
      }
    }
  }

  test("remove rebalance shuffle - cached table produces TableCacheQueryStageExec") {
    // A cached table produces a TableCacheQueryStageExec (not an ExchangeQueryStageExec).
    // The non-expanding cached table scan satisfies the small-data condition, so the
    // rebalance shuffle should be removed.
    withSQLConf(
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true",
      KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_ENABLED.key -> "true",
      ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB",
      KyuubiSQLConf.REBALANCE_PARTITIONS_ADVISORY_PARTITION_SIZE.key -> "128MB",
      COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "2",
      AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("tmp1") {
        sql("CACHE TABLE t2")
        try {
          sql("CREATE TABLE tmp1 USING PARQUET as select * from range(10)")
          assertRebalanceRemoved(
            sql("INSERT INTO TABLE tmp1 SELECT c1 FROM t2"),
            removed = true)
        } finally {
          sql("UNCACHE TABLE t2")
        }
      }
    }
  }

  test("remove rebalance shuffle - static partition insert with non-expanding query") {
    // staticPartitions.size == partitionColumns.size triggers the rule for static partition
    // inserts. A simple SELECT has no reducing or expanding operators, so the small-data
    // condition applies and the rebalance shuffle is removed.
    withSQLConf(
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true",
      KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_ENABLED.key -> "true",
      ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB",
      KyuubiSQLConf.REBALANCE_PARTITIONS_ADVISORY_PARTITION_SIZE.key -> "128MB",
      COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "2",
      AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("tmp1") {
        sql("CREATE TABLE tmp1 (c1 int) USING PARQUET PARTITIONED BY (c2 string)")
        assertRebalanceRemoved(
          sql("INSERT INTO TABLE tmp1 PARTITION(c2='a') SELECT c1 FROM t2"),
          removed = true)
      }
    }
  }

  test("keep rebalance shuffle - static partition insert with inner join") {
    // An inner join is both reducing and expanding, so neither the large-data nor the small-data
    // condition can be satisfied and the rebalance shuffle is kept.
    withSQLConf(
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true",
      KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_ENABLED.key -> "true",
      ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB",
      KyuubiSQLConf.REBALANCE_PARTITIONS_ADVISORY_PARTITION_SIZE.key -> "128MB",
      COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "2",
      AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("tmp1") {
        sql("CREATE TABLE tmp1 (c1 int) USING PARQUET PARTITIONED BY (c2 string)")
        assertRebalanceRemoved(
          sql("INSERT INTO TABLE tmp1 PARTITION(c2='a') " +
            "SELECT a.c1 FROM t1 a JOIN t2 b ON a.c1 = b.c1"),
          removed = false)
      }
    }
  }

  test("keep rebalance shuffle - dynamic partition insert skips rule") {
    // Dynamic partition inserts (staticPartitions.size < partitionColumns.size) do not trigger
    // the rule because the rebalance helps cluster data by partition values, so the rebalance
    // shuffle is kept regardless of data size or operators.
    withSQLConf(
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true",
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE_IF_NO_SHUFFLE.key -> "true",
      KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_ENABLED.key -> "true",
      ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB",
      KyuubiSQLConf.REBALANCE_PARTITIONS_ADVISORY_PARTITION_SIZE.key -> "128MB",
      COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "2",
      AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("tmp1") {
        sql("CREATE TABLE tmp1 (c1 int) USING PARQUET PARTITIONED BY (c2 string)")
        assertRebalanceRemoved(
          sql("INSERT INTO TABLE tmp1 PARTITION(c2) SELECT c1, c2 FROM t2"),
          removed = false)
      }
    }
  }

  test("remove both rebalance and local sort inferred by InferRebalanceAndSortOrders") {
    // When InferRebalanceAndSortOrders infers non-empty partitionExpressions from a join query
    // and injects both a RebalancePartitions and a wrapping local Sort, the
    // RemoveRebalanceShuffle rule removes the RebalancePartitions (the relaxed partitionExpressions
    // match allows non-empty expressions). The local Sort then serves no purpose and is
    // removed by Spark's RemoveRedundantSorts rule.
    // Uses a left-semi join: it is reducing (not expanding), so the small-data condition applies.
    withSQLConf(
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE.key -> "true",
      KyuubiSQLConf.INSERT_REPARTITION_BEFORE_WRITE_IF_NO_SHUFFLE.key -> "true",
      KyuubiSQLConf.INFER_REBALANCE_AND_SORT_ORDERS.key -> "true",
      KyuubiSQLConf.INFER_REBALANCE_AND_SORT_ORDERS_MAX_COLUMNS.key -> "2",
      KyuubiSQLConf.REMOVE_REBALANCE_SHUFFLE_ENABLED.key -> "true",
      ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB",
      KyuubiSQLConf.REBALANCE_PARTITIONS_ADVISORY_PARTITION_SIZE.key -> "128MB",
      COALESCE_PARTITIONS_INITIAL_PARTITION_NUM.key -> "2",
      AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      withTable("tmp_pt", "input1", "input2") {
        sql("CREATE TABLE tmp_pt (c1 int, c2 long) USING PARQUET PARTITIONED BY (p string)")
        sql("CREATE TABLE input1 USING PARQUET AS SELECT * FROM VALUES(1,2),(1,3)")
        sql("CREATE TABLE input2 USING PARQUET AS SELECT * FROM VALUES(1,3),(1,3)")

        val df = sql(
          """INSERT INTO TABLE tmp_pt PARTITION(p='a')
            |SELECT input1.col1, input1.col2
            |FROM input1 LEFT SEMI JOIN input2 ON input1.col1 = input2.col1
            |""".stripMargin)

        // Verify rebalance is removed
        assertRebalanceRemoved(df, removed = true)

        // Verify the local Sort injected by InferRebalanceAndSortOrders is also removed
        df.collect()
        val sorts = collectSorts(df.queryExecution.executedPlan)
        assert(
          sorts.isEmpty,
          s"Expected no local Sort in the final plan after rebalance removal, " +
            s"but found ${sorts.size}:\n${df.queryExecution.executedPlan}")
      }
    }
  }
}
