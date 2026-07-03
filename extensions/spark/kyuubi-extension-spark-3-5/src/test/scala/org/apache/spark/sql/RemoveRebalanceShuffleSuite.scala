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

import org.apache.spark.sql.execution.{CommandResultExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{REBALANCE_PARTITIONS_BY_NONE, ShuffleExchangeExec}
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

  private def rebalanceExchanges(plan: SparkPlan): Seq[ShuffleExchangeExec] = {
    collectExchanges(plan).filter(_.shuffleOrigin == REBALANCE_PARTITIONS_BY_NONE)
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
}
