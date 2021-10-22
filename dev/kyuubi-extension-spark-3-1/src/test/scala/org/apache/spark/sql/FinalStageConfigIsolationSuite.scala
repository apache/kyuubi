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

import org.apache.spark.sql.execution.adaptive.{CustomShuffleReaderExec, QueryStageExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.kyuubi.sql.{FinalStageConfigIsolation, KyuubiSQLConf}

class FinalStageConfigIsolationSuite extends KyuubiSparkSQLExtensionTest {
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    setupData()
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
    def checkPartitionNum(sqlString: String, previousPartitionNum: Int,
                          finalPartitionNum: Int): Unit = {
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
}
