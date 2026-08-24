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

package org.apache.kyuubi.spark.connector.hive

import scala.annotation.tailrec

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.kyuubi.spark.connector.hive.read.{HiveScan, KyuubiOrcScan, KyuubiParquetScan}

class DynamicPartitionPruningSuite extends KyuubiHiveTest {

  private def findBatchScanExec(plan: SparkPlan, tableNameHint: String): BatchScanExec = {
    // Match on the underlying Hive `catalogTable` rather than the node's `toString`
    // because `BatchScanExec.toString` shape differs across Spark versions.
    def hiveTableName(b: BatchScanExec): Option[String] = b.scan match {
      case h: HiveScan => Some(h.catalogTable.identifier.table)
      case o: KyuubiOrcScan => Some(o.catalogTable.identifier.table)
      case p: KyuubiParquetScan => Some(p.catalogTable.identifier.table)
      case _ => None
    }

    @tailrec
    def findBatchScan(p: SparkPlan): Option[BatchScanExec] = p match {
      case aqe: AdaptiveSparkPlanExec => findBatchScan(aqe.inputPlan)
      case _ => p.collectFirst {
          case b: BatchScanExec if hiveTableName(b).contains(tableNameHint) => b
        }
    }

    val exec = findBatchScan(plan)
    assert(exec.isDefined)
    exec.get
  }

  private def runDppCase(storedAs: String): Unit = {
    // Collect the number of input partitions actually planned under DPP on / off
    // and later assert a strict reduction.
    val plannedPartitions = scala.collection.mutable.Map.empty[Boolean, Int]

    Seq(true, false).foreach { enabled =>
      withSparkSession(Map(
        "hive.exec.dynamic.partition.mode" -> "nonstrict",
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> enabled.toString)) { spark =>
        val suffix = s"${storedAs.toLowerCase}_${if (enabled) "on" else "off"}"
        val fact = s"hive.default.dpp_fact_$suffix"
        val dim = s"hive.default.dpp_dim_$suffix"

        dropTableAfter(fact, dim) {
          spark.sql(
            s"""
               | CREATE TABLE $fact (id INT, v STRING) PARTITIONED BY (dt STRING)
               | STORED AS $storedAs
               |""".stripMargin)
          spark.sql(s"INSERT INTO $fact PARTITION (dt='2026-01-01') VALUES (1, 'a'), (2, 'b')")
          spark.sql(s"INSERT INTO $fact PARTITION (dt='2026-05-01') VALUES (3, 'c'), (4, 'd')")
          spark.sql(s"INSERT INTO $fact PARTITION (dt='2026-09-01') VALUES (5, 'e'), (6, 'f')")

          spark.sql(
            s"""
               | CREATE TABLE $dim (dt STRING, tag STRING)
               | STORED AS $storedAs
               |""".stripMargin)
          spark.sql(s"INSERT INTO $dim VALUES ('2026-05-01', 'target')")

          val df = spark.sql(
            s"""
               | SELECT f.id, f.v, f.dt
               | FROM $fact f JOIN $dim d ON f.dt = d.dt
               | WHERE d.tag = 'target'
               |""".stripMargin)
          checkAnswer(
            df,
            Seq(
              Row(3, "c", "2026-05-01"),
              Row(4, "d", "2026-05-01")))

          // DPP being actually applied is observable as a `DynamicPruningExpression`
          // injected into `BatchScanExec.runtimeFilters`.
          val exec = findBatchScanExec(df.queryExecution.executedPlan, fact.split('.').last)
          val hasDpp = exec.runtimeFilters.exists(_.isInstanceOf[DynamicPruningExpression])
          assert(hasDpp == enabled)

          val planned = exec.scan.toBatch.planInputPartitions().length
          plannedPartitions(enabled) = planned

          exec.scan match {
            case _: KyuubiOrcScan | _: KyuubiParquetScan =>
              assert(exec.scan.columnarSupportMode() == Scan.ColumnarSupportMode.SUPPORTED)
            case _: HiveScan =>
              assert(exec.scan.columnarSupportMode() == Scan.ColumnarSupportMode.UNSUPPORTED)
            case other =>
              fail(s"unexpected scan type: ${other.getClass.getName}")
          }
        }
      }
    }

    val planOn = plannedPartitions(true)
    val planOff = plannedPartitions(false)
    assert(
      planOn < planOff,
      s"DPP ($storedAs) should plan fewer partitions when enabled")
  }

  test("HiveScan supports DPP runtime filtering on partition columns") {
    runDppCase(storedAs = "TEXTFILE")
  }

  test("KyuubiOrcScan supports DPP runtime filtering on partition columns") {
    runDppCase(storedAs = "ORC")
  }

  test("KyuubiParquetScan supports DPP runtime filtering on partition columns") {
    runDppCase(storedAs = "PARQUET")
  }

  /**
   * Build a fact table and assert `columnarSupportMode()` matches
   * `expectedColumnarMode` under the given `extraConf`.
   */
  private def runColumnarModeCase(
      storedAs: String,
      extraConf: Map[String, String],
      expectedColumnarMode: Scan.ColumnarSupportMode): Unit = {
    withSparkSession(extraConf ++ Map(
      "hive.exec.dynamic.partition.mode" -> "nonstrict")) { spark =>
      val fact = s"hive.default.mode_fact_${storedAs.toLowerCase}"
      dropTableAfter(fact) {
        spark.sql(
          s"""
             | CREATE TABLE $fact (id INT, v STRING) PARTITIONED BY (dt STRING)
             | STORED AS $storedAs
             |""".stripMargin)
        spark.sql(s"INSERT INTO $fact PARTITION (dt='2026-05-01') VALUES (1, 'a')")

        val df = spark.sql(s"SELECT id, v, dt FROM $fact")
        val exec = findBatchScanExec(df.queryExecution.executedPlan, fact.split('.').last)
        exec.scan match {
          case _: KyuubiOrcScan | _: KyuubiParquetScan =>
            assert(exec.scan.columnarSupportMode() == expectedColumnarMode)
          case other =>
            fail(s"unexpected scan type: ${other.getClass.getName}")
        }
      }
    }
  }

  test("KyuubiOrcScan returns UNSUPPORTED when orc vectorized reader is disabled") {
    runColumnarModeCase(
      storedAs = "ORC",
      extraConf = Map(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> "false"),
      expectedColumnarMode = Scan.ColumnarSupportMode.UNSUPPORTED)
  }

  test("KyuubiParquetScan returns UNSUPPORTED when parquet vectorized reader is disabled") {
    runColumnarModeCase(
      storedAs = "PARQUET",
      extraConf = Map(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false"),
      expectedColumnarMode = Scan.ColumnarSupportMode.UNSUPPORTED)
  }

  private def runAllPrunedCase(storedAs: String): Unit = {
    withSparkSession(Map(
      "hive.exec.dynamic.partition.mode" -> "nonstrict",
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true")) { spark =>
      val suffix = storedAs.toLowerCase
      val fact = s"hive.default.pruned_fact_$suffix"
      val dim = s"hive.default.pruned_dim_$suffix"

      dropTableAfter(fact, dim) {
        spark.sql(
          s"""
             | CREATE TABLE $fact (id INT, v STRING) PARTITIONED BY (dt STRING)
             | STORED AS $storedAs
             |""".stripMargin)
        spark.sql(s"INSERT INTO $fact PARTITION (dt='2026-01-01') VALUES (1, 'a')")
        spark.sql(s"INSERT INTO $fact PARTITION (dt='2026-05-01') VALUES (2, 'b')")

        spark.sql(
          s"""
             | CREATE TABLE $dim (dt STRING, tag STRING)
             | STORED AS $storedAs
             |""".stripMargin)
        // Dim key matches no fact partition, so DPP prunes every fact partition.
        spark.sql(s"INSERT INTO $dim VALUES ('1999-12-31', 'target')")

        val df = spark.sql(
          s"""
             | SELECT f.id, f.v, f.dt
             | FROM $fact f JOIN $dim d ON f.dt = d.dt
             | WHERE d.tag = 'target'
             |""".stripMargin)
        // Trigger full execution so `BatchScanExec.filteredPartitions` pushes
        // runtime filters into the wrapped scan (via `SupportsRuntimeFiltering`)
        // and `planInputPartitions()` below reflects DPP-pruned partitions.
        assert(df.collect().isEmpty)

        val exec = findBatchScanExec(df.queryExecution.executedPlan, fact.split('.').last)
        exec.scan match {
          case _: KyuubiOrcScan | _: KyuubiParquetScan =>
            assert(exec.scan.toBatch.planInputPartitions().isEmpty)
            assert(exec.scan.columnarSupportMode() == Scan.ColumnarSupportMode.SUPPORTED)
          case other =>
            fail(s"unexpected scan type: ${other.getClass.getName}")
        }
      }
    }
  }

  test("KyuubiOrcScan returns SUPPORTED when DPP prunes every partition") {
    runAllPrunedCase(storedAs = "ORC")
  }

  test("KyuubiParquetScan returns SUPPORTED when DPP prunes every partition") {
    runAllPrunedCase(storedAs = "PARQUET")
  }
}
