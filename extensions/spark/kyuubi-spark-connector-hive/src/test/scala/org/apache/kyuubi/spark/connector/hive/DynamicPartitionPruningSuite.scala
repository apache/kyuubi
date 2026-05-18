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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import org.apache.kyuubi.spark.connector.hive.read.HiveScan

class DynamicPartitionPruningSuite extends KyuubiHiveTest {

  private def findBatchScanExec(
      spark: SparkSession,
      sql: String,
      tableNameHint: String): BatchScanExec = {
    // Match on `HiveScan.catalogTable` rather than the node's `toString` because
    // `BatchScanExec.toString` shape differs across Spark versions.
    def matchesHint(b: BatchScanExec): Boolean = b.scan match {
      case h: HiveScan => h.catalogTable.identifier.table == tableNameHint
      case _ => false
    }

    @tailrec
    def findBatchScan(plan: SparkPlan): Option[BatchScanExec] = plan match {
      case aqe: AdaptiveSparkPlanExec => findBatchScan(aqe.inputPlan)
      case _ => plan.collectFirst {
          case b: BatchScanExec if matchesHint(b) => b
        }
    }

    val exec = findBatchScan(spark.sql(sql).queryExecution.executedPlan)
    assert(exec.isDefined)
    exec.get
  }

  test("HiveScan supports DPP runtime filtering on partition columns") {
    Seq(true, false).foreach { enabled =>
      withSparkSession(Map(
        "hive.exec.dynamic.partition.mode" -> "nonstrict",
        "spark.sql.optimizer.dynamicPartitionPruning.enabled" -> enabled.toString)) { spark =>
        val suffix = if (enabled) "on" else "off"
        val fact = s"hive.default.dpp_fact_$suffix"
        val dim = s"hive.default.dpp_dim_$suffix"

        withTable(fact, dim) {
          spark.sql(
            s"""
               | CREATE TABLE $fact (id INT, v STRING) PARTITIONED BY (dt STRING)
               | STORED AS TEXTFILE
               |""".stripMargin).collect()
          spark.sql(s"INSERT INTO $fact PARTITION (dt='2026-01-01') VALUES (1, 'a'), (2, 'b')")
          spark.sql(s"INSERT INTO $fact PARTITION (dt='2026-05-01') VALUES (3, 'c'), (4, 'd')")
          spark.sql(s"INSERT INTO $fact PARTITION (dt='2026-09-01') VALUES (5, 'e'), (6, 'f')")

          spark.sql(
            s"""
               | CREATE TABLE $dim (dt STRING, tag STRING)
               | STORED AS TEXTFILE
               |""".stripMargin).collect()
          spark.sql(s"INSERT INTO $dim VALUES ('2026-05-01', 'target')")

          val sql =
            s"""
               | SELECT f.id, f.v, f.dt
               | FROM $fact f JOIN $dim d ON f.dt = d.dt
               | WHERE d.tag = 'target'
               |""".stripMargin

          checkAnswer(
            spark.sql(sql),
            Seq(
              Row(3, "c", "2026-05-01"),
              Row(4, "d", "2026-05-01")))

          // DPP being actually applied is observable as a `DynamicPruningExpression`
          // injected into `BatchScanExec.runtimeFilters`.
          val exec = findBatchScanExec(spark, sql, fact.split('.').last)
          val hasDpp = exec.runtimeFilters.exists(_.isInstanceOf[DynamicPruningExpression])
          assert(hasDpp == enabled)
        }
      }
    }
  }
}
