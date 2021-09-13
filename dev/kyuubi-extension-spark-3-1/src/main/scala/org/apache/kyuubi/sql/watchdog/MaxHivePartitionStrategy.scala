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

package org.apache.kyuubi.sql.watchdog

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

import org.apache.kyuubi.sql.KyuubiSQLConf

/**
 * Add maxHivePartitions Strategy to avoid scan excessive hive partitions on partitioned table
 * 1 Check if scan exceed maxHivePartition
 * 2 Check if Using partitionFilter on partitioned table
 * This Strategy Add Planner Strategy after LogicalOptimizer
 */
case class MaxHivePartitionStrategy(session: SparkSession)
  extends Strategy with SQLConfHelper {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    conf.getConf(KyuubiSQLConf.WATCHDOG_MAX_HIVEPARTITION) match {
      case Some(maxHivePartition) => plan match {
        case ScanOperation(_, _, relation: HiveTableRelation) if relation.isPartitioned =>
            relation.prunedPartitions match {
              case Some(prunedPartitions) => if (prunedPartitions.size > maxHivePartition) {
                throw new MaxHivePartitionExceedException(
                  s"""
                    |SQL job scan hive partition: ${prunedPartitions.size}
                    |exceed restrict of hive scan maxPartition $maxHivePartition
                    |You should optimize your SQL logical according partition structure
                    |or shorten query scope such as p_date, detail as below:
                    |Table: ${relation.tableMeta.qualifiedName}
                    |Owner: ${relation.tableMeta.owner}
                    |Partition Structure: ${relation.partitionCols.map(_.name).mkString(" -> ")}
                    |""".stripMargin)
              } else {
                Nil
              }
              case _ => throw new HivePartitionFilterUnusedException(
                s"""
                   |Your SQL job scan a whole huge table without any partition filter,
                   |You should optimize your SQL logical according partition structure
                   |or shorten query scope such as p_date, detail as below:
                   |Table: ${relation.tableMeta.qualifiedName}
                   |Owner: ${relation.tableMeta.owner}
                   |Partition Structure: ${relation.partitionCols.map(_.name).mkString(" -> ")}
                   |""".stripMargin)
            }
        case _ => Nil
      }
      case _ => Nil
    }
  }
}
