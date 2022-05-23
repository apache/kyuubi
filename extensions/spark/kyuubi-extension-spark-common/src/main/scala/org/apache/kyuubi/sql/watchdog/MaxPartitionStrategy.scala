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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{PruneFileSourcePartitionHelper, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.sql.KyuubiSQLConf

/**
 * Add maxPartitions Strategy to avoid scan excessive partitions on partitioned table
 * 1 Check if scan exceed maxPartition
 * 2 Check if Using partitionFilter on partitioned table
 * This Strategy Add Planner Strategy after LogicalOptimizer
 */
case class MaxPartitionStrategy(session: SparkSession)
  extends Strategy
  with SQLConfHelper
  with PruneFileSourcePartitionHelper {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val maxScanPartitionsOpt = conf.getConf(KyuubiSQLConf.WATCHDOG_MAX_PARTITIONS)

    if (maxScanPartitionsOpt.isDefined) {
      checkRelationMaxPartitions(plan, maxScanPartitionsOpt.get)
    }
    Nil
  }

  private def checkRelationMaxPartitions(
      plan: LogicalPlan,
      maxScanPartitions: Int): Unit = {
    plan match {
      case ScanOperation(_, _, relation: HiveTableRelation) if relation.isPartitioned =>
        relation.prunedPartitions match {
          case Some(prunedPartitions) =>
            if (prunedPartitions.size > maxScanPartitions) {
              throw new MaxPartitionExceedException(
                s"""
                   |SQL job scan hive partition: ${prunedPartitions.size}
                   |exceed restrict of hive scan maxPartition $maxScanPartitions
                   |You should optimize your SQL logical according partition structure
                   |or shorten query scope such as p_date, detail as below:
                   |Table: ${relation.tableMeta.qualifiedName}
                   |Owner: ${relation.tableMeta.owner}
                   |Partition Structure: ${relation.partitionCols.map(_.name).mkString(", ")}
                   |""".stripMargin)
            }
          case _ =>
            val totalPartitions = session
              .sessionState.catalog.externalCatalog.listPartitionNames(
                relation.tableMeta.database,
                relation.tableMeta.identifier.table)
            if (totalPartitions.size > maxScanPartitions) {
              throw new MaxPartitionExceedException(
                s"""
                   |Your SQL job scan a whole huge table without any partition filter,
                   |You should optimize your SQL logical according partition structure
                   |or shorten query scope such as p_date, detail as below:
                   |Table: ${relation.tableMeta.qualifiedName}
                   |Owner: ${relation.tableMeta.owner}
                   |Partition Structure: ${relation.partitionCols.map(_.name).mkString(", ")}
                   |""".stripMargin)
            }
        }
      case ScanOperation(
            _,
            filters,
            relation @ LogicalRelation(
              fsRelation @ HadoopFsRelation(
                fileIndex: InMemoryFileIndex,
                partitionSchema,
                _,
                _,
                _,
                _),
              _,
              _,
              _)) if fsRelation.partitionSchema.nonEmpty =>
        val (partitionKeyFilters, dataFilter) =
          getPartitionKeyFiltersAndDataFilters(
            fsRelation.sparkSession,
            relation,
            partitionSchema,
            filters,
            relation.output)
        val prunedPartitionSize = fileIndex.listFiles(
          partitionKeyFilters.toSeq,
          dataFilter)
          .size
        if (prunedPartitionSize > maxScanPartitions) {
          throw maxPartitionExceedError(
            prunedPartitionSize,
            maxScanPartitions,
            relation.catalogTable,
            fileIndex.rootPaths,
            fsRelation.partitionSchema)
        }
      case ScanOperation(
            _,
            filters,
            logicalRelation @ LogicalRelation(
              fsRelation @ HadoopFsRelation(
                catalogFileIndex: CatalogFileIndex,
                partitionSchema,
                _,
                _,
                _,
                _),
              _,
              _,
              _)) if fsRelation.partitionSchema.nonEmpty =>
        val (partitionKeyFilters, _) =
          getPartitionKeyFiltersAndDataFilters(
            fsRelation.sparkSession,
            logicalRelation,
            partitionSchema,
            filters,
            logicalRelation.output)

        val prunedPartitionSize =
          catalogFileIndex.filterPartitions(
            partitionKeyFilters.toSeq)
            .partitionSpec()
            .partitions
            .size
        if (prunedPartitionSize > maxScanPartitions) {
          throw maxPartitionExceedError(
            prunedPartitionSize,
            maxScanPartitions,
            logicalRelation.catalogTable,
            catalogFileIndex.rootPaths,
            fsRelation.partitionSchema)
        }
      case _ =>
    }
  }

  def maxPartitionExceedError(
      prunedPartitionSize: Int,
      maxPartitionSize: Int,
      tableMeta: Option[CatalogTable],
      rootPaths: Seq[Path],
      partitionSchema: StructType): Throwable = {
    val truncatedPaths =
      if (rootPaths.length > 5) {
        rootPaths.slice(0, 5).mkString(",") + """... """ + (rootPaths.length - 5) + " more paths"
      } else {
        rootPaths.mkString(",")
      }

    new MaxPartitionExceedException(
      s"""
         |SQL job scan data source partition: $prunedPartitionSize
         |exceed restrict of data source scan maxPartition $maxPartitionSize
         |You should optimize your SQL logical according partition structure
         |or shorten query scope such as p_date, detail as below:
         |Table: ${tableMeta.map(_.qualifiedName).getOrElse("")}
         |Owner: ${tableMeta.map(_.owner).getOrElse("")}
         |RootPaths: $truncatedPaths
         |Partition Structure: ${partitionSchema.map(_.name).mkString(", ")}
         |""".stripMargin)
  }
}
