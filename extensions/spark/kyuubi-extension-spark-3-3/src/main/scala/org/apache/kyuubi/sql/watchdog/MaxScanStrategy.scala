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
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.sql.KyuubiSQLConf

/**
 * Add MaxScanStrategy to avoid scan excessive partitions or files
 * 1. Check if scan exceed maxPartition of partitioned table
 * 2. Check if scan exceed maxFileSize (calculated by hive table and partition statistics)
 * This Strategy Add Planner Strategy after LogicalOptimizer
 * @param session
 */
case class MaxScanStrategy(session: SparkSession)
  extends Strategy
  with SQLConfHelper
  with PruneFileSourcePartitionHelper {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val maxScanPartitionsOpt = conf.getConf(KyuubiSQLConf.WATCHDOG_MAX_PARTITIONS)
    val maxFileSizeOpt = conf.getConf(KyuubiSQLConf.WATCHDOG_MAX_FILE_SIZE)
    if (maxScanPartitionsOpt.isDefined || maxFileSizeOpt.isDefined) {
      checkScan(plan, maxScanPartitionsOpt, maxFileSizeOpt)
    }
    Nil
  }

  private def checkScan(
      plan: LogicalPlan,
      maxScanPartitionsOpt: Option[Int],
      maxFileSizeOpt: Option[Long]): Unit = {
    plan match {
      case ScanOperation(_, _, relation: HiveTableRelation) =>
        if (relation.isPartitioned) {
          relation.prunedPartitions match {
            case Some(prunedPartitions) =>
              if (maxScanPartitionsOpt.exists(_ < prunedPartitions.size)) {
                throw new MaxPartitionExceedException(
                  s"""
                     |SQL job scan hive partition: ${prunedPartitions.size}
                     |exceed restrict of hive scan maxPartition ${maxScanPartitionsOpt.get}
                     |You should optimize your SQL logical according partition structure
                     |or shorten query scope such as p_date, detail as below:
                     |Table: ${relation.tableMeta.qualifiedName}
                     |Owner: ${relation.tableMeta.owner}
                     |Partition Structure: ${relation.partitionCols.map(_.name).mkString(", ")}
                     |""".stripMargin)
              }
              lazy val scanFileSize = prunedPartitions.flatMap(_.stats).map(_.sizeInBytes).sum
              if (maxFileSizeOpt.exists(_ < scanFileSize)) {
                throw partTableMaxFileExceedError(
                  scanFileSize,
                  maxFileSizeOpt.get,
                  Some(relation.tableMeta),
                  prunedPartitions.flatMap(_.storage.locationUri).map(_.toString),
                  relation.partitionCols.map(_.name))
              }
            case _ =>
              lazy val scanPartitions: Int = session
                .sessionState.catalog.externalCatalog.listPartitionNames(
                  relation.tableMeta.database,
                  relation.tableMeta.identifier.table).size
              if (maxScanPartitionsOpt.exists(_ < scanPartitions)) {
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

              lazy val scanFileSize: BigInt =
                relation.tableMeta.stats.map(_.sizeInBytes).getOrElse {
                  session
                    .sessionState.catalog.externalCatalog.listPartitions(
                      relation.tableMeta.database,
                      relation.tableMeta.identifier.table).flatMap(_.stats).map(_.sizeInBytes).sum
                }
              if (maxFileSizeOpt.exists(_ < scanFileSize)) {
                throw new MaxFileSizeExceedException(
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
        } else {
          lazy val scanFileSize = relation.tableMeta.stats.map(_.sizeInBytes).sum
          if (maxFileSizeOpt.exists(_ < scanFileSize)) {
            throw nonPartTableMaxFileExceedError(
              scanFileSize,
              maxFileSizeOpt.get,
              Some(relation.tableMeta))
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
              _)) =>
        if (fsRelation.partitionSchema.nonEmpty) {
          val (partitionKeyFilters, dataFilter) =
            getPartitionKeyFiltersAndDataFilters(
              fsRelation.sparkSession,
              relation,
              partitionSchema,
              filters,
              relation.output)
          val prunedPartitions = fileIndex.listFiles(
            partitionKeyFilters.toSeq,
            dataFilter)
          if (maxScanPartitionsOpt.exists(_ < prunedPartitions.size)) {
            throw maxPartitionExceedError(
              prunedPartitions.size,
              maxScanPartitionsOpt.get,
              relation.catalogTable,
              fileIndex.rootPaths,
              fsRelation.partitionSchema)
          }
          lazy val scanFileSize = prunedPartitions.flatMap(_.files).map(_.getLen).sum
          if (maxFileSizeOpt.exists(_ < scanFileSize)) {
            throw partTableMaxFileExceedError(
              scanFileSize,
              maxFileSizeOpt.get,
              relation.catalogTable,
              fileIndex.rootPaths.map(_.toString),
              fsRelation.partitionSchema.map(_.name))
          }
        } else {
          lazy val scanFileSize = fileIndex.sizeInBytes
          if (maxFileSizeOpt.exists(_ < scanFileSize)) {
            throw nonPartTableMaxFileExceedError(
              scanFileSize,
              maxFileSizeOpt.get,
              relation.catalogTable)
          }
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
              _)) =>
        if (fsRelation.partitionSchema.nonEmpty) {
          val (partitionKeyFilters, _) =
            getPartitionKeyFiltersAndDataFilters(
              fsRelation.sparkSession,
              logicalRelation,
              partitionSchema,
              filters,
              logicalRelation.output)

          val fileIndex = catalogFileIndex.filterPartitions(
            partitionKeyFilters.toSeq)

          lazy val prunedPartitionSize = fileIndex.partitionSpec().partitions.size
          if (maxScanPartitionsOpt.exists(_ < prunedPartitionSize)) {
            throw maxPartitionExceedError(
              prunedPartitionSize,
              maxScanPartitionsOpt.get,
              logicalRelation.catalogTable,
              catalogFileIndex.rootPaths,
              fsRelation.partitionSchema)
          }

          lazy val scanFileSize = fileIndex
            .listFiles(Nil, Nil).flatMap(_.files).map(_.getLen).sum
          if (maxFileSizeOpt.exists(_ < scanFileSize)) {
            throw partTableMaxFileExceedError(
              scanFileSize,
              maxFileSizeOpt.get,
              logicalRelation.catalogTable,
              catalogFileIndex.rootPaths.map(_.toString),
              fsRelation.partitionSchema.map(_.name))
          }
        } else {
          lazy val scanFileSize = catalogFileIndex.sizeInBytes
          if (maxFileSizeOpt.exists(_ < scanFileSize)) {
            throw nonPartTableMaxFileExceedError(
              scanFileSize,
              maxFileSizeOpt.get,
              logicalRelation.catalogTable)
          }
        }
      case ScanOperation(
            _,
            _,
            relation @ DataSourceV2ScanRelation(_, _, _, _)) =>
        val table = relation.relation.table
        if (table.partitioning().nonEmpty) {
          val partitionColumnNames = table.partitioning().map(_.describe())
          val stats = relation.computeStats()
          lazy val scanFileSize = stats.sizeInBytes
          if (maxFileSizeOpt.exists(_ < scanFileSize)) {
            throw new MaxFileSizeExceedException(
              s"""
                 |SQL job scan file size in bytes: $scanFileSize
                 |exceed restrict of table scan maxFileSize ${maxFileSizeOpt.get}
                 |You should optimize your SQL logical according partition structure
                 |or shorten query scope such as p_date, detail as below:
                 |Table: ${table.name()}
                 |Partition Structure: ${partitionColumnNames.mkString(",")}
                 |""".stripMargin)
          }
        } else {
          val stats = relation.computeStats()
          lazy val scanFileSize = stats.sizeInBytes
          if (maxFileSizeOpt.exists(_ < scanFileSize)) {
            throw new MaxFileSizeExceedException(
              s"""
                 |SQL job scan file size in bytes: $scanFileSize
                 |exceed restrict of table scan maxFileSize ${maxFileSizeOpt.get}
                 |detail as below:
                 |Table: ${table.name()}
                 |""".stripMargin)
          }
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

  private def partTableMaxFileExceedError(
      scanFileSize: Number,
      maxFileSize: Long,
      tableMeta: Option[CatalogTable],
      rootPaths: Seq[String],
      partitions: Seq[String]): Throwable = {
    val truncatedPaths =
      if (rootPaths.length > 5) {
        rootPaths.slice(0, 5).mkString(",") + """... """ + (rootPaths.length - 5) + " more paths"
      } else {
        rootPaths.mkString(",")
      }

    new MaxFileSizeExceedException(
      s"""
         |SQL job scan file size in bytes: $scanFileSize
         |exceed restrict of table scan maxFileSize $maxFileSize
         |You should optimize your SQL logical according partition structure
         |or shorten query scope such as p_date, detail as below:
         |Table: ${tableMeta.map(_.qualifiedName).getOrElse("")}
         |Owner: ${tableMeta.map(_.owner).getOrElse("")}
         |RootPaths: $truncatedPaths
         |Partition Structure: ${partitions.mkString(", ")}
         |""".stripMargin)
  }

  private def nonPartTableMaxFileExceedError(
      scanFileSize: Number,
      maxFileSize: Long,
      tableMeta: Option[CatalogTable]): Throwable = {
    new MaxFileSizeExceedException(
      s"""
         |SQL job scan file size in bytes: $scanFileSize
         |exceed restrict of table scan maxFileSize $maxFileSize
         |detail as below:
         |Table: ${tableMeta.map(_.qualifiedName).getOrElse("")}
         |Owner: ${tableMeta.map(_.owner).getOrElse("")}
         |Location: ${tableMeta.map(_.location).getOrElse("")}
         |""".stripMargin)
  }
}
