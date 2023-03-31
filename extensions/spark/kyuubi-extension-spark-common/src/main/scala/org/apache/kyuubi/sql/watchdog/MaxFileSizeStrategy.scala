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

import org.apache.spark.sql.{PruneFileSourcePartitionHelper, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, HadoopFsRelation, InMemoryFileIndex, LogicalRelation}

import org.apache.kyuubi.sql.KyuubiSQLConf

/**
 * Add maxFileSize Strategy to avoid scan excessive size of files
 * 1 Check if scan exceed maxFileSize
 * This Strategy Add Planner Strategy after LogicalOptimizer
 */
case class MaxFileSizeStrategy(session: SparkSession)
  extends Strategy
  with SQLConfHelper
  with PruneFileSourcePartitionHelper {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val maxFileSizeOpt = conf.getConf(KyuubiSQLConf.WATCHDOG_MAX_FILE_SIZE)
    if (maxFileSizeOpt.isDefined) {
      checkRelationMaxFileSize(plan, maxFileSizeOpt.get)
    }
    Nil
  }

  private def checkRelationMaxFileSize(
      plan: LogicalPlan,
      maxFileSize: Long): Unit = {
    plan match {
      case ScanOperation(_, _, relation: HiveTableRelation) =>
        if (relation.isPartitioned) {
          relation.prunedPartitions match {
            case Some(prunedPartitions) =>
              val scanFileSize = prunedPartitions.flatMap(_.stats).map(_.sizeInBytes).sum
              if (scanFileSize > maxFileSize) {
                throw partitionTableExceedError(
                  scanFileSize,
                  maxFileSize,
                  Some(relation.tableMeta),
                  prunedPartitions.flatMap(_.storage.locationUri).map(_.toString),
                  relation.partitionCols.map(_.name))
              }
            case _ =>
              val totalPartitions = session
                .sessionState.catalog.externalCatalog.listPartitions(
                  relation.tableMeta.database,
                  relation.tableMeta.identifier.table)
              val scanFileSize = totalPartitions.flatMap(_.stats).map(_.sizeInBytes).sum
              if (scanFileSize > maxFileSize) {
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
          val scanFileSize = relation.tableMeta.stats.map(_.sizeInBytes).sum
          if (scanFileSize > maxFileSize) {
            throw nonPartitionTableExceedError(
              scanFileSize,
              maxFileSize,
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
          val scanFileSize = fileIndex.listFiles(
            partitionKeyFilters.toSeq,
            dataFilter).flatMap(_.files).map(_.getLen).sum
          if (scanFileSize > maxFileSize) {
            throw partitionTableExceedError(
              scanFileSize,
              maxFileSize,
              relation.catalogTable,
              fileIndex.rootPaths.map(_.toString),
              fsRelation.partitionSchema.map(_.name))
          }
        } else {
          val scanFileSize = fileIndex.sizeInBytes
          if (scanFileSize > maxFileSize) {
            throw nonPartitionTableExceedError(
              scanFileSize,
              maxFileSize,
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
          val scanFileSize = catalogFileIndex.filterPartitions(
            partitionKeyFilters.toSeq)
            .listFiles(Nil, Nil).flatMap(_.files).map(_.getLen).sum

          if (scanFileSize > maxFileSize) {
            throw partitionTableExceedError(
              scanFileSize,
              maxFileSize,
              logicalRelation.catalogTable,
              catalogFileIndex.rootPaths.map(_.toString),
              fsRelation.partitionSchema.map(_.name))
          }
        } else {
          val scanFileSize = catalogFileIndex.sizeInBytes
          if (scanFileSize > maxFileSize) {
            throw nonPartitionTableExceedError(
              scanFileSize,
              maxFileSize,
              logicalRelation.catalogTable)
          }
        }
      case _ =>
    }
  }

  private def partitionTableExceedError(
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
         |exceed restrict of data source scan maxFileSize $maxFileSize
         |You should optimize your SQL logical according partition structure
         |or shorten query scope such as p_date, detail as below:
         |Table: ${tableMeta.map(_.qualifiedName).getOrElse("")}
         |Owner: ${tableMeta.map(_.owner).getOrElse("")}
         |RootPaths: $truncatedPaths
         |Partition Structure: ${partitions.mkString(", ")}
         |""".stripMargin)
  }

  private def nonPartitionTableExceedError(
      scanFileSize: Number,
      maxFileSize: Long,
      tableMeta: Option[CatalogTable]): Throwable = {
    new MaxFileSizeExceedException(
      s"""
         |SQL job scan file size in bytes: $scanFileSize
         |exceed restrict of data source scan maxFileSize $maxFileSize
         |detail as below:
         |Table: ${tableMeta.map(_.qualifiedName).getOrElse("")}
         |Owner: ${tableMeta.map(_.owner).getOrElse("")}
         |Location: ${tableMeta.map(_.location).getOrElse("")}
         |""".stripMargin)
  }
}
