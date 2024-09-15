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

package org.apache.kyuubi.sql.compact

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

case class CompactTableResolver(sparkSession: SparkSession) extends Rule[LogicalPlan] with Logging {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan match {
      case RecoverCompactTableStatement(tableParts) =>
        RecoverCompactTable(UnresolvedRelation(CompactTableUtils.getTableIdentifier(tableParts)))

      case RecoverCompactTable(SubqueryAlias(
            _,
            LogicalRelation(
              _: HadoopFsRelation,
              _,
              Some(catalogTable),
              _))) =>
        RecoverCompactTableCommand(catalogTable)

      case CompactTableStatement(tableParts, targetSizeInMB, options) =>
        CompactTable(
          UnresolvedRelation(CompactTableUtils.getTableIdentifier(tableParts)),
          targetSizeInMB.map(ByteUnit.MiB.toBytes),
          options)
      case CompactTable(
            SubqueryAlias(
              _,
              logicalRelation @ LogicalRelation(
                _: HadoopFsRelation,
                _,
                Some(catalogTable),
                _)),
            targetSizeInBytes,
            options) =>
        createCacheCommand(
          logicalRelation,
          catalogTable,
          targetSizeInBytes,
          options)

      case CompactTable(
            SubqueryAlias(_, hiveTableRelation: HiveTableRelation),
            targetSizeInBytes,
            options) =>
        createCacheCommand(
          hiveTableRelation,
          hiveTableRelation.tableMeta,
          targetSizeInBytes,
          options)
      case _ => plan
    }

  private def createCacheCommand(
      relation: LeafNode,
      catalogTable: CatalogTable,
      targetSizeInBytes: Option[Long],
      options: CompactTableOption): CachePerformanceViewCommand = {

    val smallFileCollect = SmallFileCollect(relation, targetSizeInBytes)
    val repartitionByExpression =
      RepartitionByExpression(Seq(smallFileCollect.output.head), smallFileCollect, None)
    val smallFileMerge =
      SmallFileMerge(repartitionByExpression, options == CompactTableOptions.DryRun)
    val originalFileLocation = CompactTableUtils.getCompactDataDir(catalogTable.storage)
    CachePerformanceViewCommand(
      Seq(CompactTable.mergedFilesCachedTableName),
      smallFileMerge,
      originalFileLocation,
      options)
  }
}
