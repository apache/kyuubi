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

package org.apache.kyuubi.spark.connector.hive.read

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * An [[OrcScan]] that additionally implements [[SupportsRuntimeFiltering]],
 * enabling Spark's Dynamic Partition Pruning (DPP) to plug runtime IN predicates
 * into the Hive partitioned scan.
 *
 * Runtime filters arriving via [[filter]] are translated once into catalyst
 * partition predicates and merged with the existing [[partitionFilters]] every
 * time [[partitions]] is invoked, so the downstream [[HiveCatalogFileIndex]]
 * only has to resolve partition metadata for the pruned subset.
 *
 * Implementation note: a plain class extending Spark case class [[OrcScan]]
 * to reuse its methods (whose internals differ across supported Spark
 * versions). Two consequences of inheriting a case class:
 *  1. [[equals]] / [[hashCode]] are overridden to key on `getClass`, so a
 *     `KyuubiOrcScan` is never reused in place of a plain [[OrcScan]]
 *     during exchange/subquery reuse.
 *  2. The parent-synthesised `copy` is intentionally NOT used: it is generated
 *     against [[OrcScan]]'s own constructor and would silently drop the
 *     [[catalogTable]] field.
 */
class KyuubiOrcScan(
    _sparkSession: SparkSession,
    _hadoopConf: Configuration,
    _fileIndex: PartitioningAwareFileIndex,
    _dataSchema: StructType,
    _readDataSchema: StructType,
    _readPartitionSchema: StructType,
    _options: CaseInsensitiveStringMap,
    _pushedAggregate: Option[Aggregation],
    _pushedFilters: Array[Filter],
    _partitionFilters: Seq[Expression],
    _dataFilters: Seq[Expression],
    val catalogTable: CatalogTable)
  extends OrcScan(
    _sparkSession,
    _hadoopConf,
    _fileIndex,
    _dataSchema,
    _readDataSchema,
    _readPartitionSchema,
    _options,
    _pushedAggregate,
    _pushedFilters,
    _partitionFilters,
    _dataFilters)
  with SupportsRuntimeFiltering
  with KyuubiOrcColumnarMixin {

  private var runtimeFilters: Seq[Expression] = Seq.empty

  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis

  override def filterAttributes(): Array[NamedReference] = {
    HiveRuntimeFilterSupport.filterAttributes(readPartitionSchema.fieldNames.toSeq)
  }

  override def filter(filters: Array[Filter]): Unit = {
    runtimeFilters = HiveRuntimeFilterSupport.toCatalystPartitionFilters(
      filters,
      fileIndex.partitionSchema,
      isCaseSensitive)
    if (runtimeFilters.nonEmpty) {
      logInfo(s"Received ${runtimeFilters.length} runtime partition filter(s) for " +
        s"${catalogTable.identifier}")
      logDebug(s"Runtime partition filter(s) for ${catalogTable.identifier}: " +
        s"${runtimeFilters.mkString(", ")}")
    }
  }

  override protected def partitions: Seq[FilePartition] = {
    if (runtimeFilters.isEmpty) {
      super.partitions
    } else {
      // Inject runtime IN predicates into the partition-file planning step.
      // We delegate to a sibling OrcScan carrying the combined partitionFilters,
      // which keeps Spark's own partition planning logic fully reused.
      val sibling = OrcScan(
        sparkSession,
        hadoopConf,
        fileIndex,
        dataSchema,
        readDataSchema,
        readPartitionSchema,
        options,
        pushedAggregate,
        pushedFilters,
        partitionFilters ++ runtimeFilters,
        dataFilters)
      sibling.planInputPartitions().toSeq.map(_.asInstanceOf[FilePartition])
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case that: KyuubiOrcScan => super.equals(that)
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()
}
