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

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog

/**
 * A ScanBuilder that mirrors Spark's built-in [[ParquetScanBuilder]] but builds
 * [[KyuubiParquetScan]] instances, which additionally implement
 * `SupportsRuntimeFiltering` so that Dynamic Partition Pruning works when
 * a Hive Parquet table goes through Spark's vectorized Parquet reader path.
 *
 * Filter, aggregate and column pushdown behaviour matches [[ParquetScanBuilder]].
 * Gap: on Spark 4.1+ [[ParquetScanBuilder]] also mixes in
 * `SupportsPushDownVariantExtractions`, which this builder does not support,
 * but this is unreachable in practice. Hive-serde Parquet tables have no way
 * to declare `variant` data columns (Hive 4.2 adds `variant` only for Iceberg
 * tables, see HIVE-29184).
 */
class KyuubiParquetScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap,
    catalogTable: CatalogTable,
    hiveTableCatalog: HiveTableCatalog)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema)
  with SupportsPushDownAggregates {

  /**
   * Starts from `hiveTableCatalog.hadoopConfiguration()` so per-catalog Hadoop
   * settings are honored. Cloned so per-scan `options` do not pollute the
   * shared catalog instance.
   *
   * Note: the catalog's `hadoopConfiguration()` is a `lazy val` snapshot of
   * `sessionState.newHadoopConf()` taken at first use. Session confs changed
   * after that snapshot may not reach reader code that reads straight from
   * the Hadoop `Configuration`.
   */
  lazy val hadoopConf: Configuration = {
    val conf = new Configuration(hiveTableCatalog.hadoopConfiguration())
    // Hadoop Configurations are case sensitive.
    options.asCaseSensitiveMap.asScala.foreach { case (k, v) => conf.set(k, v) }
    conf
  }

  private var finalSchema = new StructType()

  private var pushedAggregations = Option.empty[Aggregation]

  override protected val supportsNestedSchemaPruning: Boolean = true

  override def build(): KyuubiParquetScan = {
    // the `finalSchema` is either pruned in pushAggregation (if aggregates are
    // pushed down), or pruned in readDataSchema() (in regular column pruning). These
    // two are mutual exclusive.
    if (pushedAggregations.isEmpty) {
      finalSchema = readDataSchema()
    }
    new KyuubiParquetScan(
      sparkSession,
      hadoopConf,
      fileIndex,
      dataSchema,
      finalSchema,
      readPartitionSchema(),
      pushedDataFilters,
      options,
      pushedAggregations,
      partitionFilters,
      dataFilters,
      catalogTable)
  }

  override def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = {
    if (sparkSession.sessionState.conf.parquetFilterPushDown) {
      HiveBridgeHelper.parquetConvertibleFilters(readDataSchema(), dataFilters.toSeq).toArray
    } else {
      Array.empty[Filter]
    }
  }

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (!sparkSession.sessionState.conf.parquetAggregatePushDown) {
      return false
    }

    AggregatePushDownUtils.getSchemaForPushedAggregation(
      aggregation,
      schema,
      partitionNameSet,
      dataFilters) match {

      case Some(schema) =>
        finalSchema = schema
        this.pushedAggregations = Some(aggregation)
        true
      case _ => false
    }
  }
}
