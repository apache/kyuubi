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

import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.types.StructType

/**
 * Hard prerequisite for [[KyuubiParquetScan]]'s DPP support on Spark 3.5+.
 * [[org.apache.spark.sql.connector.read.Scan.ColumnarSupportMode]] is
 * introduced in Spark 3.5.0 (SPARK-44505). A no-op trait lives under
 * `src/main/scala-spark-pre-3.5/` for Spark 3.3 / 3.4.
 *
 * With the default [[Scan.ColumnarSupportMode.PARTITION_DEFINED]],
 * `DataSourceV2ScanExecBase.supportsColumnar` materialises `inputPartitions`
 * during planning to probe each partition's reader factory, which through
 * `FileScan.partitions` -> `HiveCatalogFileIndex.listFiles` triggers a
 * full-table HDFS listing before runtime filters arrive via
 * [[org.apache.spark.sql.connector.read.SupportsRuntimeFiltering.filter]].
 * DPP still prunes the data scan correctly, but its end-to-end win is
 * cancelled by this wasted pre-DPP listing. Vanilla Spark sidesteps the
 * issue because Hive Parquet/ORC tables fall back to V1 `FileSourceScanExec`
 * and never consult [[Scan.columnarSupportMode]].
 */
trait KyuubiParquetColumnarMixin { this: ParquetScan =>

  /**
   * The decision returned here is semantically identical to Spark's
   * `ParquetPartitionReaderFactory.supportColumnarReads`, so advertising it
   * at scan-level is safe.
   */
  override def columnarSupportMode(): Scan.ColumnarSupportMode = {
    val sqlConf = sparkSession.sessionState.conf
    val schema = StructType(readDataSchema.fields ++ readPartitionSchema.fields)
    val supportsColumnar = ParquetUtils.isBatchReadSupportedForSchema(sqlConf, schema) &&
      sqlConf.wholeStageEnabled &&
      !WholeStageCodegenExec.isTooManyFields(sqlConf, schema)
    if (supportsColumnar) Scan.ColumnarSupportMode.SUPPORTED
    else Scan.ColumnarSupportMode.UNSUPPORTED
  }
}
