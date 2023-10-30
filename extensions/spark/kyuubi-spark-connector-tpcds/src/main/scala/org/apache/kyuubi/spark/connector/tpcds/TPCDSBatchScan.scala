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

package org.apache.kyuubi.spark.connector.tpcds

import java.util.OptionalLong

import io.trino.tpcds._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types._

case class TPCDSTableChunk(table: String, scale: Double, parallelism: Int, index: Int)
  extends InputPartition

class TPCDSBatchScan(
    @transient table: Table,
    scale: Double,
    schema: StructType,
    readConf: TPCDSReadConf) extends ScanBuilder
  with SupportsReportStatistics with Batch with Serializable {

  private val _sizeInBytes: Long = TPCDSStatisticsUtils.sizeInBytes(table, scale)
  private val _numRows: Long = TPCDSStatisticsUtils.numRows(table, scale)

  // Tables with fewer than 1000000 are not parallelized,
  // the limit made in `io.trino.tpcds.Parallel#splitWork`.
  private val parallelism: Int =
    if (table.isSmall) 1
    else math.max(
      SparkSession.active.sparkContext.defaultParallelism,
      (_sizeInBytes / readConf.maxPartitionBytes).ceil.toInt)

  override def build: Scan = this

  override def toBatch: Batch = this

  override def description: String =
    s"Scan TPC-DS ${TPCDSSchemaUtils.dbName(scale)}.${table.getName}, " +
      s"count: ${_numRows}, parallelism: $parallelism"

  override def readSchema: StructType = schema

  override def planInputPartitions: Array[InputPartition] =
    (1 to parallelism).map { i => TPCDSTableChunk(table.getName, scale, parallelism, i) }.toArray

  def createReaderFactory: PartitionReaderFactory = (partition: InputPartition) => {
    val chuck = partition.asInstanceOf[TPCDSTableChunk]
    new TPCDSPartitionReader(chuck.table, chuck.scale, chuck.parallelism, chuck.index, schema)
  }

  override def estimateStatistics: Statistics = new Statistics {
    override def sizeInBytes: OptionalLong = OptionalLong.of(_sizeInBytes)
    override def numRows: OptionalLong = OptionalLong.of(_numRows)
  }
}

class TPCDSPartitionReader(
    table: String,
    scale: Double,
    parallelism: Int,
    index: Int,
    schema: StructType) extends PartitionReader[InternalRow] {

  private val chuckInfo: Session = {
    val opt = new Options
    opt.table = table
    opt.scale = scale
    opt.parallelism = parallelism
    opt.toSession.withChunkNumber(index)
  }

  private val iterator = KyuubiTPCDSResults
    .constructResults(chuckInfo.getOnlyTableToGenerate, chuckInfo, schema)
    .iterator

  private var currentRow: InternalRow = _

  override def next(): Boolean = {
    val hasNext = iterator.hasNext
    if (hasNext) currentRow = iterator.next()
    hasNext
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {}
}
