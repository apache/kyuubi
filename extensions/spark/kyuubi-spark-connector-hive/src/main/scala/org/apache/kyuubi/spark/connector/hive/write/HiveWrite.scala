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

package org.apache.kyuubi.spark.connector.hive.write

import java.util.{Locale, UUID}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.ErrorMsg
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, Write}
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, WriteJobDescription}
import org.apache.spark.sql.execution.datasources.v2.FileBatchWrite
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.execution.{HiveFileFormat, HiveOptions}
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.{hiveClientImpl, FileSinkDesc, StructTypeHelper}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import org.apache.kyuubi.spark.connector.hive.{HiveTableCatalog, KyuubiHiveConnectorException}
import org.apache.kyuubi.spark.connector.hive.write.HiveWriteHelper.getPartitionSpec

case class HiveWrite(
    sparkSession: SparkSession,
    table: CatalogTable,
    info: LogicalWriteInfo,
    hiveTableCatalog: HiveTableCatalog,
    forceOverwrite: Boolean,
    partition: Map[String, Option[String]],
    ifPartitionNotExists: Boolean) extends Write with Logging {

  private val options = info.options()

  private val hiveTable = hiveClientImpl.toHiveTable(table)

  private val hadoopConf = hiveTableCatalog.hadoopConfiguration()

  private val externalCatalog = hiveTableCatalog.externalCatalog

  private val tableLocation = hiveTable.getDataLocation

  private val allColumns = info.schema().toAttributes
  private val dataColumns = allColumns.take(allColumns.length - hiveTable.getPartCols.size())
  private val partColumns = allColumns.takeRight(hiveTable.getPartCols.size())

  lazy val tableDesc: TableDesc = new TableDesc(
    hiveTable.getInputFormatClass,
    hiveTable.getOutputFormatClass,
    hiveTable.getMetadata)

  override def description(): String = "Kyuubi-Hive-Connector"

  override def toBatch: BatchWrite = {
    val tmpLocation = HiveWriteHelper.getExternalTmpPath(sparkSession, hadoopConf, tableLocation)

    val fileSinkConf = new FileSinkDesc(tmpLocation.toString, tableDesc, false)
    handleCompression(fileSinkConf, hadoopConf)

    val partitionColumnNames = extractAndValidatePartitionCols(fileSinkConf, hadoopConf)

    val committer = FileCommitProtocol.instantiate(
      className = sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = java.util.UUID.randomUUID().toString,
      outputPath = tmpLocation.toString)

    val job = getJobInstance(hadoopConf, tmpLocation)

    val description = createWriteJobDescription(
      fileSinkConf,
      sparkSession,
      hadoopConf,
      job,
      tmpLocation.toString,
      Map.empty,
      options.asScala.toMap)

    committer.setupJob(job)

    new HiveBatchWrite(
      sparkSession,
      table,
      hiveTableCatalog,
      Some(tmpLocation),
      partition,
      partitionColumnNames,
      forceOverwrite,
      ifPartitionNotExists,
      hadoopConf,
      new FileBatchWrite(job, description, committer),
      externalCatalog,
      description,
      committer)
  }

  private def createWriteJobDescription(
      fileSinkConf: FileSinkDesc,
      sparkSession: SparkSession,
      hadoopConf: Configuration,
      job: Job,
      pathName: String,
      customPartitionLocations: Map[TablePartitionSpec, String],
      options: Map[String, String]): WriteJobDescription = {
    val hiveFileFormat = new HiveFileFormat(fileSinkConf)
    val dataSchema = StructType(info.schema().fields.take(dataColumns.length))
    val outputWriterFactory = hiveFileFormat.prepareWrite(sparkSession, job, options, dataSchema)
    val metrics: Map[String, SQLMetric] = BasicWriteJobStatsTracker.metrics
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    val statsTracker = new BasicWriteJobStatsTracker(serializableHadoopConf, metrics)

    new WriteJobDescription(
      uuid = UUID.randomUUID().toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = allColumns,
      dataColumns = dataColumns,
      partitionColumns = partColumns,
      bucketSpec = None,
      path = pathName,
      customPartitionLocations = customPartitionLocations,
      maxRecordsPerFile = sparkSession.sessionState.conf.maxRecordsPerFile,
      timeZoneId = sparkSession.sessionState.conf.sessionLocalTimeZone,
      statsTrackers = Seq(statsTracker))
  }

  private def getJobInstance(hadoopConf: Configuration, path: Path): Job = {
    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, path)
    job
  }

  private def handleCompression(fileSinkConf: FileSinkDesc, hadoopConf: Configuration): Unit = {
    val isCompressed =
      fileSinkConf.getTableInfo.getOutputFileFormatClassName.toLowerCase(Locale.ROOT) match {
        case formatName if formatName.endsWith("orcoutputformat") =>
          // For ORC,"mapreduce.output.fileoutputformat.compress",
          // "mapreduce.output.fileoutputformat.compress.codec", and
          // "mapreduce.output.fileoutputformat.compress.type"
          // have no impact because it uses table properties to store compression information.
          false
        case _ => hadoopConf.get("hive.exec.compress.output", "false").toBoolean
      }

    if (isCompressed) {
      hadoopConf.set("mapreduce.output.fileoutputformat.compress", "true")
      fileSinkConf.setCompressed(true)
      fileSinkConf.setCompressCodec(hadoopConf
        .get("mapreduce.output.fileoutputformat.compress.codec"))
      fileSinkConf.setCompressType(hadoopConf
        .get("mapreduce.output.fileoutputformat.compress.type"))
    } else {
      // Set compression by priority
      HiveOptions.getHiveWriteCompression(fileSinkConf.getTableInfo, sparkSession.sessionState.conf)
        .foreach { case (compression, codec) => hadoopConf.set(compression, codec) }
    }
  }

  private def extractAndValidatePartitionCols(
      fileSinkConf: FileSinkDesc,
      hadoopConf: Configuration): Seq[String] = {
    val partitionSpec = getPartitionSpec(partition)
    val numDynamicPartitions = partitionSpec.values.count(_.equals(""))
    val numStaticPartitions = partitionSpec.size - numDynamicPartitions

    // All partition column names in the format of "<column name 1>/<column name 2>/..."
    val partitionColumns = fileSinkConf.getTableInfo.getProperties.getProperty("partition_columns")
    val partitionColumnNames = Option(partitionColumns).map(_.split("/")).getOrElse(Array.empty)

    // Validate partition spec if there exist any dynamic partitions
    if (numDynamicPartitions > 0) {
      // Report error if dynamic partitioning is not enabled
      if (!hadoopConf.get("hive.exec.dynamic.partition", "true").toBoolean) {
        throw KyuubiHiveConnectorException(ErrorMsg.DYNAMIC_PARTITION_DISABLED.getMsg)
      }

      // Report error if dynamic partition strict mode is on but no static partition is found
      if (numStaticPartitions == 0 &&
        hadoopConf.get("hive.exec.dynamic.partition.mode", "strict").equalsIgnoreCase("strict")) {
        throw KyuubiHiveConnectorException(ErrorMsg.DYNAMIC_PARTITION_STRICT_MODE.getMsg)
      }

      // Report error if any static partition appears after a dynamic partition
      val isDynamic = partitionColumnNames.map(partitionSpec(_).equals(""))
      if (isDynamic.init.zip(isDynamic.tail).contains((true, false))) {
        throw KyuubiHiveConnectorException(ErrorMsg.PARTITION_DYN_STA_ORDER.getMsg)
      }
    }
    partitionColumnNames
  }
}
