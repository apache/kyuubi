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

import java.util.Locale

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.HiveClientImpl
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import org.apache.kyuubi.spark.connector.hive.{HiveConnectorUtils, KyuubiHiveConnectorException}

case class HiveScan(
    sparkSession: SparkSession,
    fileIndex: HiveCatalogFileIndex,
    catalogTable: CatalogTable,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter] = Array.empty,
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty) extends FileScan {

  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis

  private val partFileToHivePartMap: mutable.Map[PartitionedFile, CatalogTablePartition] =
    mutable.Map()

  override def isSplitable(path: Path): Boolean = {
    catalogTable.provider.map(_.toUpperCase(Locale.ROOT)).exists {
      case "PARQUET" => true
      case "ORC" => true
      case "HIVE" => isHiveOrcOrParquet(catalogTable.storage)
      case _ => super.isSplitable(path)
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val hiveConf = new Configuration(fileIndex.hiveCatalog.hadoopConfiguration())
    addCatalogTableConfToConf(hiveConf, catalogTable)

    val table = HiveClientImpl.toHiveTable(catalogTable)
    HiveReader.initializeHiveConf(table, hiveConf, dataSchema, readDataSchema)
    val broadcastHiveConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hiveConf))

    HivePartitionReaderFactory(
      sparkSession.sessionState.conf.clone(),
      broadcastHiveConf,
      table,
      dataSchema,
      readDataSchema,
      readPartitionSchema,
      partFileToHivePartMap.toMap,
      pushedFilters = pushedFilters)
  }

  override protected def partitions: Seq[FilePartition] = {
    val (selectedPartitions, partDirToHivePartMap) =
      fileIndex.listHiveFiles(partitionFilters, dataFilters)
    val maxSplitBytes = FilePartition.maxSplitBytes(sparkSession, selectedPartitions)
    val partitionAttributes = toAttributes(fileIndex.partitionSchema)
    val attributeMap = partitionAttributes.map(a => normalizeName(a.name) -> a).toMap
    val readPartitionAttributes = readPartitionSchema.map { readField =>
      attributeMap.getOrElse(
        normalizeName(readField.name),
        throw KyuubiHiveConnectorException(s"Can't find required partition " +
          s"column ${readField.name} in partition schema ${fileIndex.partitionSchema}"))
    }
    lazy val partitionValueProject =
      GenerateUnsafeProjection.generate(readPartitionAttributes, partitionAttributes)
    val splitFiles: Seq[PartitionedFile] = selectedPartitions.flatMap { partition =>
      val partitionValues =
        if (readPartitionAttributes != partitionAttributes) {
          partitionValueProject(partition.values).copy()
        } else {
          partition.values
        }
      partition.files.flatMap { file =>
        val filePath = file.getPath
        val partFiles = HiveConnectorUtils.splitFiles(
          sparkSession = sparkSession,
          file = file,
          filePath = filePath,
          isSplitable = isSplitable(filePath),
          maxSplitBytes = maxSplitBytes,
          partitionValues = partitionValues)

        if (partDirToHivePartMap.contains(partition)) {
          partFiles.foreach { partFile =>
            partFileToHivePartMap += (partFile -> partDirToHivePartMap(partition))
          }
        }
        partFiles
      }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }

    if (splitFiles.length == 1) {
      val path = new Path(HiveConnectorUtils.partitionedFilePath(splitFiles(0)))
      if (!isSplitable(path) && splitFiles(0).length >
          sparkSession.sparkContext.getConf.getOption("spark.io.warning.largeFileThreshold")
            .getOrElse("1024000000").toLong) {
        logWarning(s"Loading one large unsplittable file ${path.toString} with only one " +
          s"partition, the reason is: ${getFileUnSplittableReason(path)}")
      }
    }

    FilePartition.getFilePartitions(sparkSession, splitFiles, maxSplitBytes)
  }

  private def addCatalogTableConfToConf(hiveConf: Configuration, table: CatalogTable): Unit = {
    table.properties.foreach {
      case (key, value) =>
        hiveConf.set(key, value)
    }
  }

  private def normalizeName(name: String): String = {
    if (isCaseSensitive) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
  }

  private def isHiveOrcOrParquet(storage: CatalogStorageFormat): Boolean = {
    val serde = storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
    serde.contains("parquet") || serde.contains("orc")
  }

  def toAttributes(structType: StructType): Seq[AttributeReference] =
    structType.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
}
