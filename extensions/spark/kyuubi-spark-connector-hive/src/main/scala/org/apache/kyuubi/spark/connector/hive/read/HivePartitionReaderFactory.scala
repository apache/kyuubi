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

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition, Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.{HiveClientImpl, NextIterator}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

import org.apache.kyuubi.spark.connector.hive.HiveConnectorUtils

case class HivePartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastHiveConf: Broadcast[SerializableConfiguration],
    hiveTable: HiveTable,
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    partFileToHivePart: Map[PartitionedFile, CatalogTablePartition],
    pushedFilters: Array[Filter] = Array.empty)
  extends PartitionReaderFactory with Logging {

  private val charset: String =
    sqlConf.getConfString("hive.exec.default.charset", "utf-8")

  val tableDesc = HiveReader.getTableDec(hiveTable)
  val nonPartitionReadDataKeys = HiveReader.toAttributes(readDataSchema)

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    assert(partition.isInstanceOf[FilePartition])
    val filePartition = partition.asInstanceOf[FilePartition]
    val iter: Iterator[HivePartitionedFileReader[InternalRow]] =
      filePartition.files.toIterator.map { file =>
        val bindHivePart = partFileToHivePart.get(file)
        val hivePartition = bindHivePart.map(HiveClientImpl.toHivePartition(_, hiveTable))
        HivePartitionedFileReader(
          file,
          new PartitionReaderWithPartitionValues(
            HivePartitionedReader(
              file,
              buildReaderInternal(file, hivePartition),
              tableDesc,
              broadcastHiveConf,
              nonPartitionReadDataKeys,
              hivePartition,
              charset),
            readDataSchema,
            partitionSchema,
            file.partitionValues))
      }
    new SparkFilePartitionReader[InternalRow](iter)
  }

  private def buildReaderInternal(
      file: PartitionedFile,
      bindPartition: Option[HivePartition]): PartitionReader[Writable] = {
    val reader = createPartitionWritableReader(file, bindPartition)
    val fileReader = new PartitionReader[Writable] {
      override def next(): Boolean = reader.hasNext
      override def get(): Writable = reader.next()
      override def close(): Unit = {}
    }
    fileReader
  }

  private def createPartitionWritableReader[T](
      file: PartitionedFile,
      bindPartition: Option[HivePartition]): Iterator[Writable] = {
    // Obtain binding HivePartition from input partitioned file
    val ifc = bindPartition.map(Utilities.getPartitionDesc) match {
      case Some(partDesc) =>
        partDesc.getInputFileFormatClass
          .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
      case None =>
        hiveTable.getInputFormatClass
          .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
    }

    val jobConf = new JobConf(broadcastHiveConf.value.value)

    val filePath = new Path(new URI(HiveConnectorUtils.partitionedFilePath(file)))

    if (tableDesc != null) {
      configureJobPropertiesForStorageHandler(tableDesc, jobConf, true)
      Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
    }

    val eventIter: NextIterator[Writable] = new NextIterator[Writable] {
      // Initial 'FileSplit' and 'InputFormat' instance for Record reader construction
      private val fileSplit = new FileSplit(filePath, file.start, file.length, Array.empty[String])
      private val inputFormat: InputFormat[Writable, Writable] =
        HiveReader.getInputFormat(ifc, jobConf)
      logDebug(s"Final input format $inputFormat")
      var reader: RecordReader[Writable, Writable] =
        try {
          inputFormat.getRecordReader(fileSplit.asInstanceOf[InputSplit], jobConf, Reporter.NULL)
        } catch {
          case e: Exception =>
            logError("Exception raised when creating iterator reader", e)
            throw e
        }
      val key: Writable =
        if (reader == null) null.asInstanceOf[Writable] else reader.createKey()
      val value: Writable =
        if (reader == null) null.asInstanceOf[Writable] else reader.createValue()

      override def getNext(): Writable = {
        try {
          finished = !reader.next(key, value)
        } catch {
          case e: Exception =>
            logError(s"Exception raised when reading corrupt file: $fileSplit", e)
            throw e
        }
        value
      }

      override def close(): Unit = {
        if (reader != null) {
          try {
            reader.close()
          } catch {
            case e @ (_: Exception | _: Throwable) =>
              logError("Exception in RecordReader.close()", e)
          } finally {
            reader = null
          }
        }
      }
    }

    Option(TaskContext.get())
      .foreach(_.addTaskCompletionListener[Unit](_ => eventIter.closeIfNeeded()))
    eventIter
  }

  def configureJobPropertiesForStorageHandler(
      tableDesc: TableDesc,
      conf: Configuration,
      input: Boolean): Unit = {
    val property = tableDesc.getProperties.getProperty(META_TABLE_STORAGE)
    val storageHandler =
      org.apache.hadoop.hive.ql.metadata.HiveUtils.getStorageHandler(conf, property)
    if (storageHandler != null) {
      val jobProperties = new java.util.LinkedHashMap[String, String]
      if (input) {
        storageHandler.configureInputJobProperties(tableDesc, jobProperties)
      } else {
        storageHandler.configureOutputJobProperties(tableDesc, jobProperties)
      }
      if (!jobProperties.isEmpty) {
        tableDesc.setJobProperties(jobProperties)
      }
    }
  }
}
