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

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.execution.datasources.{WriteJobDescription, WriteTaskResult}
import org.apache.spark.sql.execution.datasources.v2.FileBatchWrite
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.{hive, toSQLValue, HiveExternalCatalog}
import org.apache.spark.sql.types.StringType

import org.apache.kyuubi.spark.connector.hive.{HiveTableCatalog, KyuubiHiveConnectorException}
import org.apache.kyuubi.spark.connector.hive.write.HiveWriteHelper.getPartitionSpec

class HiveBatchWrite(
    sparkSession: SparkSession,
    table: CatalogTable,
    hiveTableCatalog: HiveTableCatalog,
    tmpLocation: Option[Path],
    partition: Map[String, Option[String]],
    partitionColumnNames: Seq[String],
    overwrite: Boolean,
    ifPartitionNotExists: Boolean,
    hadoopConf: Configuration,
    fileBatchWrite: FileBatchWrite,
    externalCatalog: ExternalCatalog,
    description: WriteJobDescription,
    committer: FileCommitProtocol) extends BatchWrite with Logging {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    FileWriterFactory(description, committer)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    fileBatchWrite.commit(messages)

    val results = messages.map(_.asInstanceOf[WriteTaskResult])
    val writtenPaths: Set[String] =
      results.map(_.summary.updatedPartitions).reduceOption(_ ++ _).getOrElse(Set.empty)

    try {
      commitToMetastore(writtenPaths)
    } finally {
      // Attempt to delete the staging directory and the inclusive files. If failed, the files are
      // expected to be dropped at the normal termination of VM since deleteOnExit is used.
      deleteExternalTmpPath(hadoopConf)
    }

    // un-cache this table.
    hiveTableCatalog.catalog.invalidateCachedTable(table.identifier)

    val catalog = hiveTableCatalog.catalog
    if (sparkSession.sessionState.conf.autoSizeUpdateEnabled) {
      val newTable = catalog.getTableMetadata(table.identifier)
      val newSize = CommandUtils.calculateTotalSize(sparkSession, newTable)
      val newStats = CatalogStatistics(sizeInBytes = newSize)
      catalog.alterTableStats(table.identifier, Some(newStats))
    } else if (table.stats.nonEmpty) {
      catalog.alterTableStats(table.identifier, None)
    } else {
      // In other cases, we still need to invalidate the table relation cache.
      catalog.refreshTable(table.identifier)
    }

  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    fileBatchWrite.abort(messages)
  }

  override def useCommitCoordinator(): Boolean = fileBatchWrite.useCommitCoordinator()

  protected def deleteExternalTmpPath(hadoopConf: Configuration): Unit = {
    // Attempt to delete the staging directory and the inclusive files. If failed, the files are
    // expected to be dropped at the normal termination of VM since deleteOnExit is used.
    try {
      tmpLocation.foreach { path =>
        val fs = path.getFileSystem(hadoopConf)
        if (fs.delete(path, true)) {
          // If we successfully delete the staging directory, remove it from FileSystem's cache.
          fs.cancelDeleteOnExit(path)
        }
      }
    } catch {
      case NonFatal(e) =>
        val stagingDir = hadoopConf.get("hive.exec.stagingdir", ".hive-staging")
        logWarning(s"Unable to delete staging directory: $stagingDir.\n" + e)
    }
  }

  private def commitToMetastore(writtenParts: Set[String]): Unit = {
    val numDynamicPartitions = partition.values.count(_.isEmpty)
    val partitionSpec = getPartitionSpec(partition)
    val staticPartitionSpec = partitionSpec.filter {
      case (_, v) => !v.equals("")
    }

    if (partition.isEmpty) {
      externalCatalog.loadTable(
        table.database,
        table.identifier.table,
        tmpLocation.get.toString, // TODO: URI
        overwrite,
        isSrcLocal = false)
      return
    }

    if (numDynamicPartitions > 0) {
      if (overwrite && table.tableType == CatalogTableType.EXTERNAL) {
        val numWrittenParts = writtenParts.size
        val maxDynamicPartitionsKey = HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS.varname
        val maxDynamicPartitions = hadoopConf.getInt(
          maxDynamicPartitionsKey,
          HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS.defaultIntVal)
        if (numWrittenParts > maxDynamicPartitions) {
          throw KyuubiHiveConnectorException(
            s"Number of dynamic partitions created is $numWrittenParts, " +
              s"which is more than $maxDynamicPartitions. " +
              s"To solve this try to set $maxDynamicPartitionsKey " +
              s"to at least $numWrittenParts.")
        }
        // SPARK-29295: When insert overwrite to a Hive external table partition, if the
        // partition does not exist, Hive will not check if the external partition directory
        // exists or not before copying files. So if users drop the partition, and then do
        // insert overwrite to the same partition, the partition will have both old and new
        // data. We construct partition path. If the path exists, we delete it manually.
        writtenParts.foreach { partPath =>
          val dpMap = partPath.split("/").map { part =>
            val splitPart = part.split("=")
            assert(splitPart.size == 2, s"Invalid written partition path: $part")
            ExternalCatalogUtils.unescapePathName(splitPart(0)) ->
              ExternalCatalogUtils.unescapePathName(splitPart(1))
          }.toMap

          val caseInsensitiveDpMap = CaseInsensitiveMap(dpMap)

          val updatedPartitionSpec = partition.map {
            case (key, Some(null)) => key -> ExternalCatalogUtils.DEFAULT_PARTITION_NAME
            case (key, Some(value)) => key -> value
            case (key, None) if caseInsensitiveDpMap.contains(key) =>
              key -> caseInsensitiveDpMap(key)
            case (key, _) =>
              throw KyuubiHiveConnectorException(
                s"Dynamic partition key ${toSQLValue(key, StringType)} " +
                  "is not among written partition paths.")
          }
          val partitionColumnNames = table.partitionColumnNames
          val tablePath = new Path(table.location)
          val partitionPath = ExternalCatalogUtils.generatePartitionPath(
            updatedPartitionSpec,
            partitionColumnNames,
            tablePath)

          val fs = partitionPath.getFileSystem(hadoopConf)
          if (fs.exists(partitionPath)) {
            if (!fs.delete(partitionPath, true)) {
              throw KyuubiHiveConnectorException(s"Cannot remove partition directory " +
                s"'$partitionPath'")
            }
          }
        }
      }

      val loadPath = ExternalCatalogUtils.generatePartitionPath(
        staticPartitionSpec,
        partitionColumnNames.take(partitionColumnNames.length - numDynamicPartitions),
        tmpLocation.get)

      externalCatalog.loadDynamicPartitions(
        db = table.database,
        table = table.identifier.table,
        loadPath.toString,
        partitionSpec,
        overwrite,
        numDynamicPartitions)
    } else {
      // scalastyle:off
      // ifNotExists is only valid with static partition, refer to
      // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-InsertingdataintoHiveTablesfromqueries
      // scalastyle:on
      val oldPart =
        externalCatalog.getPartitionOption(
          table.database,
          table.identifier.table,
          partitionSpec)

      var doHiveOverwrite = overwrite

      if (oldPart.isEmpty || !ifPartitionNotExists) {
        // SPARK-29295: When insert overwrite to a Hive external table partition, if the
        // partition does not exist, Hive will not check if the external partition directory
        // exists or not before copying files. So if users drop the partition, and then do
        // insert overwrite to the same partition, the partition will have both old and new
        // data. We construct partition path. If the path exists, we delete it manually.
        val partitionPath =
          if (oldPart.isEmpty && overwrite
            && table.tableType == CatalogTableType.EXTERNAL) {
            val partitionColumnNames = table.partitionColumnNames
            val tablePath = new Path(table.location)
            Some(ExternalCatalogUtils.generatePartitionPath(
              partitionSpec,
              partitionColumnNames,
              tablePath))
          } else {
            oldPart.flatMap(_.storage.locationUri.map(uri => new Path(uri)))
          }

        import hive._
        // SPARK-18107: Insert overwrite runs much slower than hive-client.
        // Newer Hive largely improves insert overwrite performance. As Spark uses older Hive
        // version and we may not want to catch up new Hive version every time. We delete the
        // Hive partition first and then load data file into the Hive partition.
        val hiveVersion = externalCatalog.asInstanceOf[ExternalCatalogWithListener]
          .unwrapped.asInstanceOf[HiveExternalCatalog]
          .client
          .version
        // SPARK-31684:
        // For Hive 2.0.0 and onwards, as https://issues.apache.org/jira/browse/HIVE-11940
        // has been fixed, and there is no performance issue anymore. We should leave the
        // overwrite logic to hive to avoid failure in `FileSystem#checkPath` when the table
        // and partition locations do not belong to the same `FileSystem`
        // TODO(SPARK-31675): For Hive 2.2.0 and earlier, if the table and partition locations
        // do not belong together, we will still get the same error thrown by hive encryption
        // check. see https://issues.apache.org/jira/browse/HIVE-14380.
        // So we still disable for Hive overwrite for Hive 1.x for better performance because
        // the partition and table are on the same cluster in most cases.
        if (partitionPath.nonEmpty && overwrite && hiveVersion < v2_0) {
          partitionPath.foreach { path =>
            val fs = path.getFileSystem(hadoopConf)
            if (fs.exists(path)) {
              if (!fs.delete(path, true)) {
                throw new RuntimeException(s"Cannot remove partition directory '$partitionPath'")
              }
              // Don't let Hive do overwrite operation since it is slower.
              doHiveOverwrite = false
            }
          }
        }

        // inheritTableSpecs is set to true. It should be set to false for an IMPORT query
        // which is currently considered as a Hive native command.
        val inheritTableSpecs = true
        val loadPath = ExternalCatalogUtils.generatePartitionPath(
          staticPartitionSpec,
          partitionColumnNames.take(partitionColumnNames.length - numDynamicPartitions),
          tmpLocation.get)

        externalCatalog.loadPartition(
          table.database,
          table.identifier.table,
          loadPath.toString,
          partitionSpec,
          isOverwrite = doHiveOverwrite,
          inheritTableSpecs = inheritTableSpecs,
          isSrcLocal = false)
      }
    }
  }
}
