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

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition, ExternalCatalogUtils}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, Predicate}
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.spark.connector.hive.{HiveConnectorUtils, HiveTableCatalog, KyuubiHiveConnectorException}

class HiveCatalogFileIndex(
    sparkSession: SparkSession,
    val catalogTable: CatalogTable,
    val hiveCatalog: HiveTableCatalog,
    override val sizeInBytes: Long)
  extends PartitioningAwareFileIndex(
    sparkSession,
    catalogTable.storage.properties,
    Some(catalogTable.schema)) {

  private val table = catalogTable

  private val partPathToBindHivePart: mutable.Map[PartitionPath, CatalogTablePartition] =
    mutable.Map()

  private val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)

  private val baseLocation: Option[URI] = table.storage.locationUri

  override def partitionSchema: StructType = table.partitionSchema

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val fileIndex = filterPartitions(partitionFilters)
    fileIndex.listFiles(partitionFilters, dataFilters)
  }

  private[hive] def listHiveFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression])
      : (Seq[PartitionDirectory], Map[PartitionDirectory, CatalogTablePartition]) = {
    val fileIndex = filterPartitions(partitionFilters)
    val partDirs = fileIndex.listFiles(partitionFilters, dataFilters)
    val partDirToHivePart = fileIndex.partDirToBindHivePartMap()
    (partDirs, partDirToHivePart)
  }

  def filterPartitions(filters: Seq[Expression]): HiveInMemoryFileIndex = {
    if (table.partitionColumnNames.nonEmpty) {
      val startTime = System.nanoTime()
      val selectedPartitions: Seq[BindPartition] = {
        val partitions =
          ExternalCatalogUtils.listPartitionsByFilter(
            sparkSession.sessionState.conf,
            hiveCatalog.catalog,
            table,
            filters)
        partitions.map(buildBindPartition)
      }

      val partitions = selectedPartitions.map {
        case BindPartition(catalogTablePartition) =>
          val path = new Path(catalogTablePartition.location)
          val fs = path.getFileSystem(hiveCatalog.hadoopConfiguration())
          val partPath = PartitionPath(
            catalogTablePartition.toRow(
              partitionSchema,
              sparkSession.sessionState.conf.sessionLocalTimeZone),
            path.makeQualified(fs.getUri, fs.getWorkingDirectory))
          partPathToBindHivePart += (partPath -> catalogTablePartition)
          partPath
      }
      val partitionSpec = PartitionSpec(partitionSchema, partitions)
      val timeNs = System.nanoTime() - startTime
      new HiveInMemoryFileIndex(
        sparkSession = sparkSession,
        rootPathsSpecified = partitionSpec.partitions.map(_.path),
        parameters = table.properties,
        partPathToBindHivePart = partPathToBindHivePart.toMap,
        userSpecifiedSchema = Some(partitionSpec.partitionColumns),
        fileStatusCache = fileStatusCache,
        userSpecifiedPartitionSpec = Some(partitionSpec),
        metadataOpsTimeNs = Some(timeNs),
        hadoopConf = hiveCatalog.hadoopConfiguration())
    } else {
      new HiveInMemoryFileIndex(
        sparkSession = sparkSession,
        rootPathsSpecified = rootPaths,
        parameters = table.properties,
        userSpecifiedSchema = None,
        fileStatusCache = fileStatusCache,
        hadoopConf = hiveCatalog.hadoopConfiguration())
    }
  }

  private def buildBindPartition(partition: CatalogTablePartition): BindPartition =
    BindPartition(partition)

  override def partitionSpec(): PartitionSpec = {
    throw notSupportOperator("partitionSpec")
  }

  override protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus] = {
    throw notSupportOperator("leafFiles")
  }

  override protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = {
    throw notSupportOperator("leafDirToChildrenFiles")
  }

  override def rootPaths: Seq[Path] = baseLocation.map(new Path(_)).toSeq

  override def refresh(): Unit = fileStatusCache.invalidateAll()

  def notSupportOperator(operator: String): UnsupportedOperationException = {
    new UnsupportedOperationException(s"Not support $operator in Hive file index.")
  }
}

class HiveInMemoryFileIndex(
    sparkSession: SparkSession,
    rootPathsSpecified: Seq[Path],
    parameters: Map[String, String],
    userSpecifiedSchema: Option[StructType],
    partPathToBindHivePart: Map[PartitionPath, CatalogTablePartition] = Map.empty,
    fileStatusCache: FileStatusCache = NoopCache,
    userSpecifiedPartitionSpec: Option[PartitionSpec] = None,
    override val metadataOpsTimeNs: Option[Long] = None,
    override protected val hadoopConf: Configuration)
  extends InMemoryFileIndex(
    sparkSession,
    rootPathsSpecified,
    parameters,
    userSpecifiedSchema,
    fileStatusCache,
    userSpecifiedPartitionSpec,
    metadataOpsTimeNs) {

  private val partDirToBindHivePart: mutable.Map[PartitionDirectory, CatalogTablePartition] =
    mutable.Map()

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    def isNonEmptyFile(f: FileStatus): Boolean = {
      isDataPath(f.getPath) && f.getLen > 0
    }
    val selectedPartitions =
      if (partitionSpec().partitionColumns.isEmpty) {
        HiveConnectorUtils.createPartitionDirectory(
          InternalRow.empty,
          allFiles().filter(isNonEmptyFile)) :: Nil
      } else {
        if (recursiveFileLookup) {
          throw new IllegalArgumentException(
            "Datasource with partition do not allow recursive file loading.")
        }
        prunePartitions(partitionFilters, partitionSpec()).map {
          case partPath @ PartitionPath(values, path) =>
            val files: Seq[FileStatus] = leafDirToChildrenFiles.get(path) match {
              case Some(existingDir) =>
                // Directory has children files in it, return them
                existingDir.filter(f => matchPathPattern(f) && isNonEmptyFile(f))

              case None =>
                // Directory does not exist, or has no children files
                Nil
            }
            val partDir = HiveConnectorUtils.createPartitionDirectory(values, files)
            // Update Partition Directory -> binding Hive part map
            updatePartDirHivePartitionMapping(partDir, partPath)

            partDir
        }
      }
    logTrace("Selected files after partition pruning:\n\t" + selectedPartitions.mkString("\n\t"))
    selectedPartitions
  }

  private def prunePartitions(
      predicates: Seq[Expression],
      partitionSpec: PartitionSpec): Seq[PartitionPath] = {
    val PartitionSpec(partitionColumns, partitions) = partitionSpec
    val partitionColumnNames = partitionColumns.map(_.name).toSet
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }

    if (partitionPruningPredicates.nonEmpty) {
      val predicate = partitionPruningPredicates.reduce(expressions.And)

      val boundPredicate = Predicate.createInterpreted(predicate.transform {
        case a: AttributeReference =>
          val index = partitionColumns.indexWhere(a.name == _.name)
          BoundReference(index, partitionColumns(index).dataType, nullable = true)
      })

      val selected = partitions.filter {
        case PartitionPath(values, _) => boundPredicate.eval(values)
      }
      logInfo {
        val total = partitions.length
        val selectedSize = selected.length
        val percentPruned = (1 - selectedSize.toDouble / total.toDouble) * 100
        s"Selected $selectedSize partitions out of $total, " +
          s"pruned ${if (total == 0) "0" else s"$percentPruned%"} partitions."
      }

      selected
    } else {
      partitions
    }
  }

  private def isDataPath(path: Path): Boolean = {
    val name = path.getName
    !((name.startsWith("_") && !name.contains("=")) || name.startsWith("."))
  }

  def partDirToBindHivePartMap(): Map[PartitionDirectory, CatalogTablePartition] = {
    partDirToBindHivePart.toMap
  }

  def updatePartDirHivePartitionMapping(
      partDir: PartitionDirectory,
      partPath: PartitionPath): Unit = {
    if (partPathToBindHivePart.contains(partPath)) {
      partDirToBindHivePart += (partDir -> partPathToBindHivePart(partPath))
    }
  }
}

case class BindPartition(catalogTablePartition: CatalogTablePartition)

object HiveTableCatalogFileIndex {
  implicit class CatalogHelper(plugin: CatalogPlugin) {
    def asHiveCatalog: HiveTableCatalog = plugin match {
      case hiveTableCatalog: HiveTableCatalog =>
        hiveTableCatalog
      case _ =>
        throw KyuubiHiveConnectorException(
          s"Cannot use catalog ${plugin.name}: not a HiveTableCatalog")
    }
  }
}
