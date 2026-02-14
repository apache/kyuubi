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

package org.apache.kyuubi.spark.connector.hive

import java.net.URI
import java.util
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal}
import org.apache.spark.sql.connector.catalog.{SupportsPartitionManagement, SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, OVERWRITE_BY_FILTER, OVERWRITE_DYNAMIC}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScanBuilder
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScanBuilder
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.{BucketSpecHelper, LogicalExpressions}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.spark.connector.hive.KyuubiHiveConnectorConf.{READ_CONVERT_METASTORE_ORC, READ_CONVERT_METASTORE_PARQUET}
import org.apache.kyuubi.spark.connector.hive.read.{HiveCatalogFileIndex, HiveScanBuilder}
import org.apache.kyuubi.spark.connector.hive.write.HiveWriteBuilder

case class HiveTable(
    sparkSession: SparkSession,
    catalogTable: CatalogTable,
    hiveTableCatalog: HiveTableCatalog)
  extends Table with SupportsRead with SupportsWrite with SupportsPartitionManagement with Logging {

  lazy val dataSchema: StructType = catalogTable.dataSchema

  lazy val partitionSchema: StructType = catalogTable.partitionSchema

  def rootPaths: Seq[Path] = catalogTable.storage.locationUri.map(new Path(_)).toSeq

  lazy val fileIndex: HiveCatalogFileIndex = {
    val defaultTableSize = sparkSession.sessionState.conf.defaultSizeInBytes
    new HiveCatalogFileIndex(
      sparkSession,
      catalogTable,
      hiveTableCatalog,
      catalogTable.stats.map(_.sizeInBytes.toLong).getOrElse(defaultTableSize))
  }

  lazy val convertedProvider: Option[String] = {
    val serde = catalogTable.storage.serde.getOrElse("").toUpperCase(Locale.ROOT)
    val parquet = serde.contains("PARQUET")
    val orc = serde.contains("ORC")
    val provider = catalogTable.provider.map(_.toUpperCase(Locale.ROOT))
    if (orc || provider.contains("ORC")) {
      Some("ORC")
    } else if (parquet || provider.contains("PARQUET")) {
      Some("PARQUET")
    } else {
      None
    }
  }

  override def name(): String = catalogTable.identifier.unquotedString

  override def schema(): StructType = catalogTable.schema

  override def properties(): util.Map[String, String] = catalogTable.properties.asJava

  override def partitioning: Array[Transform] = {
    val partitions = new mutable.ArrayBuffer[Transform]()
    catalogTable.partitionColumnNames.foreach { col =>
      partitions += LogicalExpressions.identity(LogicalExpressions.reference(Seq(col)))
    }
    catalogTable.bucketSpec.foreach { spec =>
      partitions += spec.asTransform
    }
    partitions.toArray
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    convertedProvider match {
      case Some("ORC") if sparkSession.sessionState.conf.getConf(READ_CONVERT_METASTORE_ORC) =>
        OrcScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
      case Some("PARQUET")
          if sparkSession.sessionState.conf.getConf(READ_CONVERT_METASTORE_PARQUET) =>
        ParquetScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)
      case _ => HiveScanBuilder(sparkSession, fileIndex, dataSchema, catalogTable)
    }
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    HiveWriteBuilder(sparkSession, catalogTable, info, hiveTableCatalog)
  }

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(BATCH_READ, BATCH_WRITE, OVERWRITE_BY_FILTER, OVERWRITE_DYNAMIC)
  }

  override def createPartition(ident: InternalRow, properties: util.Map[String, String]): Unit = {
    val spec = toPartitionSpec(ident)
    val location = Option(properties.get(HiveTableProperties.LOCATION)).map(new URI(_))
    val newPart = CatalogTablePartition(
      spec,
      catalogTable.storage.copy(locationUri = location),
      properties.asScala.toMap)
    hiveTableCatalog.externalCatalog.createPartitions(
      catalogTable.database,
      catalogTable.identifier.table,
      Seq(newPart),
      ignoreIfExists = false)
  }

  override def dropPartition(ident: InternalRow): Boolean = {
    try {
      hiveTableCatalog.externalCatalog.dropPartitions(
        catalogTable.database,
        catalogTable.identifier.table,
        Seq(toPartitionSpec(ident)),
        ignoreIfNotExists = false,
        purge = false,
        retainData = false)
      true
    } catch {
      case _: NoSuchPartitionException => false
    }
  }

  override def replacePartitionMetadata(
      ident: InternalRow,
      properties: util.Map[String, String]): Unit = {
    throw new UnsupportedOperationException("Replace partition is not supported")
  }

  override def loadPartitionMetadata(ident: InternalRow): util.Map[String, String] = {
    val spec = toPartitionSpec(ident)
    val partition = hiveTableCatalog.externalCatalog.getPartition(
      catalogTable.database,
      catalogTable.identifier.table,
      spec)
    val metadata = new util.HashMap[String, String](partition.parameters.asJava)
    partition.storage.locationUri.foreach { uri =>
      metadata.put(HiveTableProperties.LOCATION, uri.toString)
    }
    metadata
  }

  override def listPartitionIdentifiers(
      names: Array[String],
      ident: InternalRow): Array[InternalRow] = {
    val partialSpec = if (names.isEmpty) {
      None
    } else {
      val fields = names.map(partitionSchema(_))
      val schema = StructType(fields)
      Some(toPartitionSpec(ident, schema))
    }
    hiveTableCatalog.externalCatalog.listPartitions(
      catalogTable.database,
      catalogTable.identifier.table,
      partialSpec).map { part =>
      val values = partitionSchema.map { field =>
        val strValue = part.spec(field.name)
        HiveConnectorUtils.castExpression(Literal(strValue), field.dataType).eval()
      }
      new GenericInternalRow(values.toArray)
    }.toArray
  }

  private def toPartitionSpec(ident: InternalRow, schema: StructType): Map[String, String] = {
    require(
      schema.size == ident.numFields,
      s"Schema size (${schema.size}) does not match numFields (${ident.numFields})")
    schema.zipWithIndex.map { case (field, index) =>
      val value = ident.get(index, field.dataType)
      val filedValue = HiveConnectorUtils.castExpression(
        Literal(value, field.dataType),
        StringType,
        Some(sparkSession.sessionState.conf.sessionLocalTimeZone)).eval().toString
      field.name -> filedValue
    }.toMap
  }

  private def toPartitionSpec(ident: InternalRow): Map[String, String] = {
    toPartitionSpec(ident, partitionSchema)
  }
}
