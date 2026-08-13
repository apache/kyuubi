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

import java.lang.{Boolean => JBoolean, Long => JLong}
import java.net.URI

import scala.util.Try

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.ql.plan.{FileSinkDesc, TableDesc}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStatistics, CatalogStorageFormat, CatalogTable, CatalogTablePartition, CatalogTableType}
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.connector.catalog.TableChange._
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.execution.datasources.{PartitionDirectory, PartitionedFile}
import org.apache.spark.sql.hive.execution.HiveFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}

import org.apache.kyuubi.util.reflect.{DynClasses, DynConstructors, DynMethods}
import org.apache.kyuubi.util.reflect.ReflectUtils.invokeAs

object HiveConnectorUtils extends Logging {

  def getHiveFileFormat(fileSinkConf: FileSinkDesc): HiveFileFormat =
    Try { // SPARK-43186: 3.5.0
      DynConstructors.builder()
        .impl(classOf[HiveFileFormat], classOf[FileSinkDesc])
        .build[HiveFileFormat]()
        .newInstance(fileSinkConf)
    }.recover { case _: Exception =>
      val shimFileSinkDescClz = DynClasses.builder()
        .impl("org.apache.spark.sql.hive.HiveShim$ShimFileSinkDesc")
        .build()
      val shimFileSinkDesc = DynConstructors.builder()
        .impl(
          "org.apache.spark.sql.hive.HiveShim$ShimFileSinkDesc",
          classOf[String],
          classOf[TableDesc],
          classOf[Boolean])
        .build[AnyRef]()
        .newInstance(
          fileSinkConf.getDirName.toString,
          fileSinkConf.getTableInfo,
          fileSinkConf.getCompressed.asInstanceOf[JBoolean])
      DynConstructors.builder()
        .impl(classOf[HiveFileFormat], shimFileSinkDescClz)
        .build[HiveFileFormat]()
        .newInstance(shimFileSinkDesc)
    }.get

  // `serdeName` widened the case-class `apply` from 6 to 7 args. `DynMethods.invoke`
  // truncates trailing args to the matched arity, so the trailing `serdeName` is
  // silently dropped on the 6-arg impl.
  private lazy val storageFormatApply: DynMethods.StaticMethod =
    DynMethods.builder("apply")
      .impl( // SPARK-55645 (4.2.0): 7-arg apply with serdeName
        classOf[CatalogStorageFormat],
        classOf[Option[URI]],
        classOf[Option[String]],
        classOf[Option[String]],
        classOf[Option[String]],
        classOf[Boolean],
        classOf[Map[String, String]],
        classOf[Option[String]])
      .impl( // Spark < 4.2.0: 6-arg apply without serdeName
        classOf[CatalogStorageFormat],
        classOf[Option[URI]],
        classOf[Option[String]],
        classOf[Option[String]],
        classOf[Option[String]],
        classOf[Boolean],
        classOf[Map[String, String]])
      .build()
      .asStatic()

  // SPARK-55645 (4.2.0): serdeName getter
  private def storageFormatSerdeName(base: CatalogStorageFormat): Option[String] =
    Try {
      DynMethods.builder("serdeName")
        .impl(classOf[CatalogStorageFormat])
        .build()
        .invoke[Option[String]](base)
    }.getOrElse(None)

  def newStorageFormat(
      locationUri: Option[URI] = None,
      inputFormat: Option[String] = None,
      outputFormat: Option[String] = None,
      serde: Option[String] = None,
      compressed: Boolean = false,
      properties: Map[String, String] = Map.empty): CatalogStorageFormat =
    storageFormatApply.invoke[CatalogStorageFormat](
      locationUri,
      inputFormat,
      outputFormat,
      serde,
      compressed.asInstanceOf[JBoolean],
      properties,
      None
    ) // serdeName defaults to None; ignored on Spark < 4.2

  def copyStorageFormat(
      base: CatalogStorageFormat)(
      locationUri: Option[URI] = base.locationUri,
      inputFormat: Option[String] = base.inputFormat,
      outputFormat: Option[String] = base.outputFormat,
      serde: Option[String] = base.serde,
      compressed: Boolean = base.compressed,
      properties: Map[String, String] = base.properties): CatalogStorageFormat =
    storageFormatApply.invoke[CatalogStorageFormat](
      locationUri,
      inputFormat,
      outputFormat,
      serde,
      compressed.asInstanceOf[JBoolean],
      properties,
      storageFormatSerdeName(base))

  // `collation` was inserted in the middle of `CatalogTable`'s parameter list, so
  // `DynMethods`' trailing-arg truncation cannot be relied on. Resolve the `apply`
  // arity and argument list from the presence of the two getters.

  // SPARK-50675 (4.0.0): collation getter presence
  private lazy val catalogTableHasCollation: Boolean =
    Try(DynMethods.builder("collation").impl(classOf[CatalogTable]).build()).isSuccess

  // SPARK-52729 (4.2.0): multipartIdentifier getter presence
  private lazy val catalogTableHasMultipartIdentifier: Boolean =
    Try(DynMethods.builder("multipartIdentifier").impl(classOf[CatalogTable]).build()).isSuccess

  // SPARK-50675 (4.0.0): collation getter
  private def catalogTableCollation(base: CatalogTable): Option[String] =
    Try {
      DynMethods.builder("collation")
        .impl(classOf[CatalogTable])
        .build()
        .invoke[Option[String]](base)
    }.getOrElse(None)

  // SPARK-52729 (4.2.0): multipartIdentifier getter
  private def catalogTableMultipartIdentifier(base: CatalogTable): Option[Seq[String]] =
    Try {
      DynMethods.builder("multipartIdentifier")
        .impl(classOf[CatalogTable])
        .build()
        .invoke[Option[Seq[String]]](base)
    }.getOrElse(None)

  private lazy val catalogTableApply: DynMethods.StaticMethod = {
    val baseParams: Seq[Class[_]] = Seq[Class[_]](
      classOf[TableIdentifier],
      classOf[CatalogTableType],
      classOf[CatalogStorageFormat],
      classOf[StructType],
      classOf[Option[String]],
      classOf[Seq[String]],
      classOf[Option[BucketSpec]],
      classOf[String],
      classOf[Long],
      classOf[Long],
      classOf[String],
      classOf[Map[String, String]],
      classOf[Option[CatalogStatistics]],
      classOf[Option[String]],
      classOf[Option[String]])
    // SPARK-50675 (4.0.0): collation inserted between comment and unsupportedFeatures
    val collationParam: Seq[Class[_]] =
      if (catalogTableHasCollation) Seq[Class[_]](classOf[Option[String]]) else Nil
    val trailingParams: Seq[Class[_]] = Seq[Class[_]](
      classOf[Seq[String]],
      classOf[Boolean],
      classOf[Boolean],
      classOf[Map[String, String]],
      classOf[Option[String]])
    // SPARK-52729 (4.2.0): multipartIdentifier appended after viewOriginalText
    val multipartParam: Seq[Class[_]] =
      if (catalogTableHasMultipartIdentifier) Seq[Class[_]](classOf[Option[Seq[String]]]) else Nil
    DynMethods.builder("apply")
      .impl(
        classOf[CatalogTable],
        (baseParams ++ collationParam ++ trailingParams ++ multipartParam): _*)
      .build()
      .asStatic()
  }

  // scalastyle:off parameter.number
  def copyCatalogTable(
      base: CatalogTable)(
      identifier: TableIdentifier = base.identifier,
      tableType: CatalogTableType = base.tableType,
      storage: CatalogStorageFormat = base.storage,
      schema: StructType = base.schema,
      provider: Option[String] = base.provider,
      partitionColumnNames: Seq[String] = base.partitionColumnNames,
      bucketSpec: Option[BucketSpec] = base.bucketSpec,
      owner: String = base.owner,
      createTime: Long = base.createTime,
      lastAccessTime: Long = base.lastAccessTime,
      createVersion: String = base.createVersion,
      properties: Map[String, String] = base.properties,
      stats: Option[CatalogStatistics] = base.stats,
      viewText: Option[String] = base.viewText,
      comment: Option[String] = base.comment,
      unsupportedFeatures: Seq[String] = base.unsupportedFeatures,
      tracksPartitionsInCatalog: Boolean = base.tracksPartitionsInCatalog,
      schemaPreservesCase: Boolean = base.schemaPreservesCase,
      ignoredProperties: Map[String, String] = base.ignoredProperties,
      viewOriginalText: Option[String] = base.viewOriginalText): CatalogTable = {
    val baseArgs: Seq[AnyRef] = Seq[AnyRef](
      identifier,
      tableType,
      storage,
      schema,
      provider,
      partitionColumnNames,
      bucketSpec,
      owner,
      createTime.asInstanceOf[JLong],
      lastAccessTime.asInstanceOf[JLong],
      createVersion,
      properties,
      stats,
      viewText,
      comment)
    val collationArg: Seq[AnyRef] =
      if (catalogTableHasCollation) Seq[AnyRef](catalogTableCollation(base)) else Nil
    val trailingArgs: Seq[AnyRef] = Seq[AnyRef](
      unsupportedFeatures,
      tracksPartitionsInCatalog.asInstanceOf[JBoolean],
      schemaPreservesCase.asInstanceOf[JBoolean],
      ignoredProperties,
      viewOriginalText)
    val multipartArg: Seq[AnyRef] =
      if (catalogTableHasMultipartIdentifier) Seq[AnyRef](catalogTableMultipartIdentifier(base))
      else Nil
    catalogTableApply.invoke[CatalogTable](
      (baseArgs ++ collationArg ++ trailingArgs ++ multipartArg): _*)
  }
  // scalastyle:on parameter.number

  def partitionedFilePath(file: PartitionedFile): String =
    Try { // SPARK-41970: 3.4.0
      invokeAs[String](file, "urlEncodedPath")
    }.recover { case _: Exception =>
      invokeAs[String](file, "filePath")
    }.get

  def splitFiles(
      sparkSession: SparkSession,
      file: AnyRef,
      filePath: Path,
      isSplitable: JBoolean,
      maxSplitBytes: JLong,
      partitionValues: InternalRow): Seq[PartitionedFile] =
    Try { // SPARK-42821, SPARK-51185: Spark 4.0
      val fileStatusWithMetadataClz = DynClasses.builder()
        .impl("org.apache.spark.sql.execution.datasources.FileStatusWithMetadata")
        .buildChecked()
      DynMethods
        .builder("splitFiles")
        .impl(
          "org.apache.spark.sql.execution.PartitionedFileUtil",
          fileStatusWithMetadataClz,
          classOf[Path],
          classOf[Boolean],
          classOf[Long],
          classOf[InternalRow])
        .buildChecked()
        .invokeChecked[Seq[PartitionedFile]](
          null,
          file,
          filePath,
          isSplitable,
          maxSplitBytes,
          partitionValues)
    }.recover { case _: Exception => // SPARK-42821: 4.0.0-preview2
      val fileStatusWithMetadataClz = DynClasses.builder()
        .impl("org.apache.spark.sql.execution.datasources.FileStatusWithMetadata")
        .buildChecked()
      DynMethods
        .builder("splitFiles")
        .impl(
          "org.apache.spark.sql.execution.PartitionedFileUtil",
          fileStatusWithMetadataClz,
          classOf[Boolean],
          classOf[Long],
          classOf[InternalRow])
        .buildChecked()
        .invokeChecked[Seq[PartitionedFile]](
          null,
          file,
          isSplitable,
          maxSplitBytes,
          partitionValues)
    }.recover { case _: Exception => // SPARK-51185: Spark 3.5.7
      val fileStatusWithMetadataClz = DynClasses.builder()
        .impl("org.apache.spark.sql.execution.datasources.FileStatusWithMetadata")
        .buildChecked()
      DynMethods
        .builder("splitFiles")
        .impl(
          "org.apache.spark.sql.execution.PartitionedFileUtil",
          classOf[SparkSession],
          fileStatusWithMetadataClz,
          classOf[Path],
          classOf[Boolean],
          classOf[Long],
          classOf[InternalRow])
        .buildChecked()
        .invokeChecked[Seq[PartitionedFile]](
          null,
          sparkSession,
          file,
          filePath,
          isSplitable,
          maxSplitBytes,
          partitionValues)
    }.recover { case _: Exception => // SPARK-43039: 3.5.0
      val fileStatusWithMetadataClz = DynClasses.builder()
        .impl("org.apache.spark.sql.execution.datasources.FileStatusWithMetadata")
        .buildChecked()
      DynMethods
        .builder("splitFiles")
        .impl(
          "org.apache.spark.sql.execution.PartitionedFileUtil",
          classOf[SparkSession],
          fileStatusWithMetadataClz,
          classOf[Boolean],
          classOf[Long],
          classOf[InternalRow])
        .buildChecked()
        .invokeChecked[Seq[PartitionedFile]](
          null,
          sparkSession,
          file,
          isSplitable,
          maxSplitBytes,
          partitionValues)
    }.recover { case _: Exception =>
      DynMethods
        .builder("splitFiles")
        .impl(
          "org.apache.spark.sql.execution.PartitionedFileUtil",
          classOf[SparkSession],
          classOf[FileStatus],
          classOf[Path],
          classOf[Boolean],
          classOf[Long],
          classOf[InternalRow])
        .buildChecked()
        .invokeChecked[Seq[PartitionedFile]](
          null,
          sparkSession,
          file,
          filePath,
          isSplitable,
          maxSplitBytes,
          partitionValues)
    }.get

  def createPartitionDirectory(values: InternalRow, files: Seq[FileStatus]): PartitionDirectory =
    Try { // SPARK-43039: 3.5.0
      new DynMethods.Builder("apply")
        .impl(classOf[PartitionDirectory], classOf[InternalRow], classOf[Array[FileStatus]])
        .buildChecked()
        .asStatic()
        .invoke[PartitionDirectory](values, files.toArray)
    }.recover { case _: Exception =>
      new DynMethods.Builder("apply")
        .impl(classOf[PartitionDirectory], classOf[InternalRow], classOf[Seq[FileStatus]])
        .buildChecked()
        .asStatic()
        .invoke[PartitionDirectory](values, files)
    }.get

  def getPartitionFilePath(file: AnyRef): Path =
    Try { // SPARK-43039: 3.5.0
      new DynMethods.Builder("getPath")
        .impl("org.apache.spark.sql.execution.datasources.FileStatusWithMetadata")
        .build()
        .invoke[Path](file)
    }.recover { case _: Exception =>
      file.asInstanceOf[FileStatus].getPath
    }.get

  private def calculateMultipleLocationSizes(
      sparkSession: SparkSession,
      tid: TableIdentifier,
      paths: Seq[Option[URI]]): Seq[Long] = {

    val sparkSessionClz = DynClasses.builder()
      .impl("org.apache.spark.sql.classic.SparkSession") // SPARK-49700 (4.0.0)
      .impl("org.apache.spark.sql.SparkSession")
      .build()

    val calculateMultipleLocationSizesMethod =
      DynMethods.builder("calculateMultipleLocationSizes")
        .impl(
          CommandUtils.getClass,
          sparkSessionClz,
          classOf[TableIdentifier],
          classOf[Seq[Option[URI]]])
        .buildChecked(CommandUtils)

    calculateMultipleLocationSizesMethod
      .invokeChecked[Seq[Long]](sparkSession, tid, paths)
  }

  def calculateTotalSize(
      spark: SparkSession,
      catalogTable: CatalogTable,
      hiveTableCatalog: HiveTableCatalog): (BigInt, Seq[CatalogTablePartition]) = {
    val sessionState = spark.sessionState
    val startTime = System.nanoTime()
    val (totalSize, newPartitions) = if (catalogTable.partitionColumnNames.isEmpty) {
      val tableSize = CommandUtils.calculateSingleLocationSize(
        sessionState,
        catalogTable.identifier,
        catalogTable.storage.locationUri)
      (tableSize, Seq())
    } else {
      // Calculate table size as a sum of the visible partitions. See SPARK-21079
      val partitions = hiveTableCatalog.listPartitions(catalogTable.identifier)
      logInfo(s"Starting to calculate sizes for ${partitions.length} partitions.")
      val paths = partitions.map(_.storage.locationUri)
      val sizes = calculateMultipleLocationSizes(spark, catalogTable.identifier, paths)
      val newPartitions = partitions.zipWithIndex.flatMap { case (p, idx) =>
        val newStats = CommandUtils.compareAndGetNewStats(p.stats, sizes(idx), None)
        newStats.map(_ => p.copy(stats = newStats))
      }
      (sizes.sum, newPartitions)
    }
    logInfo(s"It took ${(System.nanoTime() - startTime) / (1000 * 1000)} ms to calculate" +
      s" the total size for table ${catalogTable.identifier}.")
    (totalSize, newPartitions)
  }

  def applySchemaChanges(schema: StructType, changes: Seq[TableChange]): StructType = {
    changes.foldLeft(schema) { (schema, change) =>
      change match {
        case add: AddColumn =>
          add.fieldNames match {
            case Array(name) =>
              val field = StructField(name, add.dataType, nullable = add.isNullable)
              val newField = Option(add.comment).map(field.withComment).getOrElse(field)
              addField(schema, newField, add.position())

            case names =>
              replace(
                schema,
                names.init,
                parent =>
                  parent.dataType match {
                    case parentType: StructType =>
                      val field = StructField(names.last, add.dataType, nullable = add.isNullable)
                      val newField = Option(add.comment).map(field.withComment).getOrElse(field)
                      Some(parent.copy(dataType = addField(parentType, newField, add.position())))

                    case _ =>
                      throw new IllegalArgumentException(s"Not a struct: ${names.init.last}")
                  })
          }

        case rename: RenameColumn =>
          replace(
            schema,
            rename.fieldNames,
            field =>
              Some(StructField(rename.newName, field.dataType, field.nullable, field.metadata)))

        case update: UpdateColumnType =>
          replace(
            schema,
            update.fieldNames,
            field => Some(field.copy(dataType = update.newDataType)))

        case update: UpdateColumnNullability =>
          replace(
            schema,
            update.fieldNames,
            field => Some(field.copy(nullable = update.nullable)))

        case update: UpdateColumnComment =>
          replace(
            schema,
            update.fieldNames,
            field => Some(field.withComment(update.newComment)))

        case update: UpdateColumnPosition =>
          def updateFieldPos(struct: StructType, name: String): StructType = {
            val oldField = struct.fields.find(_.name == name).getOrElse {
              throw new IllegalArgumentException("Field not found: " + name)
            }
            val withFieldRemoved = StructType(struct.fields.filter(_ != oldField))
            addField(withFieldRemoved, oldField, update.position())
          }

          update.fieldNames() match {
            case Array(name) =>
              updateFieldPos(schema, name)
            case names =>
              replace(
                schema,
                names.init,
                parent =>
                  parent.dataType match {
                    case parentType: StructType =>
                      Some(parent.copy(dataType = updateFieldPos(parentType, names.last)))
                    case _ =>
                      throw new IllegalArgumentException(s"Not a struct: ${names.init.last}")
                  })
          }

        case delete: DeleteColumn =>
          replace(schema, delete.fieldNames, _ => None, delete.ifExists)

        case _ =>
          // ignore non-schema changes
          schema
      }
    }
  }

  private def addField(
      schema: StructType,
      field: StructField,
      position: ColumnPosition): StructType = {
    if (position == null) {
      schema.add(field)
    } else if (position.isInstanceOf[First]) {
      StructType(field +: schema.fields)
    } else {
      val afterCol = position.asInstanceOf[After].column()
      val fieldIndex = schema.fields.indexWhere(_.name == afterCol)
      if (fieldIndex == -1) {
        throw new IllegalArgumentException("AFTER column not found: " + afterCol)
      }
      val (before, after) = schema.fields.splitAt(fieldIndex + 1)
      StructType(before ++ (field +: after))
    }
  }

  private def replace(
      struct: StructType,
      fieldNames: Seq[String],
      update: StructField => Option[StructField],
      ifExists: Boolean = false): StructType = {

    val posOpt = fieldNames.zipWithIndex.toMap.get(fieldNames.head)
    if (posOpt.isEmpty) {
      if (ifExists) {
        // We couldn't find the column to replace, but with IF EXISTS, we will silence the error
        // Currently only DROP COLUMN may pass down the IF EXISTS parameter
        return struct
      } else {
        throw new IllegalArgumentException(s"Cannot find field: ${fieldNames.head}")
      }
    }

    val pos = posOpt.get
    val field = struct.fields(pos)
    val replacement: Option[StructField] = (fieldNames.tail, field.dataType) match {
      case (Seq(), _) =>
        update(field)

      case (names, struct: StructType) =>
        val updatedType: StructType = replace(struct, names, update, ifExists)
        Some(StructField(field.name, updatedType, field.nullable, field.metadata))

      case (Seq("key"), map @ MapType(keyType, _, _)) =>
        val updated = update(StructField("key", keyType, nullable = false))
          .getOrElse(throw new IllegalArgumentException(s"Cannot delete map key"))
        Some(field.copy(dataType = map.copy(keyType = updated.dataType)))

      case (Seq("key", names @ _*), map @ MapType(keyStruct: StructType, _, _)) =>
        Some(field.copy(dataType = map.copy(keyType = replace(keyStruct, names, update, ifExists))))

      case (Seq("value"), map @ MapType(_, mapValueType, isNullable)) =>
        val updated = update(StructField("value", mapValueType, nullable = isNullable))
          .getOrElse(throw new IllegalArgumentException(s"Cannot delete map value"))
        Some(field.copy(dataType = map.copy(
          valueType = updated.dataType,
          valueContainsNull = updated.nullable)))

      case (Seq("value", names @ _*), map @ MapType(_, valueStruct: StructType, _)) =>
        Some(field.copy(dataType = map.copy(valueType =
          replace(valueStruct, names, update, ifExists))))

      case (Seq("element"), array @ ArrayType(elementType, isNullable)) =>
        val updated = update(StructField("element", elementType, nullable = isNullable))
          .getOrElse(throw new IllegalArgumentException(s"Cannot delete array element"))
        Some(field.copy(dataType = array.copy(
          elementType = updated.dataType,
          containsNull = updated.nullable)))

      case (Seq("element", names @ _*), array @ ArrayType(elementStruct: StructType, _)) =>
        Some(field.copy(dataType = array.copy(elementType =
          replace(elementStruct, names, update, ifExists))))

      case (names, dataType) =>
        if (!ifExists) {
          throw new IllegalArgumentException(
            s"Cannot find field: ${names.head} in ${dataType.simpleString}")
        }
        None
    }

    val newFields = struct.fields.zipWithIndex.flatMap {
      case (_, index) if pos == index =>
        replacement
      case (other, _) =>
        Some(other)
    }

    new StructType(newFields)
  }

  // This is a fork of Spark's withSQLConf, and we use a different name to avoid linkage
  // issue on cross-version cases.
  // For example, SPARK-46227(4.0.0) moves `withSQLConf` from SQLHelper to SQLConfHelper,
  // classes that extend SQLConfHelper will prefer to linkage super class's method when
  // compiling with Spark 4.0, then linkage error will happen when run the jar with lower
  // Spark versions.
  def withSparkSQLConf[T](pairs: (String, String)*)(f: => T): T = {
    val conf = SQLConf.get
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (conf.contains(key)) {
        Some(conf.getConfString(key))
      } else {
        None
      }
    }
    (keys, values).zipped.foreach { (k, v) =>
      if (SQLConf.isStaticConfigKey(k)) {
        throw KyuubiHiveConnectorException(s"Cannot modify the value of a static config: $k")
      }
      conf.setConfString(k, v)
    }
    try f
    finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }
}
