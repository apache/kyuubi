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

package org.apache.kyuubi.plugin.spark.authz.serde

import java.util.{LinkedHashMap, Map => JMap}

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String

import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.plugin.spark.authz.util.PathIdentifier._
import org.apache.kyuubi.util.reflect.ReflectUtils._

/**
 * A trait for extracting database and table as string tuple
 * from the give object whose class type is define by `key`.
 */
trait TableExtractor extends ((SparkSession, AnyRef) => Option[Table]) with Extractor

object TableExtractor {
  val tableExtractors: Map[String, TableExtractor] = {
    loadExtractorsToMap[TableExtractor]
  }

  /**
   * Get table owner from table properties
   * @param v a object contains a org.apache.spark.sql.connector.catalog.Table
   * @return owner
   */
  def getOwner(v: AnyRef): Option[String] = {
    // org.apache.spark.sql.connector.catalog.Table
    val table = invokeAs[AnyRef](v, "table")
    val properties = invokeAs[JMap[String, String]](table, "properties").asScala
    properties.get("owner")
  }

  def getOwner(spark: SparkSession, catalogName: String, tableIdent: AnyRef): Option[String] = {
    try {
      val catalogManager = invokeAs[AnyRef](spark.sessionState, "catalogManager")
      val catalog = invokeAs[AnyRef](catalogManager, "catalog", (classOf[String], catalogName))
      val table = invokeAs[AnyRef](
        catalog,
        "loadTable",
        (Class.forName("org.apache.spark.sql.connector.catalog.Identifier"), tableIdent))
      getOwner(table)
    } catch {
      // Exception may occur due to invalid reflection or table not found
      case _: Exception => None
    }
  }
}

/**
 * org.apache.spark.sql.catalyst.TableIdentifier
 */
class TableIdentifierTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val identifier = v1.asInstanceOf[TableIdentifier]
    if (isPathIdentifier(identifier.table, spark)) {
      None
    } else {
      val owner =
        try {
          val catalogTable = spark.sessionState.catalog.getTableMetadata(identifier)
          Option(catalogTable.owner).filter(_.nonEmpty)
        } catch {
          case _: Exception => None
        }
      Some(Table(None, identifier.database, identifier.table, owner))
    }
  }
}

/**
 * org.apache.spark.sql.catalyst.TableIdentifier Option
 */
class TableIdentifierOptionTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val tableIdentifier = v1.asInstanceOf[Option[TableIdentifier]]
    tableIdentifier.flatMap(lookupExtractor[TableIdentifierTableExtractor].apply(spark, _))
  }
}

/**
 * org.apache.spark.sql.catalyst.catalog.CatalogTable
 */
class CatalogTableTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    if (null == v1) {
      None
    } else {
      val catalogTable = v1.asInstanceOf[CatalogTable]
      val identifier = catalogTable.identifier
      val owner = Option(catalogTable.owner).filter(_.nonEmpty)
      Some(Table(None, identifier.database, identifier.table, owner))
    }
  }
}

/**
 * org.apache.spark.sql.catalyst.catalog.CatalogTable Option
 */
class CatalogTableOptionTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val catalogTable = v1.asInstanceOf[Option[CatalogTable]]
    catalogTable.flatMap(lookupExtractor[CatalogTableTableExtractor].apply(spark, _))
  }
}

/**
 * org.apache.spark.sql.catalyst.analysis.ResolvedTable
 */
class ResolvedTableTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val catalogVal = invokeAs[AnyRef](v1, "catalog")
    val catalog = lookupExtractor[CatalogPluginCatalogExtractor].apply(catalogVal)
    val identifier = invokeAs[AnyRef](v1, "identifier")
    val maybeTable = lookupExtractor[IdentifierTableExtractor].apply(spark, identifier)
    // Iceberg show create table $viewName use ResolvedV2View
    if (!v1.getClass.getName
        .equals("org.apache.spark.sql.catalyst.plans.logical.views.ResolvedV2View")) {
      val maybeOwner = TableExtractor.getOwner(v1)
      maybeTable.map(_.copy(catalog = catalog, owner = maybeOwner))
    }
    maybeTable
  }
}

/**
 * org.apache.spark.sql.connector.catalog.Identifier
 */
class IdentifierTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = v1 match {
    case identifier: Identifier if !isPathIdentifier(identifier.name(), spark) =>
      Some(Table(None, Some(quote(identifier.namespace())), identifier.name(), None))
    case _ => None
  }
}

/**
 * java.lang.String
 * with concat parts by "."
 */
class StringTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val tableNameArr = v1.asInstanceOf[String].split("\\.")
    val maybeTable = tableNameArr.length match {
      case 1 => Table(None, None, tableNameArr(0), None)
      case 2 => Table(None, Some(tableNameArr(0)), tableNameArr(1), None)
      case 3 => Table(Some(tableNameArr(0)), Some(tableNameArr(1)), tableNameArr(2), None)
    }
    Option(maybeTable)
  }
}

/**
 * Seq[org.apache.spark.sql.catalyst.expressions.Expression]
 */
class ExpressionSeqTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val expressions = v1.asInstanceOf[Seq[Expression]]
    // Iceberg will rearrange the parameters according to the parameter order
    // defined in the procedure, where the table parameters are currently always the first.
    lookupExtractor[StringTableExtractor].apply(spark, expressions.head.toString())
  }
}

/**
 * org.apache.spark.sql.catalyst.plans.logical.AddPartitionField
 */
class ArrayBufferTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    // Iceberg will transform table to ArrayBuffer[String]
    val maybeTable = v1.asInstanceOf[Seq[String]] match {
      case Seq(tblName) => Table(None, None, tblName, None)
      case Seq(dbName, tblName) => Table(None, Some(dbName), tblName, None)
      case Seq(catalogName, dbName, tblName) =>
        Table(Some(catalogName), Some(dbName), tblName, None)
    }
    Option(maybeTable)
  }
}

/**
 * org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
 */
class DataSourceV2RelationTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val plan = v1.asInstanceOf[LogicalPlan]
    plan.find(_.getClass.getSimpleName == "DataSourceV2Relation").get match {
      case v2Relation: DataSourceV2Relation
          if v2Relation.identifier.isEmpty ||
            !isPathIdentifier(v2Relation.identifier.get.name(), spark) =>
        val maybeCatalog = v2Relation.catalog.flatMap(catalogPlugin =>
          lookupExtractor[CatalogPluginCatalogExtractor].apply(catalogPlugin))
        lookupExtractor[TableTableExtractor].apply(spark, v2Relation.table)
          .map { table =>
            val maybeOwner = TableExtractor.getOwner(v2Relation)
            val maybeDatabase: Option[String] = table.database match {
              case Some(x) => Some(x)
              case None =>
                val maybeIdentifier = invokeAs[Option[AnyRef]](v2Relation, "identifier")
                maybeIdentifier.flatMap { id =>
                  lookupExtractor[IdentifierTableExtractor].apply(spark, id)
                }.flatMap(table => table.database)
            }
            table.copy(catalog = maybeCatalog, database = maybeDatabase, owner = maybeOwner)
          }
      case _ => None
    }
  }
}

/**
 * org.apache.spark.sql.execution.datasources.LogicalRelation
 */
class LogicalRelationTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val maybeCatalogTable = invokeAs[Option[AnyRef]](v1, "catalogTable")
    maybeCatalogTable.flatMap { ct =>
      lookupExtractor[CatalogTableTableExtractor].apply(spark, ct)
    }
  }
}

/**
 * org.apache.spark.sql.catalyst.analysis.ResolvedDbObjectName
 */
class ResolvedDbObjectNameTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val nameParts = invokeAs[Seq[String]](v1, "nameParts")
    val table = nameParts.last
    if (isPathIdentifier(table, spark)) {
      None
    } else {
      val catalogVal = invokeAs[AnyRef](v1, "catalog")
      val catalog = lookupExtractor[CatalogPluginCatalogExtractor].apply(catalogVal)
      val namespace = nameParts.init.toArray
      Some(Table(catalog, Some(quote(namespace)), table, None))
    }
  }
}

/**
 * org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
 */
class ResolvedIdentifierTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    v1.getClass.getName match {
      case "org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier" =>
        val catalogVal = invokeAs[AnyRef](v1, "catalog")
        val catalog = lookupExtractor[CatalogPluginCatalogExtractor].apply(catalogVal)
        val identifier = invokeAs[AnyRef](v1, "identifier")
        val maybeTable = lookupExtractor[IdentifierTableExtractor].apply(spark, identifier)
        val owner = catalog.flatMap(name => TableExtractor.getOwner(spark, name, identifier))
        maybeTable.map(_.copy(catalog = catalog, owner = owner))
      case _ => None
    }
  }
}

/**
 * org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
 */
class SubqueryAliasTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    v1.asInstanceOf[SubqueryAlias] match {
      case SubqueryAlias(_, SubqueryAlias(identifier, _)) =>
        if (isPathIdentifier(identifier.name, spark)) {
          None
        } else {
          lookupExtractor[StringTableExtractor].apply(spark, identifier.toString())
        }
      case SubqueryAlias(identifier, _) if !isPathIdentifier(identifier.name, spark) =>
        lookupExtractor[StringTableExtractor].apply(spark, identifier.toString())
      case _ => None
    }
  }
}

/**
 * org.apache.spark.sql.connector.catalog.Table
 */
class TableTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val tableName = invokeAs[String](v1, "name")
    lookupExtractor[StringTableExtractor].apply(spark, tableName)
  }
}

class HudiDataSourceV2RelationTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    invokeAs[LogicalPlan](v1, "table") match {
      // Match multipartIdentifier with tableAlias
      case SubqueryAlias(_, SubqueryAlias(identifier, _)) =>
        lookupExtractor[StringTableExtractor].apply(spark, identifier.toString())
      // Match multipartIdentifier without tableAlias
      case SubqueryAlias(identifier, _) =>
        lookupExtractor[StringTableExtractor].apply(spark, identifier.toString())
      case _ => None
    }
  }
}

class HudiMergeIntoTargetTableExtractor extends TableExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    invokeAs[LogicalPlan](v1, "targetTable") match {
      // Match multipartIdentifier with tableAlias
      case SubqueryAlias(_, SubqueryAlias(identifier, relation)) =>
        lookupExtractor[StringTableExtractor].apply(spark, identifier.toString())
      // Match multipartIdentifier without tableAlias
      case SubqueryAlias(identifier, _) =>
        lookupExtractor[StringTableExtractor].apply(spark, identifier.toString())
      case _ => None
    }
  }
}

trait HudiCallProcedureExtractor {

  protected def extractTableIdentifier(
      procedure: AnyRef,
      args: AnyRef,
      tableParameterKey: String): Option[String] = {
    val tableIdentifierParameter =
      invokeAs[Array[AnyRef]](procedure, "parameters")
        .find(invokeAs[String](_, "name").equals(tableParameterKey))
        .getOrElse(throw new IllegalArgumentException(s"Could not find param $tableParameterKey"))
    val tableIdentifierParameterIndex = invokeAs[LinkedHashMap[String, Int]](args, "map")
      .getOrDefault(tableParameterKey, INVALID_INDEX)
    tableIdentifierParameterIndex match {
      case INVALID_INDEX =>
        None
      case argsIndex =>
        val dataType = invokeAs[DataType](tableIdentifierParameter, "dataType")
        val row = invokeAs[InternalRow](args, "internalRow")
        val tableName = InternalRow.getAccessor(dataType, true)(row, argsIndex)
        Option(tableName.asInstanceOf[UTF8String].toString)
    }
  }

  case class ProcedureArgsInputOutputTuple(
      inputTable: Option[String] = None,
      outputTable: Option[String] = None,
      inputUri: Option[String] = None,
      outputUri: Option[String] = None)

  protected val PROCEDURE_CLASS_PATH = "org.apache.spark.sql.hudi.command.procedures"

  protected val INVALID_INDEX = -1

  // These pairs are used to get the procedure input/output args which user passed in call command.
  protected val procedureArgsInputOutputPairs: Map[String, ProcedureArgsInputOutputTuple] = Map(
    (
      s"$PROCEDURE_CLASS_PATH.ArchiveCommitsProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.CommitsCompareProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.CopyToTableProcedure",
      ProcedureArgsInputOutputTuple(
        inputTable = Some("table"),
        outputTable = Some("new_table"))),
    (
      s"$PROCEDURE_CLASS_PATH.CopyToTempViewProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.CreateMetadataTableProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.CreateSavepointProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.DeleteMarkerProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.DeleteMetadataTableProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.DeleteSavepointProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ExportInstantsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.HdfsParquetImportProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.HelpProcedure",
      ProcedureArgsInputOutputTuple()),
    (
      s"$PROCEDURE_CLASS_PATH.HiveSyncProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.InitMetadataTableProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RepairAddpartitionmetaProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RepairCorruptedCleanFilesProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RepairDeduplicateProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RepairMigratePartitionMetaProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RepairOverwriteHoodiePropsProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RollbackToInstantTimeProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RollbackToSavepointProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RunBootstrapProcedure",
      ProcedureArgsInputOutputTuple(
        inputTable = Some("table"),
        outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RunCleanProcedure",
      ProcedureArgsInputOutputTuple(
        inputTable = Some("table"),
        outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.RunClusteringProcedure",
      ProcedureArgsInputOutputTuple(
        inputTable = Some("table"),
        outputTable = Some("table"),
        outputUri = Some("path"))),
    (
      s"$PROCEDURE_CLASS_PATH.RunCompactionProcedure",
      ProcedureArgsInputOutputTuple(
        inputTable = Some("table"),
        outputTable = Some("table"),
        outputUri = Some("path"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowArchivedCommitsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowBootstrapMappingProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowClusteringProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"), inputUri = Some("path"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowCommitsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowCommitExtraMetadataProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowCommitFilesProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowCommitPartitionsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowCommitWriteStatsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowCompactionProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"), inputUri = Some("path"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowFileSystemViewProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowFsPathDetailProcedure",
      ProcedureArgsInputOutputTuple()),
    (
      s"$PROCEDURE_CLASS_PATH.ShowHoodieLogFileMetadataProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowHoodieLogFileRecordsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowInvalidParquetProcedure",
      ProcedureArgsInputOutputTuple()),
    (
      s"$PROCEDURE_CLASS_PATH.ShowMetadataTableFilesProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowMetadataTablePartitionsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowMetadataTableStatsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowRollbacksProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowSavepointsProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ShowTablePropertiesProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.StatsFileSizeProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.StatsWriteAmplificationProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.UpgradeOrDowngradeProcedure",
      ProcedureArgsInputOutputTuple(outputTable = Some("table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ValidateHoodieSyncProcedure",
      ProcedureArgsInputOutputTuple(
        inputTable = Some("src_table"),
        outputTable = Some("dst_table"))),
    (
      s"$PROCEDURE_CLASS_PATH.ValidateMetadataTableFilesProcedure",
      ProcedureArgsInputOutputTuple(inputTable = Some("table"))))
}

class HudiCallProcedureOutputTableExtractor
  extends TableExtractor with HudiCallProcedureExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val procedure = invokeAs[AnyRef](v1, "procedure")
    val args = invokeAs[AnyRef](v1, "args")
    procedureArgsInputOutputPairs.get(procedure.getClass.getName)
      .filter(_.outputTable.isDefined)
      .map { argsPairs =>
        val tableIdentifier = extractTableIdentifier(procedure, args, argsPairs.outputTable.get)
        lookupExtractor[StringTableExtractor].apply(spark, tableIdentifier.get).orNull
      }
  }
}

class HudiCallProcedureInputTableExtractor
  extends TableExtractor with HudiCallProcedureExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Option[Table] = {
    val procedure = invokeAs[AnyRef](v1, "procedure")
    val args = invokeAs[AnyRef](v1, "args")
    procedureArgsInputOutputPairs.get(procedure.getClass.getName)
      .filter(_.inputTable.isDefined)
      .map { argsPairs =>
        val tableIdentifier = extractTableIdentifier(procedure, args, argsPairs.inputTable.get)
        lookupExtractor[StringTableExtractor].apply(spark, tableIdentifier.get).orNull
      }
  }
}

class HudiCallProcedureInputUriExtractor
  extends URIExtractor with HudiCallProcedureExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = {
    val procedure = invokeAs[AnyRef](v1, "procedure")
    val args = invokeAs[AnyRef](v1, "args")
    procedureArgsInputOutputPairs.get(procedure.getClass.getName)
      .filter(_.inputUri.isDefined)
      .map { argsPairs =>
        val tableIdentifier = extractTableIdentifier(procedure, args, argsPairs.inputUri.get)
        lookupExtractor[StringURIExtractor].apply(spark, tableIdentifier.get)
      }.getOrElse(Nil)
  }
}

class HudiCallProcedureOutputUriExtractor
  extends URIExtractor with HudiCallProcedureExtractor {
  override def apply(spark: SparkSession, v1: AnyRef): Seq[Uri] = {
    val procedure = invokeAs[AnyRef](v1, "procedure")
    val args = invokeAs[AnyRef](v1, "args")
    procedureArgsInputOutputPairs.get(procedure.getClass.getName)
      .filter(_.outputUri.isDefined)
      .map { argsPairs =>
        val tableIdentifier = extractTableIdentifier(procedure, args, argsPairs.outputUri.get)
        lookupExtractor[StringURIExtractor].apply(spark, tableIdentifier.get)
      }.getOrElse(Nil)
  }
}
