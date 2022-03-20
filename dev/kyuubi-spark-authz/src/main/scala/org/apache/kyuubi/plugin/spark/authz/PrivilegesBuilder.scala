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

package org.apache.kyuubi.plugin.spark.authz

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.StructField

import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectActionType._
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectType._

object PrivilegesBuilder {

  private val versionParts = SPARK_VERSION.split('.')

  private val majorVersion: Int = versionParts.head.toInt
  private val minorVersion: Int = versionParts(1).toInt

  private def quoteIfNeeded(part: String): String = {
    if (part.matches("[a-zA-Z0-9_]+") && !part.matches("\\d+")) {
      part
    } else {
      s"`${part.replace("`", "``")}`"
    }
  }

  private def quote(parts: Seq[String]): String = {
    parts.map(quoteIfNeeded).mkString(".")
  }

  /**
   * fixme error handling need improve here
   */
  private def getFieldVal[T](o: Any, name: String): T = {
    Try {
      val field = o.getClass.getDeclaredField(name)
      field.setAccessible(true)
      field.get(o)
    } match {
      case Success(value) => value.asInstanceOf[T]
      case Failure(e) =>
        val candidates = o.getClass.getDeclaredFields.map(_.getName).mkString("[", ",", "]")
        throw new RuntimeException(s"$name not in $candidates", e)
    }
  }

  private def databasePrivileges(db: String): PrivilegeObject = {
    PrivilegeObject(DATABASE, PrivilegeObjectActionType.OTHER, db, db)
  }

  private def tablePrivileges(
      table: TableIdentifier,
      columns: Seq[String] = Nil,
      actionType: PrivilegeObjectActionType = PrivilegeObjectActionType.OTHER): PrivilegeObject = {
    PrivilegeObject(TABLE_OR_VIEW, actionType, table.database.orNull, table.table, columns)
  }

  private def functionPrivileges(
      db: String,
      functionName: String): PrivilegeObject = {
    PrivilegeObject(FUNCTION, PrivilegeObjectActionType.OTHER, db, functionName)
  }

  /**
   * Build PrivilegeObjects from Spark LogicalPlan
   *
   * @param plan a Spark LogicalPlan used to generate SparkPrivilegeObjects
   * @param privilegeObjects input or output spark privilege object list
   * @param projectionList Projection list after pruning
   */
  private def buildQuery(
      plan: LogicalPlan,
      privilegeObjects: ArrayBuffer[PrivilegeObject],
      projectionList: Seq[NamedExpression] = Nil): Unit = {

    def mergeProjection(table: CatalogTable): Unit = {
      if (projectionList.isEmpty) {
        privilegeObjects += tablePrivileges(
          table.identifier,
          table.partitionColumnNames ++ table.schema.fieldNames)
      } else {
        privilegeObjects += tablePrivileges(table.identifier, projectionList.map(_.name))
      }
    }

    plan match {
      case p: Project => buildQuery(p.child, privilegeObjects, p.projectList)

      case h if h.nodeName == "HiveTableRelation" =>
        mergeProjection(getFieldVal[CatalogTable](h, "tableMeta"))

      case l if l.nodeName == "LogicalRelation" =>
        getFieldVal[Option[CatalogTable]](l, "catalogTable").foreach { t =>
          mergeProjection(t)
        }

      case u if u.nodeName == "UnresolvedRelation" =>
        val tableNameM = u.getClass.getMethod("tableName")
        val parts = tableNameM.invoke(u).asInstanceOf[String].split("\\.")
        val db = quote(parts.init)
        privilegeObjects += tablePrivileges(TableIdentifier(parts.last, Some(db)))

      case p =>
        for (child <- p.children) {
          buildQuery(child, privilegeObjects, projectionList)
        }
    }
  }

  /**
   * Build PrivilegeObjects from Spark LogicalPlan
   * @param plan a Spark LogicalPlan used to generate Spark PrivilegeObjects
   * @param inputObjs input privilege object list
   * @param outputObjs output privilege object list
   */
  private def buildCommand(
      plan: LogicalPlan,
      inputObjs: ArrayBuffer[PrivilegeObject],
      outputObjs: ArrayBuffer[PrivilegeObject]): Unit = {

    def getPlanField[T](field: String): T = {
      getFieldVal[T](plan, field)
    }

    def getTableName: TableIdentifier = {
      getPlanField[TableIdentifier]("tableName")
    }

    def getTableIdent: TableIdentifier = {
      getPlanField[TableIdentifier]("tableIdent")
    }

    def getMultipartIdentifier: TableIdentifier = {
      val multipartIdentifier = getPlanField[Seq[String]]("multipartIdentifier")
      assert(multipartIdentifier.nonEmpty)
      val table = multipartIdentifier.last
      val db = Some(quote(multipartIdentifier.init))
      TableIdentifier(table, db)
    }

    def getQuery: LogicalPlan = {
      getPlanField[LogicalPlan]("query")
    }

    plan.nodeName match {
      case "AlterDatabasePropertiesCommand" |
          "AlterDatabaseSetLocationCommand" |
          "CreateDatabaseCommand" |
          "DropDatabaseCommand" =>
        val database = getPlanField[String]("databaseName")
        outputObjs += databasePrivileges(database)

      case "AlterTableAddColumnsCommand" =>
        val table = getPlanField[TableIdentifier]("table")
        val cols = getPlanField[Seq[StructField]]("colsToAdd").map(_.name)
        outputObjs += tablePrivileges(table, cols)

      case "AlterTableAddPartitionCommand" =>
        val table = getTableName
        val cols = getPlanField[Seq[(TablePartitionSpec, Option[String])]]("partitionSpecsAndLocs")
          .flatMap(_._1.keySet).distinct
        outputObjs += tablePrivileges(table, cols)

      case "AlterTableChangeColumnCommand" =>
        val table = getTableName
        val cols = getPlanField[String]("columnName") :: Nil
        outputObjs += tablePrivileges(table, cols)

      case "AlterTableDropPartitionCommand" =>
        val table = getTableName
        val cols = getPlanField[Seq[TablePartitionSpec]]("specs").flatMap(_.keySet).distinct
        outputObjs += tablePrivileges(table, cols)

      case "AlterTableRenameCommand" =>
        val oldTable = getPlanField[TableIdentifier]("oldName")
        val newTable = getPlanField[TableIdentifier]("newName")
        outputObjs += tablePrivileges(oldTable, actionType = PrivilegeObjectActionType.DELETE)
        outputObjs += tablePrivileges(newTable)

      case "AlterTableRenamePartitionCommand" =>
        val table = getTableName
        val cols = getPlanField[TablePartitionSpec]("oldPartition").keySet.toSeq
        outputObjs += tablePrivileges(table, cols)

      case "AlterTableSerDePropertiesCommand" =>
        val table = getTableName
        val cols = getPlanField[Option[TablePartitionSpec]]("partSpec")
          .toSeq.flatMap(_.keySet)
        outputObjs += tablePrivileges(table, cols)

      case "AlterTableSetLocationCommand" =>
        val table = getTableName
        val cols = getPlanField[Option[TablePartitionSpec]]("partitionSpec")
          .toSeq.flatMap(_.keySet)
        outputObjs += tablePrivileges(table, cols)

      case "AlterTableSetPropertiesCommand" |
          "AlterTableUnsetPropertiesCommand" =>
        val table = getTableName
        outputObjs += tablePrivileges(table)

      case "AlterViewAsCommand" =>
        val view = getPlanField[TableIdentifier]("name")
        outputObjs += tablePrivileges(view)
        buildQuery(getQuery, inputObjs)

      case "AlterViewAs" =>

      case "AnalyzeColumnCommand" =>
        val table = getTableIdent
        val cols = getPlanField[Option[Seq[String]]]("columnNames").getOrElse(Nil)
        inputObjs += tablePrivileges(table, cols)

      case "AnalyzePartitionCommand" =>
        val table = getTableIdent
        val cols = getPlanField[Map[String, Option[String]]]("partitionSpec")
          .keySet.toSeq
        inputObjs += tablePrivileges(table, cols)

      case "AnalyzeTableCommand" |
          "RefreshTableCommand" |
          "RefreshTable" =>
        inputObjs += tablePrivileges(getTableIdent)

      case "AnalyzeTablesCommand" |
          "ShowTablesCommand" =>
        val db = getPlanField[Option[String]]("databaseName")
        if (db.nonEmpty) {
          inputObjs += databasePrivileges(db.get)
        }

      case "CacheTable" =>
        // >= 3.2
        outputObjs += tablePrivileges(getMultipartIdentifier)
        val query = getPlanField[LogicalPlan]("table") // table to cache
        buildQuery(query, inputObjs)

      case "CacheTableCommand" =>
        if (majorVersion == 3 && minorVersion == 1) {
          outputObjs += tablePrivileges(getMultipartIdentifier)
        } else {
          outputObjs += tablePrivileges(getTableIdent)
        }
        getPlanField[Option[LogicalPlan]]("plan").foreach(buildQuery(_, inputObjs))

      case "CacheTableAsSelect" =>
        val view = getPlanField[String]("tempViewName")
        outputObjs += tablePrivileges(TableIdentifier(view))

        val query = getPlanField[LogicalPlan]("plan")
        buildQuery(query, inputObjs)

      case "CreateViewCommand" =>
        val view = getPlanField[TableIdentifier]("name")
        outputObjs += tablePrivileges(view)
        val query = if (majorVersion < 3 || (majorVersion == 3 && minorVersion <= 1)) {
          getPlanField[LogicalPlan]("child")
        } else {
          getPlanField[LogicalPlan]("plan")
        }
        buildQuery(query, inputObjs)

      case "CreateView" => // revisit this after spark has view catalog

      case "CreateDataSourceTableCommand" | "CreateTableCommand" =>
        val table = getPlanField[CatalogTable]("table").identifier
        outputObjs += tablePrivileges(table)

      case "CreateDataSourceTableAsSelectCommand" |
          "OptimizedCreateHiveTableAsSelectCommand" |
          "InsertIntoHiveTable" =>
        val table = getPlanField[CatalogTable]("table").identifier
        outputObjs += tablePrivileges(table)
        buildQuery(getQuery, inputObjs)

      case "CreateHiveTableAsSelectCommand" =>
        val table = getPlanField[CatalogTable]("tableDesc").identifier
        val cols = getPlanField[Seq[String]]("outputColumnNames")
        val mode = getPlanField[SaveMode]("mode").toString
        outputObjs += tablePrivileges(table, cols, PrivilegeObjectActionType(mode))
        buildQuery(getQuery, inputObjs)

      case "CreateFunctionCommand" |
          "DropFunctionCommand" |
          "RefreshFunctionCommand" =>
        val db = getPlanField[Option[String]]("databaseName")
        val functionName = getPlanField[String]("functionName")
        outputObjs += functionPrivileges(db.orNull, functionName)

      case "CreateTableAsSelect" |
          "ReplaceTableAsSelect" =>
        val left = getPlanField[LogicalPlan]("name")
        left.nodeName match {
          case "ResolvedDBObjectName" =>
            val nameParts = getPlanField[Seq[String]]("nameParts")
            val db = Some(quote(nameParts.init))
            outputObjs += tablePrivileges(TableIdentifier(nameParts.last, db))
          case _ =>
        }
        buildQuery(getQuery, inputObjs)

      case "CreateTableLikeCommand" =>
        val target = getPlanField[TableIdentifier]("targetTable")
        val source = getPlanField[TableIdentifier]("sourceTable")
        inputObjs += tablePrivileges(source)
        outputObjs += tablePrivileges(target)

      case "CreateTempViewUsing" =>
        outputObjs += tablePrivileges(getTableIdent)

      case "DescribeColumnCommand" =>
        val table = getPlanField[TableIdentifier]("table")
        val cols = getPlanField[Seq[String]]("colNameParts")
        inputObjs += tablePrivileges(table, cols)

      case "DescribeTableCommand" =>
        val table = getPlanField[TableIdentifier]("table")
        inputObjs += tablePrivileges(table)

      case "DescribeDatabaseCommand" | "SetDatabaseCommand" =>
        val database = getPlanField[String]("databaseName")
        inputObjs += databasePrivileges(database)

      case "DescribeFunctionCommand" =>
        val func = getPlanField[FunctionIdentifier]("functionName")
        inputObjs += functionPrivileges(func.database.orNull, func.funcName)

      case "DropTableCommand" =>
        outputObjs += tablePrivileges(getTableName)

      case "ExplainCommand" =>

      case "ExternalCommandExecutor" =>

      case "InsertIntoDataSourceCommand" =>
        val logicalRelation = getPlanField[LogicalRelation]("logicalRelation")
        logicalRelation.catalogTable.foreach { t =>
          val overwrite = getPlanField[Boolean]("overwrite")
          val actionType = if (overwrite) INSERT_OVERWRITE else INSERT
          outputObjs += tablePrivileges(t.identifier, actionType = actionType)
        }
        buildQuery(getQuery, inputObjs)

      case "InsertIntoDataSourceDirCommand" |
          "SaveIntoDataSourceCommand" |
          "InsertIntoHadoopFsRelationCommand" |
          "InsertIntoHiveDirCommand" =>
        // TODO: Should get the table via datasource options
        buildQuery(getQuery, inputObjs)

      case "LoadDataCommand" =>
        val table = getPlanField[TableIdentifier]("table")
        val overwrite = getPlanField[Boolean]("isOverwrite")
        val actionType = if (overwrite) INSERT_OVERWRITE else INSERT
        val cols = getPlanField[Option[TablePartitionSpec]]("partition")
          .map(_.keySet).getOrElse(Nil)
        outputObjs += tablePrivileges(table, cols.toSeq, actionType = actionType)

      case "MergeIntoTable" =>

      case "RepairTableCommand" =>
        val enableAddPartitions = getPlanField[Boolean]("enableAddPartitions")
        if (enableAddPartitions) {
          outputObjs += tablePrivileges(getTableName, actionType = INSERT)
        } else if (getPlanField[Boolean]("enableDropPartitions")) {
          outputObjs += tablePrivileges(getTableName, actionType = DELETE)
        } else {
          inputObjs += tablePrivileges(getTableName)
        }

      case "SetCatalogCommand" =>
        inputObjs += databasePrivileges(getPlanField[String]("catalogName"))

      case "SetNamespaceCommand" =>
        val namespace = quote(getPlanField[Seq[String]]("namespace"))
        inputObjs += databasePrivileges(namespace)

      case "TruncateTableCommand" =>
        val table = getTableName
        val cols = getPlanField[Option[TablePartitionSpec]]("partitionSpec")
          .map(_.keySet).getOrElse(Nil)
        outputObjs += tablePrivileges(table, cols.toSeq, DELETE)

      case "ShowColumnsCommand" =>
        inputObjs += tablePrivileges(getTableName)

      case "ShowCreateTableCommand" |
          "ShowTablePropertiesCommand" =>
        val table = getPlanField[TableIdentifier]("table")
        inputObjs += tablePrivileges(table)

      case "ShowFunctionsCommand" =>

      case "ShowPartitionsCommand" =>
        inputObjs += tablePrivileges(getTableName)

      case _ =>
      // AddArchivesCommand
      // AddFileCommand
      // AddJarCommand
      // AnalyzeColumnCommand
      // ClearCacheCommand
      // CreateTempViewUsing
      // DescribeFunctionCommand
      // DescribeQueryCommand
      // ExplainCommand
      // ListArchivesCommand
      // ListFilesCommand
      // ListJarsCommand
      // RefreshResource
      // RefreshTable
      // RefreshTable
      // ResetCommand
      // SetCommand
      // ShowDatabasesCommand
      // StreamingExplainCommand
      // UncacheTableCommand
    }
  }

  /**
   * Build input and output privilege objects from a Spark's LogicalPlan
   *
   * For `Command`s, build outputs if it has an target to write, build inputs for the
   * inside query if exists.
   *
   * For other queries, build inputs.
   *
   * @param plan A Spark LogicalPlan
   */
  def build(plan: LogicalPlan): (Seq[PrivilegeObject], Seq[PrivilegeObject]) = {
    val inputObjs = new ArrayBuffer[PrivilegeObject]
    val outputObjs = new ArrayBuffer[PrivilegeObject]
    plan match {
      // RunnableCommand
      case cmd: Command => buildCommand(cmd, inputObjs, outputObjs)
      // Queries
      case _ => buildQuery(plan, inputObjs)
    }
    (inputObjs, outputObjs)
  }
}
