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

package org.apache.kyuubi.plugin.spark.authz.gen

import org.apache.kyuubi.plugin.spark.authz.OperationType._
import org.apache.kyuubi.plugin.spark.authz.PrivilegeObjectActionType._
import org.apache.kyuubi.plugin.spark.authz.serde._
import org.apache.kyuubi.plugin.spark.authz.serde.TableType._

object TableCommands extends CommandSpecs[TableCommandSpec] {
  // table extractors
  val tite = classOf[TableIdentifierTableExtractor]
  val tableNameDesc = TableDesc("tableName", tite)
  val tableIdentDesc = TableDesc("tableIdent", tite)
  val resolvedTableDesc = TableDesc("child", classOf[ResolvedTableTableExtractor])
  val resolvedDbObjectNameDesc =
    TableDesc("child", classOf[ResolvedDbObjectNameTableExtractor])
  val resolvedIdentifierTableDesc =
    TableDesc("child", classOf[ResolvedIdentifierTableExtractor])
  val overwriteActionTypeDesc =
    ActionTypeDesc("overwrite", classOf[OverwriteOrInsertActionTypeExtractor])
  val queryQueryDesc = QueryDesc("query")

  val AlterTable = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.AlterTable"
    val tableDesc = TableDesc("ident", classOf[IdentifierTableExtractor])
    TableCommandSpec(cmd, Seq(tableDesc), ALTERTABLE_PROPERTIES)
  }

  val AlterTableAddColumns = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableAddColumnsCommand"
    val columnDesc = ColumnDesc("colsToAdd", classOf[StructFieldSeqColumnExtractor])
    val tableDesc = TableDesc("table", tite, Some(columnDesc))
    TableCommandSpec(cmd, Seq(tableDesc), ALTERTABLE_ADDCOLS)
  }

  val AddColumns = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.AddColumns"
    TableCommandSpec(cmd, Seq(resolvedTableDesc), ALTERTABLE_ADDCOLS)
  }

  val AlterColumn = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.AlterColumn"
    AddColumns.copy(classname = cmd)
  }

  val DropColumns = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.DropColumns"
    AddColumns.copy(classname = cmd)
  }

  val ReplaceColumns = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.ReplaceColumns"
    TableCommandSpec(cmd, Seq(resolvedTableDesc), ALTERTABLE_REPLACECOLS)
  }

  val RenameColumn = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.RenameColumn"
    TableCommandSpec(cmd, Seq(resolvedTableDesc), ALTERTABLE_RENAMECOL)
  }

  val AlterTableAddPartition = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableAddPartitionCommand"
    val columnDesc =
      ColumnDesc("partitionSpecsAndLocs", classOf[PartitionLocsSeqColumnExtractor])
    TableCommandSpec(
      cmd,
      Seq(tableNameDesc.copy(columnDesc = Some(columnDesc))),
      ALTERTABLE_ADDPARTS)
  }

  val AlterTableChangeColumn = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableChangeColumnCommand"
    val columnDesc = ColumnDesc("columnName", classOf[StringColumnExtractor])
    TableCommandSpec(
      cmd,
      Seq(tableNameDesc.copy(columnDesc = Some(columnDesc))),
      ALTERTABLE_REPLACECOLS)
  }

  val AlterTableDropPartition = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableDropPartitionCommand"
    val columnDesc = ColumnDesc("specs", classOf[PartitionSeqColumnExtractor])
    TableCommandSpec(
      cmd,
      Seq(tableNameDesc.copy(columnDesc = Some(columnDesc))),
      ALTERTABLE_DROPPARTS)
  }

  val AlterTableRename = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableRenameCommand"

    val oldTableTableTypeDesc =
      TableTypeDesc(
        "oldName",
        classOf[TableIdentifierTableTypeExtractor],
        Seq(TEMP_VIEW))
    val oldTableD = TableDesc(
      "oldName",
      tite,
      tableTypeDesc = Some(oldTableTableTypeDesc))

    TableCommandSpec(cmd, Seq(oldTableD), ALTERTABLE_RENAME)
  }

  // this is for spark 3.1 or below
  val AlterTableRecoverPartitions = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableRecoverPartitionsCommand"
    TableCommandSpec(cmd, Seq(tableNameDesc), MSCK)
  }

  val RepairTable = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.RepairTable"
    TableCommandSpec(cmd, Seq(resolvedTableDesc), MSCK)
  }

  val AlterTableRenamePartition = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableRenamePartitionCommand"
    val columnDesc = ColumnDesc("oldPartition", classOf[PartitionColumnExtractor])
    TableCommandSpec(
      cmd,
      Seq(tableNameDesc.copy(columnDesc = Some(columnDesc))),
      ALTERTABLE_RENAMEPART)
  }

  val AlterTableSerDeProperties = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableSerDePropertiesCommand"
    val columnDesc = ColumnDesc("partSpec", classOf[PartitionOptionColumnExtractor])
    TableCommandSpec(
      cmd,
      Seq(tableNameDesc.copy(columnDesc = Some(columnDesc))),
      ALTERTABLE_SERDEPROPERTIES)
  }

  val AlterTableSetLocation = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableSetLocationCommand"
    val columnDesc = ColumnDesc("partitionSpec", classOf[PartitionOptionColumnExtractor])
    TableCommandSpec(
      cmd,
      Seq(tableNameDesc.copy(columnDesc = Some(columnDesc))),
      ALTERTABLE_LOCATION)
  }

  val AlterTableSetProperties = TableCommandSpec(
    "org.apache.spark.sql.execution.command.AlterTableSetPropertiesCommand",
    Seq(tableNameDesc),
    ALTERTABLE_PROPERTIES)

  val AlterTableUnsetProperties = AlterTableSetProperties.copy(classname =
    "org.apache.spark.sql.execution.command.AlterTableUnsetPropertiesCommand")

  val AlterViewAs = {
    val tableTypeDesc =
      TableTypeDesc("name", classOf[TableIdentifierTableTypeExtractor], Seq("TEMP_VIEW"))

    TableCommandSpec(
      "org.apache.spark.sql.execution.command.AlterViewAsCommand",
      Seq(TableDesc("name", tite, tableTypeDesc = Some(tableTypeDesc))),
      ALTERVIEW_AS,
      Seq(queryQueryDesc))
  }

  val AnalyzeColumn = {
    val cmd = "org.apache.spark.sql.execution.command.AnalyzeColumnCommand"
    val cd1 = ColumnDesc("columnNames", classOf[StringSeqColumnExtractor])
    val cd2 = cd1.copy(fieldExtractor = classOf[StringSeqOptionColumnExtractor])
    val td1 = tableIdentDesc.copy(columnDesc = Some(cd1), isInput = true)
    val td2 = td1.copy(columnDesc = Some(cd2))
    // AnalyzeColumn will update table properties, here we use ALTERTABLE_PROPERTIES
    TableCommandSpec(cmd, Seq(tableIdentDesc, td1, td2), ALTERTABLE_PROPERTIES)
  }

  val AnalyzePartition = {
    val cmd = "org.apache.spark.sql.execution.command.AnalyzePartitionCommand"
    val columnDesc = ColumnDesc("partitionSpec", classOf[PartitionColumnExtractor])
    TableCommandSpec(
      cmd,
      // AnalyzePartition will update table properties, here we use ALTERTABLE_PROPERTIES
      Seq(tableIdentDesc, tableIdentDesc.copy(columnDesc = Some(columnDesc), isInput = true)),
      ALTERTABLE_PROPERTIES)
  }

  val AnalyzeTable = {
    val cmd = "org.apache.spark.sql.execution.command.AnalyzeTableCommand"
    TableCommandSpec(
      cmd,
      // AnalyzeTable will update table properties, here we use ALTERTABLE_PROPERTIES
      Seq(tableIdentDesc, tableIdentDesc.copy(isInput = true)),
      ALTERTABLE_PROPERTIES)
  }

  val CreateTableV2 = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.CreateTable"
    val tableDesc = TableDesc(
      "tableName",
      classOf[IdentifierTableExtractor],
      catalogDesc = Some(CatalogDesc()))
    TableCommandSpec(
      cmd,
      Seq(resolvedIdentifierTableDesc, tableDesc, resolvedDbObjectNameDesc),
      CREATETABLE)
  }

  val CreateV2Table = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.CreateV2Table"
    val tableDesc = TableDesc(
      "tableName",
      classOf[IdentifierTableExtractor],
      catalogDesc = Some(CatalogDesc()))
    TableCommandSpec(cmd, Seq(tableDesc), CREATETABLE)
  }

  val CreateTableAsSelectV2 = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect"
    val tableDesc = TableDesc(
      "tableName",
      classOf[IdentifierTableExtractor],
      catalogDesc = Some(CatalogDesc()))
    TableCommandSpec(
      cmd,
      Seq(
        resolvedIdentifierTableDesc.copy(fieldName = "name"),
        tableDesc,
        resolvedDbObjectNameDesc.copy(fieldName = "name")),
      CREATETABLE_AS_SELECT,
      Seq(queryQueryDesc))
  }

  val CommentOnTable = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.CommentOnTable"
    TableCommandSpec(cmd, Seq(resolvedTableDesc), ALTERTABLE_PROPERTIES)
  }

  val AppendDataV2 = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.AppendData"
    val actionTypeDesc = ActionTypeDesc(actionType = Some(INSERT))
    val tableDesc =
      TableDesc(
        "table",
        classOf[DataSourceV2RelationTableExtractor],
        actionTypeDesc = Some(actionTypeDesc))
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(queryQueryDesc))
  }

  val ReplaceData = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.ReplaceData"
    val actionTypeDesc = ActionTypeDesc(actionType = Some(UPDATE))
    val tableDesc =
      TableDesc(
        "originalTable",
        classOf[DataSourceV2RelationTableExtractor],
        actionTypeDesc = Some(actionTypeDesc))
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(queryQueryDesc))
  }

  val UpdateTable = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.UpdateTable"
    val actionTypeDesc = ActionTypeDesc(actionType = Some(UPDATE))
    val tableDesc =
      TableDesc(
        "table",
        classOf[DataSourceV2RelationTableExtractor],
        actionTypeDesc = Some(actionTypeDesc))
    TableCommandSpec(cmd, Seq(tableDesc))
  }

  val DeleteFromTable = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable"
    UpdateTable.copy(classname = cmd)
  }

  val OverwriteByExpression = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression"
    val actionTypeDesc = ActionTypeDesc(actionType = Some(INSERT_OVERWRITE))
    val tableDesc =
      TableDesc(
        "table",
        classOf[DataSourceV2RelationTableExtractor],
        actionTypeDesc = Some(actionTypeDesc))
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(queryQueryDesc))
  }

  val OverwritePartitionsDynamic = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.OverwritePartitionsDynamic"
    OverwriteByExpression.copy(classname = cmd)
  }

  val AddPartitions = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.AddPartitions"
    // TODO: add column desc
    val tableDesc = TableDesc("table", classOf[DataSourceV2RelationTableExtractor])
    TableCommandSpec(cmd, Seq(tableDesc), ALTERTABLE_ADDPARTS)
  }

  val DropPartitions = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.DropPartitions"
    // TODO: add column desc
    val tableDesc = TableDesc("table", classOf[DataSourceV2RelationTableExtractor])
    TableCommandSpec(cmd, Seq(tableDesc), ALTERTABLE_DROPPARTS)
  }

  val RenamePartitions = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.RenamePartitions"
    // TODO: add column desc
    val tableDesc = TableDesc("table", classOf[DataSourceV2RelationTableExtractor])
    TableCommandSpec(cmd, Seq(tableDesc), ALTERTABLE_RENAMEPART)
  }

  val TruncatePartition = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.TruncatePartition"
    // TODO: add column desc
    val tableDesc = TableDesc("table", classOf[DataSourceV2RelationTableExtractor])
    TableCommandSpec(cmd, Seq(tableDesc), ALTERTABLE_DROPPARTS)
  }

  val CacheTableAsSelect = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.CacheTableAsSelect"
    TableCommandSpec(cmd, Nil, CREATEVIEW, queryDescs = Seq(QueryDesc("plan")))
  }

  val CacheTable = {
    val cmd = "org.apache.spark.sql.execution.command.CacheTableCommand"
    val queryDesc = QueryDesc("plan", classOf[LogicalPlanOptionQueryExtractor])
    TableCommandSpec(cmd, Nil, CREATEVIEW, queryDescs = Seq(queryDesc))
  }

  val CacheTableV2 = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.CacheTable"
    TableCommandSpec(cmd, Nil, CREATEVIEW, Seq(QueryDesc("table")))
  }

  val CreateView = {
    val cmd = "org.apache.spark.sql.execution.command.CreateViewCommand"
    val tableTypeDesc = TableTypeDesc(
      "viewType",
      classOf[ViewTypeTableTypeExtractor],
      Seq(TEMP_VIEW, GLOBAL_TEMP_VIEW))
    val tableDesc = TableDesc(
      "name",
      classOf[TableIdentifierTableExtractor],
      tableTypeDesc = Some(tableTypeDesc))
    val queryDesc1 = QueryDesc("plan")
    val queryDesc2 = QueryDesc("child")
    TableCommandSpec(
      cmd,
      Seq(tableDesc),
      CREATEVIEW,
      queryDescs = Seq(queryDesc1, queryDesc2))
  }

  val CreateTempViewUsing = {
    val cmd = "org.apache.spark.sql.execution.datasources.CreateTempViewUsing"
    TableCommandSpec(cmd, Nil, CREATEVIEW)
  }

  val CreateTable = {
    val cmd = "org.apache.spark.sql.execution.datasources.CreateTable"
    val tableDesc = TableDesc("tableDesc", classOf[CatalogTableTableExtractor])
    val queryDesc = QueryDesc("query", "LogicalPlanOptionQueryExtractor")
    TableCommandSpec(cmd, Seq(tableDesc), CREATETABLE, queryDescs = Seq(queryDesc))
  }

  val CreateDataSourceTable = {
    val cmd = "org.apache.spark.sql.execution.command.CreateDataSourceTableCommand"
    val tableDesc =
      TableDesc("table", classOf[CatalogTableTableExtractor], setCurrentDatabaseIfMissing = true)
    TableCommandSpec(cmd, Seq(tableDesc), CREATETABLE)
  }

  val CreateDataSourceTableAsSelect = {
    val cmd = "org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand"
    CreateDataSourceTable.copy(
      classname = cmd,
      opType = CREATETABLE_AS_SELECT,
      queryDescs = Seq(queryQueryDesc))
  }

  val CreateHiveTableAsSelect = {
    val cmd = "org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand"
    val columnDesc = ColumnDesc("outputColumnNames", classOf[StringSeqColumnExtractor])
    val tableDesc =
      TableDesc("tableDesc", classOf[CatalogTableTableExtractor], Some(columnDesc))
    val queryDesc = queryQueryDesc
    TableCommandSpec(cmd, Seq(tableDesc), "CREATETABLE_AS_SELECT", queryDescs = Seq(queryDesc))
  }

  val CreateTableLike = {
    val cmd = "org.apache.spark.sql.execution.command.CreateTableLikeCommand"
    val tableDesc1 = TableDesc(
      "targetTable",
      classOf[TableIdentifierTableExtractor],
      setCurrentDatabaseIfMissing = true)
    val tableDesc2 = TableDesc(
      "sourceTable",
      classOf[TableIdentifierTableExtractor],
      isInput = true,
      setCurrentDatabaseIfMissing = true)
    TableCommandSpec(cmd, Seq(tableDesc1, tableDesc2), CREATETABLE)
  }

  val DescribeColumn = {
    val cmd = "org.apache.spark.sql.execution.command.DescribeColumnCommand"
    val columnDesc = ColumnDesc("colNameParts", classOf[StringSeqLastColumnExtractor])
    val tableDesc = TableDesc(
      "table",
      classOf[TableIdentifierTableExtractor],
      Some(columnDesc),
      isInput = true)
    TableCommandSpec(cmd, Seq(tableDesc), DESCTABLE)
  }

  val DescribeTable = {
    val cmd = "org.apache.spark.sql.execution.command.DescribeTableCommand"
    val columnDesc = ColumnDesc("partitionSpec", classOf[PartitionColumnExtractor])
    val tableDesc = TableDesc(
      "table",
      classOf[TableIdentifierTableExtractor],
      Some(columnDesc),
      isInput = true,
      setCurrentDatabaseIfMissing = true)
    TableCommandSpec(cmd, Seq(tableDesc), DESCTABLE)
  }

  val DescribeRelationTable = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.DescribeRelation"
    val tableDesc = TableDesc(
      "relation",
      classOf[ResolvedTableTableExtractor],
      isInput = true,
      setCurrentDatabaseIfMissing = true)
    TableCommandSpec(cmd, Seq(tableDesc), DESCTABLE)
  }

  val DropTable = {
    val cmd = "org.apache.spark.sql.execution.command.DropTableCommand"
    val tableTypeDesc =
      TableTypeDesc(
        "tableName",
        classOf[TableIdentifierTableTypeExtractor],
        Seq(TEMP_VIEW))
    TableCommandSpec(
      cmd,
      Seq(tableNameDesc.copy(tableTypeDesc = Some(tableTypeDesc))),
      DROPTABLE)
  }

  val DropTableV2 = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.DropTable"
    TableCommandSpec(cmd, Seq(resolvedIdentifierTableDesc, resolvedTableDesc), DROPTABLE)
  }

  val MergeIntoTable = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable"
    val actionTypeDesc = ActionTypeDesc(actionType = Some(UPDATE))
    val tableDesc = TableDesc(
      "targetTable",
      classOf[DataSourceV2RelationTableExtractor],
      actionTypeDesc = Some(actionTypeDesc))
    val queryDesc = QueryDesc("sourceTable")
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(queryDesc))
  }

  val ShowColumns = {
    val cmd = "org.apache.spark.sql.execution.command.ShowColumnsCommand"
    val tableDesc = tableNameDesc.copy(isInput = true)
    TableCommandSpec(cmd, Seq(tableDesc), SHOWCOLUMNS)
  }

  val ShowCreateTable = {
    val cmd = "org.apache.spark.sql.execution.command.ShowCreateTableCommand"
    val tableDesc = tableNameDesc.copy(fieldName = "table", isInput = true)
    TableCommandSpec(cmd, Seq(tableDesc), SHOW_CREATETABLE)
  }

  val ShowTableProperties = {
    val cmd = "org.apache.spark.sql.execution.command.ShowTablePropertiesCommand"
    val tableDesc = tableNameDesc.copy(fieldName = "table", isInput = true)
    TableCommandSpec(cmd, Seq(tableDesc), SHOW_TBLPROPERTIES)
  }

  val ShowCreateTableV2 = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.ShowCreateTable"
    val tableDesc =
      TableDesc("child", classOf[ResolvedTableTableExtractor], isInput = true)
    TableCommandSpec(cmd, Seq(tableDesc), SHOW_CREATETABLE)
  }

  val ShowTablePropertiesV2 = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.ShowTableProperties"
    val tableDesc =
      TableDesc("table", classOf[ResolvedTableTableExtractor], isInput = true)
    TableCommandSpec(cmd, Seq(tableDesc), SHOW_TBLPROPERTIES)
  }

  val ShowPartitions = {
    val cmd = "org.apache.spark.sql.execution.command.ShowPartitionsCommand"
    val columnDesc = ColumnDesc("spec", classOf[PartitionOptionColumnExtractor])
    val tableDesc = tableNameDesc.copy(isInput = true, columnDesc = Some(columnDesc))
    TableCommandSpec(cmd, Seq(tableDesc), SHOWPARTITIONS)
  }

  val TruncateTable = {
    val cmd = "org.apache.spark.sql.execution.command.TruncateTableCommand"
    val columnDesc = ColumnDesc("partitionSpec", classOf[PartitionOptionColumnExtractor])
    val tableDesc = tableNameDesc.copy(columnDesc = Some(columnDesc))
    TableCommandSpec(cmd, Seq(tableDesc), TRUNCATETABLE)
  }

  val TruncateTableV2 = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.TruncateTable"
    TableCommandSpec(cmd, Seq(resolvedTableDesc), TRUNCATETABLE)
  }

  val InsertIntoDataSource = {
    val cmd = "org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand"
    val actionTypeDesc = overwriteActionTypeDesc
    val tableDesc = TableDesc(
      "logicalRelation",
      classOf[LogicalRelationTableExtractor],
      actionTypeDesc = Some(actionTypeDesc))
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(queryQueryDesc))
  }

  val InsertIntoHiveTable = {
    val cmd = "org.apache.spark.sql.hive.execution.InsertIntoHiveTable"
    val actionTypeDesc = overwriteActionTypeDesc
    val columnDesc = ColumnDesc("outputColumnNames", classOf[StringSeqColumnExtractor])
    val tableDesc = TableDesc(
      "table",
      classOf[CatalogTableTableExtractor],
      Some(columnDesc),
      Some(actionTypeDesc))
    val queryDesc = queryQueryDesc
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(queryDesc))
  }

  val InsertIntoDataSourceDir = {
    val cmd = "org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand"
    val queryDesc = queryQueryDesc
    val actionTypeDesc = overwriteActionTypeDesc
    val uriDesc = UriDesc(
      "storage",
      classOf[CatalogStorageFormatURIExtractor],
      actionTypeDesc = Some(actionTypeDesc))
    TableCommandSpec(cmd, Nil, queryDescs = Seq(queryDesc), uriDescs = Seq(uriDesc))
  }

  val SaveIntoDataSourceCommand = {
    val cmd = "org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand"
    val queryDesc = queryQueryDesc
    val actionTypeDesc = ActionTypeDesc("mode", classOf[SaveModeActionTypeExtractor])
    val uriDesc = UriDesc(
      "options",
      classOf[OptionsUriExtractor],
      actionTypeDesc = Some(actionTypeDesc))
    TableCommandSpec(cmd, Nil, queryDescs = Seq(queryDesc), uriDescs = Seq(uriDesc))
  }

  val InsertIntoHadoopFsRelationCommand = {
    val cmd = "org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand"
    val actionTypeDesc = ActionTypeDesc("mode", classOf[SaveModeActionTypeExtractor])
    val columnDesc = ColumnDesc("outputColumnNames", classOf[StringSeqColumnExtractor])
    val tableDesc = TableDesc(
      "catalogTable",
      classOf[CatalogTableOptionTableExtractor],
      Some(columnDesc),
      actionTypeDesc = Some(actionTypeDesc))
    val queryDesc = queryQueryDesc
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(queryDesc))
  }

  val LoadData = {
    val cmd = "org.apache.spark.sql.execution.command.LoadDataCommand"
    val actionTypeDesc = overwriteActionTypeDesc.copy(fieldName = "isOverwrite")
    val columnDesc = ColumnDesc("partition", classOf[PartitionOptionColumnExtractor])
    val tableDesc = tableIdentDesc.copy(
      fieldName = "table",
      columnDesc = Some(columnDesc),
      actionTypeDesc = Some(actionTypeDesc))
    val uriDesc = UriDesc("path", classOf[StringURIExtractor], isInput = true)
    TableCommandSpec(cmd, Seq(tableDesc), LOAD, uriDescs = Seq(uriDesc))
  }

  val RefreshTable = {
    val cmd = "org.apache.spark.sql.execution.command.RefreshTableCommand"
    TableCommandSpec(cmd, Seq(tableIdentDesc.copy(isInput = true)))
  }

  val RefreshTableV2 = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.RefreshTable"
    TableCommandSpec(cmd, Seq(resolvedTableDesc.copy(isInput = true)))
  }

  val RefreshTable3d0 = {
    val cmd = "org.apache.spark.sql.execution.datasources.RefreshTable"
    TableCommandSpec(cmd, Seq(tableIdentDesc.copy(isInput = true)))
  }

  val SetTableProperties = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.SetTableProperties"
    val tableDesc = TableDesc("table", classOf[ResolvedTableTableExtractor])
    TableCommandSpec(cmd, Seq(tableDesc), ALTERTABLE_PROPERTIES)
  }

  override def specs: Seq[TableCommandSpec] = Seq(
    AddPartitions,
    DropPartitions,
    RenamePartitions,
    TruncatePartition,
    AddColumns,
    AlterColumn,
    DropColumns,
    ReplaceColumns,
    RenameColumn,
    AlterTable,
    AlterTableAddColumns,
    AlterTableAddPartition,
    AlterTableChangeColumn,
    AlterTableDropPartition,
    AlterTableRename,
    AlterTableRecoverPartitions,
    AlterTableRecoverPartitions.copy(classname =
      "org.apache.spark.sql.execution.command.RepairTableCommand"),
    AlterTableRenamePartition,
    AlterTableSerDeProperties,
    AlterTableSetLocation,
    AlterTableSetProperties,
    AlterTableUnsetProperties,
    AlterViewAs,
    AnalyzeColumn,
    AnalyzePartition,
    AnalyzeTable,
    AppendDataV2,
    CacheTable,
    CacheTableAsSelect,
    CacheTableV2,
    CommentOnTable,
    CreateDataSourceTable,
    CreateDataSourceTable.copy(classname =
      "org.apache.spark.sql.execution.command.CreateTableCommand"),
    CreateDataSourceTableAsSelect,
    CreateHiveTableAsSelect,
    CreateHiveTableAsSelect.copy(classname =
      "org.apache.spark.sql.hive.execution.OptimizedCreateHiveTableAsSelectCommand"),
    CreateTable,
    CreateTableLike,
    CreateTableV2,
    CreateTableV2.copy(classname =
      "org.apache.spark.sql.catalyst.plans.logical.ReplaceTable"),
    CreateV2Table,
    CreateTableAsSelectV2,
    CreateTableAsSelectV2.copy(classname =
      "org.apache.spark.sql.catalyst.plans.logical.ReplaceTableAsSelect"),
    CreateView,
    CreateTempViewUsing,
    DeleteFromTable,
    DescribeColumn,
    DescribeTable,
    DescribeRelationTable,
    DropTable,
    DropTableV2,
    InsertIntoDataSource,
    InsertIntoDataSourceDir,
    SaveIntoDataSourceCommand,
    InsertIntoHadoopFsRelationCommand,
    InsertIntoDataSourceDir.copy(classname =
      "org.apache.spark.sql.hive.execution.InsertIntoHiveDirCommand"),
    InsertIntoHiveTable,
    LoadData,
    MergeIntoTable,
    OverwriteByExpression,
    OverwritePartitionsDynamic,
    RepairTable,
    RefreshTable,
    RefreshTableV2,
    RefreshTable3d0,
    ReplaceData,
    SetTableProperties,
    ShowColumns,
    ShowCreateTable,
    ShowCreateTable.copy(classname =
      "org.apache.spark.sql.execution.command.ShowCreateTableAsSerdeCommand"),
    ShowCreateTableV2,
    ShowPartitions,
    ShowTableProperties,
    ShowTablePropertiesV2,
    TruncateTable,
    TruncateTableV2,
    UpdateTable)
}
