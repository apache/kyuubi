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

import org.apache.kyuubi.plugin.spark.authz.serde._

object TableCommands {
  // table extractors
  val tite = classOf[TableIdentifierTableExtractor].getSimpleName
  val tableNameDesc = TableDesc("tableName", tite)
  val tableIdentDesc = TableDesc("tableIdent", tite)
  val resolvedTableDesc = TableDesc("child", "ResolvedTableTableExtractor")
  val resolvedDbObjectNameDesc = TableDesc("child", "ResolvedDbObjectNameTableExtractor")

  val overwriteActionTypeDesc =
    ActionTypeDesc("overwrite", "OverwriteOrInsertActionTypeExtractor")

  val AlterTable = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.AlterTable"
    val tableDesc = TableDesc("ident", classOf[IdentifierTableExtractor].getSimpleName)
    TableCommandSpec(cmd, Seq(tableDesc), "ALTERTABLE_PROPERTIES")
  }

  val AlterTableAddColumns = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableAddColumnsCommand"
    val columnDesc = ColumnDesc("colsToAdd", "StructFieldSeqColumnExtractor")
    val tableDesc = TableDesc("table", tite, Some(columnDesc))
    TableCommandSpec(cmd, Seq(tableDesc), "ALTERTABLE_ADDCOLS")
  }

  val AddColumns = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.AddColumns"
    TableCommandSpec(cmd, Seq(resolvedTableDesc), "ALTERTABLE_ADDCOLS")
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
    TableCommandSpec(cmd, Seq(resolvedTableDesc), "ALTERTABLE_REPLACECOLS")
  }

  val RenameColumn = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.RenameColumn"
    TableCommandSpec(cmd, Seq(resolvedTableDesc), "ALTERTABLE_RENAMECOL")
  }

  val AlterTableAddPartition = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableAddPartitionCommand"
    val columnDesc = ColumnDesc("partitionSpecsAndLocs", "PartitionLocsSeqColumnExtractor")
    TableCommandSpec(
      cmd,
      Seq(tableNameDesc.copy(columnDesc = Some(columnDesc))),
      "ALTERTABLE_ADDPARTS")
  }

  val AlterTableChangeColumn = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableChangeColumnCommand"
    val columnDesc = ColumnDesc("columnName", "StringColumnExtractor")
    TableCommandSpec(
      cmd,
      Seq(tableNameDesc.copy(columnDesc = Some(columnDesc))),
      "ALTERTABLE_REPLACECOLS")
  }

  val AlterTableDropPartition = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableDropPartitionCommand"
    val columnDesc = ColumnDesc("specs", "PartitionSeqColumnExtractor")
    TableCommandSpec(
      cmd,
      Seq(tableNameDesc.copy(columnDesc = Some(columnDesc))),
      "ALTERTABLE_DROPPARTS")
  }

  val AlterTableRename = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableRenameCommand"
    val actionTypeDesc = ActionTypeDesc(null, null, Some("DELETE"))

    val oldTableTableTypeDesc =
      TableTypeDesc("oldName", "TableIdentifierTableTypeExtractor", Seq("TEMP_VIEW"))
    val oldTableD = TableDesc(
      "oldName",
      tite,
      tableTypeDesc = Some(oldTableTableTypeDesc),
      actionTypeDesc = Some(actionTypeDesc))

    val newTableD =
      TableDesc("newName", tite, tableTypeDesc = Some(oldTableTableTypeDesc))
    TableCommandSpec(cmd, Seq(oldTableD, newTableD), "ALTERTABLE_RENAME")
  }

  // this is for spark 3.1 or below
  val AlterTableRecoverPartitions = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableRecoverPartitionsCommand"
    TableCommandSpec(cmd, Seq(tableNameDesc), "MSCK")
  }

  val RepairTable = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.RepairTable"
    TableCommandSpec(cmd, Seq(resolvedTableDesc), "MSCK")
  }

  val AlterTableRenamePartition = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableRenamePartitionCommand"
    val columnDesc = ColumnDesc("oldPartition", "PartitionColumnExtractor")
    TableCommandSpec(
      cmd,
      Seq(tableNameDesc.copy(columnDesc = Some(columnDesc))),
      "ALTERTABLE_RENAMEPART")
  }

  val AlterTableSerDeProperties = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableSerDePropertiesCommand"
    val columnDesc = ColumnDesc("partSpec", "PartitionOptionColumnExtractor")
    TableCommandSpec(
      cmd,
      Seq(tableNameDesc.copy(columnDesc = Some(columnDesc))),
      "ALTERTABLE_SERDEPROPERTIES")
  }

  val AlterTableSetLocation = {
    val cmd = "org.apache.spark.sql.execution.command.AlterTableSetLocationCommand"
    val columnDesc = ColumnDesc("partitionSpec", "PartitionOptionColumnExtractor")
    TableCommandSpec(
      cmd,
      Seq(tableNameDesc.copy(columnDesc = Some(columnDesc))),
      "ALTERTABLE_LOCATION")
  }

  val AlterTableSetProperties = TableCommandSpec(
    "org.apache.spark.sql.execution.command.AlterTableSetPropertiesCommand",
    Seq(tableNameDesc),
    "ALTERTABLE_PROPERTIES")

  val AlterTableUnsetProperties = AlterTableSetProperties.copy(classname =
    "org.apache.spark.sql.execution.command.AlterTableUnsetPropertiesCommand")

  val AlterViewAs = {
    val tableTypeDesc =
      TableTypeDesc("name", "TableIdentifierTableTypeExtractor", Seq("TEMP_VIEW"))

    TableCommandSpec(
      "org.apache.spark.sql.execution.command.AlterViewAsCommand",
      Seq(TableDesc("name", tite, tableTypeDesc = Some(tableTypeDesc))),
      "ALTERVIEW_AS",
      Seq(QueryDesc("query")))
  }

  val AnalyzeColumn = {
    val cmd = "org.apache.spark.sql.execution.command.AnalyzeColumnCommand"
    val cd1 = ColumnDesc("columnNames", "StringSeqColumnExtractor")
    val cd2 = cd1.copy(fieldExtractor = "StringSeqOptionColumnExtractor")
    val td1 = tableIdentDesc.copy(columnDesc = Some(cd1), isInput = true)
    val td2 = td1.copy(columnDesc = Some(cd2))
    TableCommandSpec(cmd, Seq(td1, td2), "ANALYZE_TABLE")
  }

  val AnalyzePartition = {
    val cmd = "org.apache.spark.sql.execution.command.AnalyzePartitionCommand"
    val columnDesc = ColumnDesc("partitionSpec", "PartitionColumnExtractor")
    TableCommandSpec(
      cmd,
      Seq(tableIdentDesc.copy(columnDesc = Some(columnDesc), isInput = true)),
      "ANALYZE_TABLE")
  }

  val AnalyzeTable = {
    val cmd = "org.apache.spark.sql.execution.command.AnalyzeTableCommand"
    TableCommandSpec(
      cmd,
      Seq(tableIdentDesc.copy(isInput = true)),
      "ANALYZE_TABLE")
  }

  val CreateTableV2 = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.CreateTable"
    val tableDesc = TableDesc(
      "tableName",
      "IdentifierTableExtractor",
      catalogDesc = Some(CatalogDesc()))
    TableCommandSpec(cmd, Seq(tableDesc, resolvedDbObjectNameDesc), "CREATETABLE")
  }

  val CreateV2Table = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.CreateV2Table"
    val tableDesc = TableDesc(
      "tableName",
      "IdentifierTableExtractor",
      catalogDesc = Some(CatalogDesc()))
    TableCommandSpec(cmd, Seq(tableDesc), "CREATETABLE")
  }

  val CreateTableAsSelectV2 = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect"
    val tableDesc = TableDesc(
      "tableName",
      "IdentifierTableExtractor",
      catalogDesc = Some(CatalogDesc()))
    TableCommandSpec(
      cmd,
      Seq(tableDesc, resolvedDbObjectNameDesc.copy(fieldName = "left")),
      "CREATETABLE_AS_SELECT",
      Seq(QueryDesc("query")))
  }

  val CommentOnTable = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.CommentOnTable"
    TableCommandSpec(cmd, Seq(resolvedTableDesc), "ALTERTABLE_PROPERTIES")
  }

  val AppendDataV2 = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.AppendData"
    val actionTypeDesc = ActionTypeDesc(null, null, Some("INSERT"))
    val tableDesc =
      TableDesc(
        "table",
        "DataSourceV2RelationTableExtractor",
        actionTypeDesc = Some(actionTypeDesc))
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(QueryDesc("query")))
  }

  val UpdateTable = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.UpdateTable"
    val actionTypeDesc = ActionTypeDesc(null, null, Some("UPDATE"))
    val tableDesc =
      TableDesc(
        "table",
        "DataSourceV2RelationTableExtractor",
        actionTypeDesc = Some(actionTypeDesc))
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(QueryDesc("query")))
  }

  val DeleteFromTable = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable"
    UpdateTable.copy(classname = cmd)
  }

  val OverwriteByExpression = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression"
    val actionTypeDesc = ActionTypeDesc(null, null, Some("INSERT_OVERWRITE"))
    val tableDesc =
      TableDesc(
        "table",
        "DataSourceV2RelationTableExtractor",
        actionTypeDesc = Some(actionTypeDesc))
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(QueryDesc("query")))
  }

  val OverwritePartitionsDynamic = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.OverwritePartitionsDynamic"
    OverwriteByExpression.copy(classname = cmd)
  }

  val AddPartitions = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.AddPartitions"
    // TODO: add column desc
    val tableDesc = TableDesc("table", "DataSourceV2RelationTableExtractor")
    TableCommandSpec(cmd, Seq(tableDesc), "ALTERTABLE_ADDPARTS")
  }

  val DropPartitions = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.DropPartitions"
    // TODO: add column desc
    val tableDesc = TableDesc("table", "DataSourceV2RelationTableExtractor")
    TableCommandSpec(cmd, Seq(tableDesc), "ALTERTABLE_DROPPARTS")
  }

  val RenamePartitions = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.RenamePartitions"
    // TODO: add column desc
    val tableDesc = TableDesc("table", "DataSourceV2RelationTableExtractor")
    TableCommandSpec(cmd, Seq(tableDesc), "ALTERTABLE_RENAMEPART")
  }

  val TruncatePartition = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.TruncatePartition"
    // TODO: add column desc
    val tableDesc = TableDesc("table", "DataSourceV2RelationTableExtractor")
    TableCommandSpec(cmd, Seq(tableDesc), "ALTERTABLE_DROPPARTS")
  }

  val CacheTableAsSelect = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.CacheTableAsSelect"
    TableCommandSpec(cmd, Nil, "CREATEVIEW", queryDescs = Seq(QueryDesc("plan")))
  }

  val CacheTable = {
    val cmd = "org.apache.spark.sql.execution.command.CacheTableCommand"
    val queryDesc = QueryDesc("plan", "LogicalPlanOptionQueryExtractor")
    TableCommandSpec(cmd, Nil, "CREATEVIEW", queryDescs = Seq(queryDesc))
  }

  val CacheTableV2 = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.CacheTable"
    TableCommandSpec(cmd, Nil, "CREATEVIEW", Seq(QueryDesc("table")))
  }

  val CreateView = {
    val cmd = "org.apache.spark.sql.execution.command.CreateViewCommand"
    val tableTypeDesc = TableTypeDesc(
      "viewType",
      "ViewTypeTableTypeExtractor",
      Seq("TEMP_VIEW", "GLOBAL_TEMP_VIEW"))
    val tableDesc = TableDesc(
      "name",
      "TableIdentifierTableExtractor",
      tableTypeDesc = Some(tableTypeDesc))
    val queryDesc1 = QueryDesc("plan")
    val queryDesc2 = QueryDesc("child")
    TableCommandSpec(cmd, Seq(tableDesc), "CREATEVIEW", queryDescs = Seq(queryDesc1, queryDesc2))
  }

  val CreateTempViewUsing = {
    val cmd = "org.apache.spark.sql.execution.datasources.CreateTempViewUsing"
    TableCommandSpec(cmd, Nil, "CREATEVIEW")
  }

  val CreateDataSourceTable = {
    val cmd = "org.apache.spark.sql.execution.command.CreateDataSourceTableCommand"
    val tableDesc = TableDesc("table", "CatalogTableTableExtractor")
    TableCommandSpec(cmd, Seq(tableDesc), "CREATETABLE")
  }

  val CreateDataSourceTableAsSelect = {
    val cmd = "org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand"
    CreateDataSourceTable.copy(
      classname = cmd,
      opType = "CREATETABLE_AS_SELECT",
      queryDescs = Seq(QueryDesc("query")))
  }

  val CreateHiveTableAsSelect = {
    val cmd = "org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand"
    val columnDesc = ColumnDesc("outputColumnNames", "StringSeqColumnExtractor")
    val tableDesc = TableDesc("tableDesc", "CatalogTableTableExtractor", Some(columnDesc))
    val queryDesc = QueryDesc("query")
    TableCommandSpec(cmd, Seq(tableDesc), "CREATETABLE_AS_SELECT", queryDescs = Seq(queryDesc))
  }

  val CreateTableLike = {
    val cmd = "org.apache.spark.sql.execution.command.CreateTableLikeCommand"
    val tableDesc1 = TableDesc(
      "targetTable",
      "TableIdentifierTableExtractor",
      setCurrentDatabaseIfMissing = true)
    val tableDesc2 = TableDesc(
      "sourceTable",
      "TableIdentifierTableExtractor",
      isInput = true,
      setCurrentDatabaseIfMissing = true)
    TableCommandSpec(cmd, Seq(tableDesc1, tableDesc2), "CREATETABLE")
  }

  val DescribeColumn = {
    val cmd = "org.apache.spark.sql.execution.command.DescribeColumnCommand"
    val columnDesc = ColumnDesc("colNameParts", "StringSeqLastColumnExtractor")
    val tableDesc = TableDesc(
      "table",
      "TableIdentifierTableExtractor",
      Some(columnDesc),
      isInput = true)
    TableCommandSpec(cmd, Seq(tableDesc), "DESCTABLE")
  }

  val DescribeTable = {
    val cmd = "org.apache.spark.sql.execution.command.DescribeTableCommand"
    val columnDesc = ColumnDesc("partitionSpec", "PartitionColumnExtractor")
    val tableDesc = TableDesc(
      "table",
      "TableIdentifierTableExtractor",
      Some(columnDesc),
      isInput = true,
      setCurrentDatabaseIfMissing = true)
    TableCommandSpec(cmd, Seq(tableDesc), "DESCTABLE")
  }

  val DropTable = {
    val cmd = "org.apache.spark.sql.execution.command.DropTableCommand"
    val tableTypeDesc =
      TableTypeDesc("tableName", "TableIdentifierTableTypeExtractor", Seq("TEMP_VIEW"))
    TableCommandSpec(
      cmd,
      Seq(tableNameDesc.copy(tableTypeDesc = Some(tableTypeDesc))),
      "DROPTABLE")
  }

  val DropTableV2 = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.DropTable"
    val tableDesc1 = resolvedTableDesc
    TableCommandSpec(cmd, Seq(tableDesc1), "DROPTABLE")
  }

  val MergeIntoTable = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable"
    val actionTypeDesc = ActionTypeDesc(null, null, Some("UPDATE"))
    val tableDesc = TableDesc(
      "targetTable",
      "DataSourceV2RelationTableExtractor",
      actionTypeDesc = Some(actionTypeDesc))
    val queryDesc = QueryDesc("sourceTable")
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(queryDesc))
  }

  val ShowColumns = {
    val cmd = "org.apache.spark.sql.execution.command.ShowColumnsCommand"
    val tableDesc = tableNameDesc.copy(isInput = true)
    TableCommandSpec(cmd, Seq(tableDesc), "SHOWCOLUMNS")
  }

  val ShowCreateTable = {
    val cmd = "org.apache.spark.sql.execution.command.ShowCreateTableCommand"
    val tableDesc = tableNameDesc.copy(fieldName = "table", isInput = true)
    TableCommandSpec(cmd, Seq(tableDesc), "SHOW_CREATETABLE")
  }

  val ShowTableProperties = {
    val cmd = "org.apache.spark.sql.execution.command.ShowTablePropertiesCommand"
    val tableDesc = tableNameDesc.copy(fieldName = "table", isInput = true)
    TableCommandSpec(cmd, Seq(tableDesc), "SHOW_TBLPROPERTIES")
  }

  val ShowCreateTableV2 = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.ShowCreateTable"
    val tableDesc = TableDesc("child", "ResolvedTableTableExtractor", isInput = true)
    TableCommandSpec(cmd, Seq(tableDesc), "SHOW_CREATETABLE")
  }

  val ShowTablePropertiesV2 = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.ShowTableProperties"
    val tableDesc = TableDesc("table", "ResolvedTableTableExtractor", isInput = true)
    TableCommandSpec(cmd, Seq(tableDesc), "SHOW_TBLPROPERTIES")
  }

  val ShowPartitions = {
    val cmd = "org.apache.spark.sql.execution.command.ShowPartitionsCommand"
    val columnDesc = ColumnDesc("spec", "PartitionOptionColumnExtractor")
    val tableDesc = tableNameDesc.copy(isInput = true, columnDesc = Some(columnDesc))
    TableCommandSpec(cmd, Seq(tableDesc), "SHOWPARTITIONS")
  }

  val TruncateTable = {
    val cmd = "org.apache.spark.sql.execution.command.TruncateTableCommand"
    val columnDesc = ColumnDesc("partitionSpec", "PartitionOptionColumnExtractor")
    val tableDesc = tableNameDesc.copy(columnDesc = Some(columnDesc))
    TableCommandSpec(cmd, Seq(tableDesc), "TRUNCATETABLE")
  }

  val TruncateTableV2 = {
    val cmd = "org.apache.spark.sql.catalyst.plans.logical.TruncateTable"
    TableCommandSpec(cmd, Seq(resolvedTableDesc), "TRUNCATETABLE")
  }

  val InsertIntoDataSource = {
    val cmd = "org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand"
    val actionTypeDesc = overwriteActionTypeDesc
    val tableDesc = TableDesc(
      "logicalRelation",
      "LogicalRelationTableExtractor",
      actionTypeDesc = Some(actionTypeDesc))
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(QueryDesc("query")))
  }

  val InsertIntoHiveTable = {
    val cmd = "org.apache.spark.sql.hive.execution.InsertIntoHiveTable"
    val actionTypeDesc = overwriteActionTypeDesc
    val columnDesc = ColumnDesc("outputColumnNames", "StringSeqColumnExtractor")
    val tableDesc = TableDesc(
      "table",
      "CatalogTableTableExtractor",
      Some(columnDesc),
      Some(actionTypeDesc))
    val queryDesc = QueryDesc("query")
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(queryDesc))
  }

  val InsertIntoDataSourceDir = {
    val cmd = "org.apache.spark.sql.execution.command.InsertIntoDataSourceDirCommand"
    val queryDesc = QueryDesc("query")
    TableCommandSpec(cmd, Nil, queryDescs = Seq(queryDesc))
  }

  val LoadData = {
    val cmd = "org.apache.spark.sql.execution.command.LoadDataCommand"
    val actionTypeDesc = overwriteActionTypeDesc.copy(fieldName = "isOverwrite")
    val columnDesc = ColumnDesc("partition", "PartitionOptionColumnExtractor")
    val tableDesc = tableIdentDesc.copy(
      fieldName = "table",
      columnDesc = Some(columnDesc),
      actionTypeDesc = Some(actionTypeDesc))
    TableCommandSpec(cmd, Seq(tableDesc), "LOAD")
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

  val data = Array(
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
    AnalyzeTable.copy(classname =
      "org.apache.spark.sql.execution.command.AnalyzeTablesCommand"),
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
    DropTable,
    DropTableV2,
    InsertIntoDataSource,
    InsertIntoDataSourceDir,
    InsertIntoDataSourceDir.copy(classname =
      "org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand"),
    // TODO: InsertIntoHadoopFsRelationCommand may have a CatalogTable
    InsertIntoDataSourceDir.copy(classname =
      "org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand"),
    InsertIntoDataSourceDir.copy(classname =
      "org.apache.spark.sql.execution.datasources.InsertIntoHiveDirCommand"),
    InsertIntoHiveTable,
    LoadData,
    MergeIntoTable,
    OverwriteByExpression,
    OverwritePartitionsDynamic,
    RepairTable,
    RefreshTable,
    RefreshTableV2,
    RefreshTable3d0,
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
