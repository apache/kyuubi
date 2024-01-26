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

object HudiCommands extends CommandSpecs[TableCommandSpec] {
  val AlterHoodieTableAddColumnsCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.AlterHoodieTableAddColumnsCommand"
    val columnDesc =
      ColumnDesc("colsToAdd", classOf[StructFieldSeqColumnExtractor], comment = "Hudi")
    val tableDesc = TableDesc(
      "tableId",
      classOf[TableIdentifierTableExtractor],
      Some(columnDesc),
      comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc), ALTERTABLE_ADDCOLS)
  }

  val AlterHoodieTableChangeColumnCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.AlterHoodieTableChangeColumnCommand"
    val columnDesc = ColumnDesc("columnName", classOf[StringColumnExtractor], comment = "Hudi")
    val tableDesc =
      TableDesc(
        "tableIdentifier",
        classOf[TableIdentifierTableExtractor],
        Some(columnDesc),
        comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc), ALTERTABLE_REPLACECOLS)
  }

  val AlterHoodieTableDropPartitionCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.AlterHoodieTableDropPartitionCommand"
    val columnDesc =
      ColumnDesc("partitionSpecs", classOf[PartitionSeqColumnExtractor], comment = "Hudi")
    val tableDesc =
      TableDesc(
        "tableIdentifier",
        classOf[TableIdentifierTableExtractor],
        Some(columnDesc),
        comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc), ALTERTABLE_DROPPARTS)
  }

  val AlterHoodieTableRenameCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.AlterHoodieTableRenameCommand"
    val oldTableTableTypeDesc =
      TableTypeDesc(
        "oldName",
        classOf[TableIdentifierTableTypeExtractor],
        Seq(TEMP_VIEW),
        comment = "Hudi")
    val oldTableD = TableDesc(
      "oldName",
      classOf[TableIdentifierTableExtractor],
      tableTypeDesc = Some(oldTableTableTypeDesc),
      comment = "Hudi")

    TableCommandSpec(cmd, Seq(oldTableD), ALTERTABLE_RENAME)
  }

  val AlterTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.AlterTableCommand"
    val tableDesc = TableDesc("table", classOf[CatalogTableTableExtractor], None, comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc), ALTERTABLE_PROPERTIES)
  }

  val Spark31AlterTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.Spark31AlterTableCommand"
    val tableDesc = TableDesc("table", classOf[CatalogTableTableExtractor], None, comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc), ALTERTABLE_PROPERTIES)
  }

  val CreateHoodieTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.CreateHoodieTableCommand"
    val tableDesc = TableDesc("table", classOf[CatalogTableTableExtractor], comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc), CREATETABLE)
  }

  val CreateHoodieTableAsSelectCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.CreateHoodieTableAsSelectCommand"
    CreateHoodieTableCommand.copy(
      classname = cmd,
      opType = CREATETABLE_AS_SELECT,
      queryDescs = Seq(QueryDesc("query")))
  }

  val CreateHoodieTableLikeCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.CreateHoodieTableLikeCommand"
    val tableDesc1 = TableDesc(
      "targetTable",
      classOf[TableIdentifierTableExtractor],
      setCurrentDatabaseIfMissing = true,
      comment = "Hudi")
    val tableDesc2 = TableDesc(
      "sourceTable",
      classOf[TableIdentifierTableExtractor],
      isInput = true,
      setCurrentDatabaseIfMissing = true,
      comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc1, tableDesc2), CREATETABLE)
  }

  val DropHoodieTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.DropHoodieTableCommand"
    val tableTypeDesc =
      TableTypeDesc(
        "tableIdentifier",
        classOf[TableIdentifierTableTypeExtractor],
        Seq(TEMP_VIEW),
        comment = "Hudi")
    TableCommandSpec(
      cmd,
      Seq(TableDesc(
        "tableIdentifier",
        classOf[TableIdentifierTableExtractor],
        tableTypeDesc = Some(tableTypeDesc))),
      DROPTABLE)
  }

  val RepairHoodieTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.RepairHoodieTableCommand"
    TableCommandSpec(cmd, Seq(TableDesc("tableName", classOf[TableIdentifierTableExtractor])), MSCK)
  }

  val TruncateHoodieTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.TruncateHoodieTableCommand"
    val columnDesc =
      ColumnDesc("partitionSpec", classOf[PartitionOptionColumnExtractor], comment = "Hudi")
    val tableDesc =
      TableDesc(
        "tableIdentifier",
        classOf[TableIdentifierTableExtractor],
        columnDesc = Some(columnDesc),
        comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc), TRUNCATETABLE)
  }

  val CompactionHoodieTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.CompactionHoodieTableCommand"
    val tableDesc = TableDesc("table", classOf[CatalogTableTableExtractor], comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc), CREATETABLE)
  }

  val CompactionShowHoodieTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.CompactionShowHoodieTableCommand"
    val tableDesc =
      TableDesc("table", classOf[CatalogTableTableExtractor], isInput = true, comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc), SHOW_TBLPROPERTIES)
  }

  val CompactionHoodiePathCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.CompactionHoodiePathCommand"
    val uriDesc = UriDesc("path", classOf[StringURIExtractor], comment = "Hudi")
    TableCommandSpec(
      cmd,
      Seq.empty,
      CREATETABLE,
      uriDescs = Seq(uriDesc))
  }

  val CompactionShowHoodiePathCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.CompactionShowHoodiePathCommand"
    val uriDesc = UriDesc("path", classOf[StringURIExtractor], isInput = true, comment = "Hudi")
    TableCommandSpec(cmd, Seq.empty, SHOW_TBLPROPERTIES, uriDescs = Seq(uriDesc))
  }

  val CreateIndexCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.CreateIndexCommand"
    val tableDesc = TableDesc("table", classOf[CatalogTableTableExtractor], comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc), CREATEINDEX)
  }

  val DropIndexCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.DropIndexCommand"
    val tableDesc = TableDesc("table", classOf[CatalogTableTableExtractor], comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc), DROPINDEX)
  }

  val ShowIndexCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.ShowIndexesCommand"
    val tableDesc =
      TableDesc("table", classOf[CatalogTableTableExtractor], isInput = true, comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc), SHOWINDEXES)
  }

  val RefreshIndexCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.RefreshIndexCommand"
    val tableDesc = TableDesc("table", classOf[CatalogTableTableExtractor], comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc), ALTERINDEX_REBUILD)
  }

  val InsertIntoHoodieTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.InsertIntoHoodieTableCommand"
    val tableDesc = TableDesc(
      "logicalRelation",
      classOf[LogicalRelationTableExtractor],
      actionTypeDesc =
        Some(ActionTypeDesc(
          "overwrite",
          classOf[OverwriteOrInsertActionTypeExtractor],
          comment = "Hudi")),
      comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(QueryDesc("query")))
  }

  val ShowHoodieTablePartitionsCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.ShowHoodieTablePartitionsCommand"
    val columnDesc =
      ColumnDesc("specOpt", classOf[PartitionOptionColumnExtractor], comment = "Hudi")
    val tableDesc = TableDesc(
      "tableIdentifier",
      classOf[TableIdentifierTableExtractor],
      isInput = true,
      columnDesc = Some(columnDesc),
      comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc), SHOWPARTITIONS)
  }

  val DeleteHoodieTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.DeleteHoodieTableCommand"
    val actionTypeDesc = ActionTypeDesc(actionType = Some(UPDATE), comment = "Hudi")
    val tableDesc =
      TableDesc(
        "dft",
        classOf[HudiDataSourceV2RelationTableExtractor],
        actionTypeDesc = Some(actionTypeDesc),
        comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc))
  }

  val UpdateHoodieTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.UpdateHoodieTableCommand"
    val actionTypeDesc = ActionTypeDesc(actionType = Some(UPDATE), comment = "Hudi")
    val tableDesc =
      TableDesc(
        "ut",
        classOf[HudiDataSourceV2RelationTableExtractor],
        actionTypeDesc = Some(actionTypeDesc),
        comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc))
  }

  val MergeIntoHoodieTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.MergeIntoHoodieTableCommand"
    val actionTypeDesc = ActionTypeDesc(actionType = Some(UPDATE), comment = "Hudi")
    val tableDesc =
      TableDesc(
        "mergeInto",
        classOf[HudiMergeIntoTargetTableExtractor],
        actionTypeDesc = Some(actionTypeDesc),
        comment = "Hudi")
    val queryDescs =
      QueryDesc("mergeInto", classOf[HudiMergeIntoSourceTableExtractor], comment = "Hudi")
    TableCommandSpec(cmd, Seq(tableDesc), queryDescs = Seq(queryDescs))
  }

  val CallProcedureHoodieCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.CallProcedureHoodieCommand"
    TableCommandSpec(
      cmd,
      Seq(
        TableDesc(
          "clone",
          classOf[HudiCallProcedureInputTableExtractor],
          actionTypeDesc = Some(ActionTypeDesc(actionType = Some(OTHER))),
          isInput = true,
          setCurrentDatabaseIfMissing = true),
        TableDesc(
          "clone",
          classOf[HudiCallProcedureOutputTableExtractor],
          actionTypeDesc = Some(ActionTypeDesc(actionType = Some(UPDATE))),
          setCurrentDatabaseIfMissing = true)),
      uriDescs = Seq(
        UriDesc(
          "clone",
          classOf[HudiCallProcedureInputUriExtractor],
          isInput = true,
          comment = "Hudi"),
        UriDesc("clone", classOf[HudiCallProcedureOutputUriExtractor], comment = "Hudi")))
  }

  override def specs: Seq[TableCommandSpec] = Seq(
    AlterHoodieTableAddColumnsCommand,
    AlterHoodieTableChangeColumnCommand,
    AlterHoodieTableDropPartitionCommand,
    AlterHoodieTableRenameCommand,
    AlterTableCommand,
    CallProcedureHoodieCommand,
    CreateHoodieTableAsSelectCommand,
    CreateHoodieTableCommand,
    CreateHoodieTableLikeCommand,
    CreateIndexCommand,
    CompactionHoodiePathCommand,
    CompactionHoodieTableCommand,
    CompactionShowHoodiePathCommand,
    CompactionShowHoodieTableCommand,
    DeleteHoodieTableCommand,
    DropHoodieTableCommand,
    DropIndexCommand,
    InsertIntoHoodieTableCommand,
    MergeIntoHoodieTableCommand,
    RefreshIndexCommand,
    RepairHoodieTableCommand,
    ShowIndexCommand,
    TruncateHoodieTableCommand,
    ShowHoodieTablePartitionsCommand,
    Spark31AlterTableCommand,
    UpdateHoodieTableCommand)
}
