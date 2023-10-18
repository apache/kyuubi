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
import org.apache.kyuubi.plugin.spark.authz.serde._
import org.apache.kyuubi.plugin.spark.authz.serde.TableType._

object HudiCommands {
  val AlterHoodieTableAddColumnsCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.AlterHoodieTableAddColumnsCommand"
    val columnDesc = ColumnDesc("colsToAdd", classOf[StructFieldSeqColumnExtractor])
    val tableDesc = TableDesc("tableId", classOf[TableIdentifierTableExtractor], Some(columnDesc))
    TableCommandSpec(cmd, Seq(tableDesc), ALTERTABLE_ADDCOLS)
  }

  val AlterHoodieTableChangeColumnCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.AlterHoodieTableChangeColumnCommand"
    val columnDesc = ColumnDesc("columnName", classOf[StringColumnExtractor])
    val tableDesc =
      TableDesc("tableIdentifier", classOf[TableIdentifierTableExtractor], Some(columnDesc))
    TableCommandSpec(cmd, Seq(tableDesc), ALTERTABLE_REPLACECOLS)
  }

  val AlterHoodieTableDropPartitionCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.AlterHoodieTableDropPartitionCommand"
    val columnDesc = ColumnDesc("partitionSpecs", classOf[PartitionSeqColumnExtractor])
    val tableDesc =
      TableDesc("tableIdentifier", classOf[TableIdentifierTableExtractor], Some(columnDesc))
    TableCommandSpec(cmd, Seq(tableDesc), ALTERTABLE_DROPPARTS)
  }

  val AlterHoodieTableRenameCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.AlterHoodieTableRenameCommand"
    val oldTableTableTypeDesc =
      TableTypeDesc(
        "oldName",
        classOf[TableIdentifierTableTypeExtractor],
        Seq(TEMP_VIEW))
    val oldTableD = TableDesc(
      "oldName",
      classOf[TableIdentifierTableExtractor],
      tableTypeDesc = Some(oldTableTableTypeDesc))

    TableCommandSpec(cmd, Seq(oldTableD), ALTERTABLE_RENAME)
  }

  val AlterTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.AlterTableCommand"
    val tableDesc = TableDesc("table", classOf[CatalogTableTableExtractor], None)
    TableCommandSpec(cmd, Seq(tableDesc), ALTERTABLE_PROPERTIES)
  }

  val Spark31AlterTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.Spark31AlterTableCommand"
    val tableDesc = TableDesc("table", classOf[CatalogTableTableExtractor], None)
    TableCommandSpec(cmd, Seq(tableDesc), ALTERTABLE_PROPERTIES)
  }

  val CreateHoodieTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.CreateHoodieTableCommand"
    val tableDesc = TableDesc("table", classOf[CatalogTableTableExtractor])
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
      setCurrentDatabaseIfMissing = true)
    val tableDesc2 = TableDesc(
      "sourceTable",
      classOf[TableIdentifierTableExtractor],
      isInput = true,
      setCurrentDatabaseIfMissing = true)
    TableCommandSpec(cmd, Seq(tableDesc1, tableDesc2), CREATETABLE)
  }

  val DropHoodieTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.DropHoodieTableCommand"
    val tableTypeDesc =
      TableTypeDesc(
        "tableIdentifier",
        classOf[TableIdentifierTableTypeExtractor],
        Seq(TEMP_VIEW))
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
    val columnDesc = ColumnDesc("partitionSpec", classOf[PartitionOptionColumnExtractor])
    val tableDesc =
      TableDesc(
        "tableIdentifier",
        classOf[TableIdentifierTableExtractor],
        columnDesc = Some(columnDesc))
    TableCommandSpec(cmd, Seq(tableDesc), TRUNCATETABLE)
  }

  val CompactionHoodieTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.CompactionHoodieTableCommand"
    val tableDesc = TableDesc("table", classOf[CatalogTableTableExtractor])
    TableCommandSpec(cmd, Seq(tableDesc, tableDesc.copy(isInput = true)), CREATETABLE)
  }

  val CompactionShowHoodieTableCommand = {
    val cmd = "org.apache.spark.sql.hudi.command.CompactionShowHoodieTableCommand"
    val tableDesc = TableDesc("table", classOf[CatalogTableTableExtractor], isInput = true)
    TableCommandSpec(cmd, Seq(tableDesc), SHOW_TBLPROPERTIES)
  }

  val data: Array[TableCommandSpec] = Array(
    AlterHoodieTableAddColumnsCommand,
    AlterHoodieTableChangeColumnCommand,
    AlterHoodieTableDropPartitionCommand,
    AlterHoodieTableRenameCommand,
    AlterTableCommand,
    CreateHoodieTableAsSelectCommand,
    CreateHoodieTableCommand,
    CreateHoodieTableLikeCommand,
    CompactionHoodieTableCommand,
    CompactionShowHoodieTableCommand,
    DropHoodieTableCommand,
    RepairHoodieTableCommand,
    TruncateHoodieTableCommand,
    Spark31AlterTableCommand)
}
