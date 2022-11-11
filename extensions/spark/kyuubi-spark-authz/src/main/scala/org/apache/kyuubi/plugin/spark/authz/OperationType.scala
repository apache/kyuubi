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

import org.apache.kyuubi.plugin.spark.authz.util.ArcticCmdUtils

object OperationType extends Enumeration {

  type OperationType = Value

  val ALTERDATABASE, ALTERDATABASE_LOCATION, ALTERTABLE_ADDCOLS, ALTERTABLE_ADDPARTS,
      ALTERTABLE_RENAMECOL, ALTERTABLE_REPLACECOLS, ALTERTABLE_DROPPARTS, ALTERTABLE_RENAMEPART,
      ALTERTABLE_RENAME, ALTERTABLE_PROPERTIES, ALTERTABLE_SERDEPROPERTIES, ALTERTABLE_LOCATION,
      ALTERVIEW_AS, ALTERVIEW_RENAME, ANALYZE_TABLE, CREATEDATABASE, CREATETABLE,
      CREATETABLE_AS_SELECT, CREATEFUNCTION, CREATEVIEW, DESCDATABASE, DESCFUNCTION, DESCTABLE,
      DROPDATABASE, DROPFUNCTION, DROPTABLE, DROPVIEW, EXPLAIN, LOAD, MSCK, QUERY, RELOADFUNCTION,
      SHOWCONF, SHOW_CREATETABLE, SHOWCOLUMNS, SHOWDATABASES, SHOWFUNCTIONS, SHOWPARTITIONS,
      SHOWTABLES, SHOW_TBLPROPERTIES, SWITCHDATABASE, TRUNCATETABLE = Value

  /**
   * Mapping Spark plan's nodeName to operation type
   * @param clzName nodeName
   * @return
   */
  def apply(clzName: String): OperationType = {
    clzName match {
      case v2Cmd if v2Commands.accept(v2Cmd) =>
        v2Commands.withName(v2Cmd).operationType
      case arcticCmd if ArcticCmdUtils.accept(arcticCmd) =>
        ArcticCmdUtils.operationType(arcticCmd)

      case "AddArchivesCommand" => EXPLAIN
      case "AddFilesCommand" => EXPLAIN
      case "AddJarsCommand" => EXPLAIN
      case "AlterDatabasePropertiesCommand" |
          "SetNamespaceProperties" => ALTERDATABASE
      case "AlterDatabaseSetLocationCommand" |
          "SetNamespaceLocation" => ALTERDATABASE_LOCATION
      case "AlterTableAddColumnsCommand" |
          "AlterHoodieTableAddColumnsCommand" => ALTERTABLE_ADDCOLS
      case "AlterTableAddPartitionCommand" => ALTERTABLE_ADDPARTS
      case "AlterTableChangeColumnCommand" => ALTERTABLE_REPLACECOLS
      case "AlterTableDropPartitionCommand" => ALTERTABLE_DROPPARTS
      case "AlterTableRenameCommand" => ALTERTABLE_RENAME
      case "AlterTableRecoverPartitionsCommand" |
          "RepairTableCommand" => MSCK
      case "AlterTableRenamePartitionCommand" => ALTERTABLE_RENAMEPART
      case "AlterTableSerDePropertiesCommand" => ALTERTABLE_SERDEPROPERTIES
      case "AlterTableSetLocationCommand" => ALTERTABLE_LOCATION
      case "AlterTableSetPropertiesCommand" |
          "AlterTableUnsetPropertiesCommand" => ALTERTABLE_PROPERTIES
      case ava if ava.contains("AlterViewAs") => ALTERVIEW_AS
      case ac if ac.startsWith("Analyze") => ANALYZE_TABLE
      case "CreateDatabaseCommand" => CREATEDATABASE
      case "CreateFunctionCommand" | "CreateFunction" => CREATEFUNCTION
      case "CreateDataSourceTableAsSelectCommand" |
          "CreateHiveTableAsSelectCommand" |
          "OptimizedCreateHiveTableAsSelectCommand" => CREATETABLE_AS_SELECT
      case "CreateTableCommand" |
          "CreateDataSourceTableCommand" |
          "CreateTableLikeCommand" => CREATETABLE
      case "CreateViewCommand" |
          "CacheTableCommand" |
          "CreateTempViewUsing" => CREATEVIEW
      case "DescribeDatabaseCommand" | "DescribeNamespace" => DESCDATABASE
      case "DescribeFunctionCommand" => DESCFUNCTION
      case "DescribeColumnCommand" | "DescribeTableCommand" => DESCTABLE
      case "DropDatabaseCommand" | "DropNamespace" => DROPDATABASE
      case "DropFunctionCommand" | "DropFunction" => DROPFUNCTION
      case "DropTableCommand" => DROPTABLE
      case "ExplainCommand" => EXPLAIN
      case "InsertIntoDataSourceCommand" |
          "InsertIntoDataSourceDirCommand" |
          "InsertIntoHiveTable" |
          "InsertIntoHiveDirCommand" |
          "SaveIntoDataSourceCommand" => QUERY
      case "LoadDataCommand" => LOAD
      case "SetCommand" => SHOWCONF
      case "RefreshFunctionCommand" | "RefreshFunction" => RELOADFUNCTION
      case "RefreshTableCommand" | "RefreshTable" => QUERY
      case "SetCatalogCommand" |
          "SetCatalogAndNamespace" |
          "SetNamespaceCommand" |
          "SetDatabaseCommand" => SWITCHDATABASE
      case "ShowCatalogsCommand" |
          "ShowCurrentNamespaceCommand" => SHOWDATABASES
      case "ShowTablesCommand" |
          "ShowViewsCommand" |
          "ShowTables" => SHOWTABLES
      case "ShowColumnsCommand" => SHOWCOLUMNS
      case "ShowCreateTableAsSerdeCommand" |
          "ShowCreateTableCommand" |
          "ShowCreateTable" => SHOW_CREATETABLE
      case "ShowFunctionsCommand" => SHOWFUNCTIONS
      case "ShowPartitionsCommand" => SHOWPARTITIONS
      case "ShowTablePropertiesCommand" |
          "ShowTableProperties" => SHOW_TBLPROPERTIES
      case "TruncateTableCommand" => TRUNCATETABLE
      case "UncacheTableCommand" => DROPVIEW
      case _ => QUERY
    }
  }

}
