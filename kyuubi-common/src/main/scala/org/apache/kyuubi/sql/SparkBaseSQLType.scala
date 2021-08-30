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

package org.apache.kyuubi.sql

/**
 * A classification of SQL_TYPE based the Spark_v3.1.2
 * Now there have 4 different types: DDL, DML, DQL and OTHERS.
 */
object SparkBaseSQLType {

  private val DDLSet = Set("AlterNamespaceSetProperties", "AlterNamespaceSetLocation",
    "RenameTableStatement", "AlterTableRenamePartitionStatement", "AlterTableAddColumnsStatement",
    "AlterTableAlterColumnStatement", "AlterTableAddPartition", "AlterTableDropPartition",
    "AlterTableSetPropertiesStatement", "AlterTableSetLocationStatement",
    "AlterTableSerDePropertiesStatement", "AlterTableUnsetPropertiesStatement",
    "AlterTableRenameColumnStatement", "AlterTableDropColumnsStatement",
    "AlterTableRecoverPartitionsStatement", "RenameTableStatement",
    "AlterViewSetPropertiesStatement", "AlterViewUnsetPropertiesStatement",
    "AlterViewAsStatement", "CreateNamespaceStatement", "CreateNamespace",
    "CreateFunctionStatement", "CreateTableStatement", "CreateV2Table",
    "CreateTableAsSelectStatement", "CreateTableAsSelect", "CreateTableLikeCommand",
    "CreateViewStatement", "DropNamespace", "DropFunction", "DropTable", "DropViewStatement",
    "TruncateTable", "RepairTableStatement", "UseStatement")

  def getSQLType(name: String): String = {
    if (DDLSet.contains(name)) {
      return "DDL"
    }
    return "OTHERS"
  }

}
