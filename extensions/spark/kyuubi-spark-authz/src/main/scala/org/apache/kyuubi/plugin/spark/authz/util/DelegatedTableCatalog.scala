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

package org.apache.kyuubi.plugin.spark.authz.util

import java.util

import org.apache.spark.sql.connector.catalog.{Identifier, StagedTable, StagingTableCatalog, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DelegatedTableCatalog(delegate: TableCatalog) extends TableCatalog {
  override def listTables(namespace: Array[String]): Array[Identifier] =
    delegate.listTables(namespace)

  override def loadTable(ident: Identifier): Table =
    delegate.loadTable(ident)

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table =
    delegate.createTable(ident, schema, partitions, properties)

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    delegate.alterTable(ident, changes: _*)

  override def dropTable(ident: Identifier): Boolean =
    delegate.dropTable(ident)

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    delegate.renameTable(oldIdent, newIdent)

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit =
    delegate.initialize(name, options)

  override def name(): String =
    delegate.name()
}

class DelegatedStagingTableCatalog(delegate: StagingTableCatalog)
  extends DelegatedTableCatalog(delegate) with StagingTableCatalog {
  override def stageCreate(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    delegate.stageCreate(ident, schema, partitions, properties)

  override def stageReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    delegate.stageReplace(ident, schema, partitions, properties)

  override def stageCreateOrReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): StagedTable =
    delegate.stageCreateOrReplace(ident, schema, partitions, properties)
}
