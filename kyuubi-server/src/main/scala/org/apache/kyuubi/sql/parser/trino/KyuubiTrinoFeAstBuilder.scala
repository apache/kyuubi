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

package org.apache.kyuubi.sql.parser.trino

import scala.collection.JavaConverters._

import org.antlr.v4.runtime.tree.ParseTree

import org.apache.kyuubi.sql.KyuubiTrinoFeBaseParser._
import org.apache.kyuubi.sql.KyuubiTrinoFeBaseParserBaseVisitor
import org.apache.kyuubi.sql.parser.KyuubiParser.unescapeSQLString
import org.apache.kyuubi.sql.plan.{KyuubiTreeNode, PassThroughNode}
import org.apache.kyuubi.sql.plan.trino.{GetCatalogs, GetColumns, GetPrimaryKeys, GetSchemas, GetTables, GetTableTypes, GetTypeInfo}

class KyuubiTrinoFeAstBuilder extends KyuubiTrinoFeBaseParserBaseVisitor[AnyRef] {

  override def visit(tree: ParseTree): AnyRef = {
    Option(tree) match {
      case Some(_) => super.visit(tree)
      case _ => null
    }

  }
  override def visitSingleStatement(
      ctx: SingleStatementContext): KyuubiTreeNode = {
    visit(ctx.statement).asInstanceOf[KyuubiTreeNode]
  }

  override def visitPassThrough(ctx: PassThroughContext): KyuubiTreeNode = {
    PassThroughNode()
  }

  override def visitGetSchemas(ctx: GetSchemasContext): KyuubiTreeNode = {
    val catalog = visit(ctx.tableCatalogFilter()).asInstanceOf[String]
    val schemaPattern = visit(ctx.tableSchemaFilter()).asInstanceOf[String]

    GetSchemas(catalog, schemaPattern)
  }

  override def visitGetCatalogs(ctx: GetCatalogsContext): KyuubiTreeNode = {
    GetCatalogs()
  }

  override def visitGetTableTypes(ctx: GetTableTypesContext): KyuubiTreeNode = {
    GetTableTypes()
  }

  override def visitGetTypeInfo(ctx: GetTypeInfoContext): KyuubiTreeNode = {
    GetTypeInfo()
  }

  override def visitGetTables(ctx: GetTablesContext): KyuubiTreeNode = {
    val catalog = visit(ctx.tableCatalogFilter()).asInstanceOf[String]
    val schemaPattern = visit(ctx.tableSchemaFilter()).asInstanceOf[String]
    val tableNamePattern = visit(ctx.tableNameFilter()).asInstanceOf[String]

    var emptyResult = false
    var tableTypes: List[String] = null

    ctx.tableTypeFilter() match {
      case _: TableTypesAlwaysFalseContext =>
        emptyResult = true
      case typesFilter: TypesFilterContext =>
        tableTypes = visitTypesFilter(typesFilter)
      case _ => // ctx.tableTypeFilter is null.
    }

    GetTables(catalog, schemaPattern, tableNamePattern, tableTypes, emptyResult)
  }

  override def visitGetColumns(ctx: GetColumnsContext): KyuubiTreeNode = {
    val catalog = visit(ctx.tableCatalogFilter()).asInstanceOf[String]
    val schemaPattern = visit(ctx.tableSchemaFilter()).asInstanceOf[String]
    val tableNamePattern = visit(ctx.tableNameFilter()).asInstanceOf[String]
    val colNamePattern = visit(ctx.colNameFilter()).asInstanceOf[String]

    GetColumns(catalog, schemaPattern, tableNamePattern, colNamePattern)
  }

  override def visitGetPrimaryKeys(ctx: GetPrimaryKeysContext): KyuubiTreeNode = {
    GetPrimaryKeys()
  }

  override def visitNullCatalog(ctx: NullCatalogContext): AnyRef = {
    null
  }

  override def visitCatalogFilter(ctx: CatalogFilterContext): String = {
    unescapeSQLString(ctx.catalog.getText)
  }

  override def visitNulTableSchema(ctx: NulTableSchemaContext): AnyRef = {
    null
  }

  override def visitSchemaFilter(ctx: SchemaFilterContext): String = {
    unescapeSQLString(ctx.schemaPattern.getText)
  }

  override def visitTableNameFilter(ctx: TableNameFilterContext): String = {
    unescapeSQLString(ctx.tableNamePattern.getText)
  }

  override def visitColNameFilter(ctx: ColNameFilterContext): String = {
    unescapeSQLString(ctx.colNamePattern.getText)
  }

  override def visitTypesFilter(ctx: TypesFilterContext): List[String] = {
    ctx.stringLit().asScala.map(v => unescapeSQLString(v.getText)).toList
  }
}
