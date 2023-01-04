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

import org.apache.kyuubi.sql.KyuubiTrinoFeBaseParser._
import org.apache.kyuubi.sql.KyuubiTrinoFeBaseParserBaseVisitor
import org.apache.kyuubi.sql.parser.KyuubiParser
import org.apache.kyuubi.sql.plan.{KyuubiTreeNode, PassThroughNode}
import org.apache.kyuubi.sql.plan.trino.{GetCatalogs, GetSchemas, GetTableTypes, GetTypeInfo}

class KyuubiTrinoFeAstBuilder extends KyuubiTrinoFeBaseParserBaseVisitor[AnyRef] {

  override def visitSingleStatement(
      ctx: SingleStatementContext): KyuubiTreeNode = {
    visit(ctx.statement).asInstanceOf[KyuubiTreeNode]
  }

  override def visitPassThrough(ctx: PassThroughContext): KyuubiTreeNode = {
    PassThroughNode()
  }

  override def visitGetSchemas(ctx: GetSchemasContext): KyuubiTreeNode = {
    val catalog = if (ctx.catalog == null) {
      null
    } else {
      KyuubiParser.unescapeSQLString(ctx.catalog.getText)
    }
    val schema = if (ctx.schema == null) {
      null
    } else {
      KyuubiParser.unescapeSQLString(ctx.schema.getText)
    }

    GetSchemas(catalog, schema)
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
}
