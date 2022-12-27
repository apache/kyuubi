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

package org.apache.kyuubi.sql.trino

import org.apache.kyuubi.sql.{KyuubiTrinoBaseParser, KyuubiTrinoBaseParserBaseVisitor}
import org.apache.kyuubi.sql.parser.KyuubiParser
import org.apache.kyuubi.sql.plan.KyuubiTreeNode

class TrinoStatementAstBuilder extends KyuubiTrinoBaseParserBaseVisitor[KyuubiTreeNode] {

  override def visitSingleStatement(
      ctx: KyuubiTrinoBaseParser.SingleStatementContext): KyuubiTreeNode = {
    visit(ctx.statement)
  }

  override def visitPassThrough(ctx: KyuubiTrinoBaseParser.PassThroughContext): KyuubiTreeNode = {
    PassThrough()
  }

  override def visitGetSchemas(ctx: KyuubiTrinoBaseParser.GetSchemasContext): KyuubiTreeNode = {
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

    TrinoGetSchemas(catalog, schema)
  }
}
