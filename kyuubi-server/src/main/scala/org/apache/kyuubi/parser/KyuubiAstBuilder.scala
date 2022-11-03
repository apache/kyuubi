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

package org.apache.kyuubi.parser

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.kyuubi.parser.node.{LogicalNode, PassThroughNode}
import org.apache.kyuubi.parser.node.runnable.{AlterEngineConfNode, KillEngineNode, LaunchEngineNode}
import org.apache.kyuubi.parser.node.translate.RenameTableNode
import org.apache.kyuubi.sql.{KyuubiSqlBaseParser, KyuubiSqlBaseParserBaseVisitor}
import org.apache.kyuubi.sql.KyuubiSqlBaseParser.{PropertyKeyContext, PropertyListContext, PropertyValueContext, SingleStatementContext, StringLitContext}

class KyuubiAstBuilder extends KyuubiSqlBaseParserBaseVisitor[AnyRef] {

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalNode = {
    visit(ctx.statement).asInstanceOf[LogicalNode]
  }

  override def visitPassThrough(ctx: KyuubiSqlBaseParser.PassThroughContext): LogicalNode = {
    PassThroughNode()
  }

  /* ------------------------------ *
   |  Visit Runnable-related nodes  |
   * ------------------------------ */

  override def visitCreareEngine(ctx: KyuubiSqlBaseParser.CreareEngineContext): AnyRef = {
    LaunchEngineNode()
  }

  override def visitDropEngine(ctx: KyuubiSqlBaseParser.DropEngineContext): AnyRef = {
    KillEngineNode()
  }

  override def visitAlterEngineConfig(ctx: KyuubiSqlBaseParser.AlterEngineConfigContext)
      : LogicalNode = {
    val conf = visitPropertyList(ctx.propertyList())
    AlterEngineConfNode(conf)
  }

  /* ------------------------------ *
   |  Visit Translate-related nodes  |
   * ------------------------------ */

  override def visitRenameTable(ctx: KyuubiSqlBaseParser.RenameTableContext): LogicalNode = {
    val from = visitIdentifier(ctx.identifier(0)).toString
    val to = visitIdentifier(ctx.identifier(1)).toString
    RenameTableNode(from, to)
  }

  override def visitPropertyList(
      ctx: PropertyListContext): Map[String, String] = {
    val properties = ctx.property.asScala.map { property =>
      val key = visitPropertyKey(property.key)
      val value = visitPropertyValue(property.value)
      key -> value
    }
    properties.toMap
  }

  override def visitPropertyKey(key: PropertyKeyContext): String = {
    if (key.stringLit() != null) {
      visitStringLit(key.stringLit())
    } else {
      key.getText
    }
  }

  override def visitPropertyValue(value: PropertyValueContext): String = {
    if (value == null) {
      null
    } else if (value.stringLit() != null) {
      visitStringLit(value.stringLit())
    } else if (value.booleanValue != null) {
      value.getText.toLowerCase(Locale.ROOT)
    } else {
      value.getText
    }
  }

  override def visitStringLit(ctx: StringLitContext): String = {
    val length = ctx.STRING.getSymbol.getText.length
    ctx.STRING.getSymbol.getText.substring(1, length - 1)
  }

  override def visitUnquotedIdentifier(ctx: KyuubiSqlBaseParser.UnquotedIdentifierContext)
      : String = {
    ctx.getText
  }

  override def visitQuotedIdentifierAlternative(
      ctx: KyuubiSqlBaseParser.QuotedIdentifierAlternativeContext): String = {
    val length = ctx.getText.length
    ctx.getText.substring(1, length - 1)
  }
}
