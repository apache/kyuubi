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

package org.apache.spark.sql.kyuubi.command

import scala.collection.JavaConverters._

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.{ParseTree, TerminalNode}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.kyuubi.command.KyuubiCommandsParser.{ConstantContext, ExecContext, ExpressionContext, NamedArgumentContext, PositionalArgumentContext, SingleStatementContext}

class KyuubiCommandsSqlAstBuilder(delegate: ParserInterface)
  extends KyuubiCommandsBaseVisitor[AnyRef] {

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = {
    visit(ctx.statement()).asInstanceOf[LogicalPlan]
  }

  override def visitExec(ctx: ExecContext): ExecStatement = withOrigin(ctx) {
    val name = ctx.identifier.getText
    val args = ctx.execArgument.asScala.map(typedVisit[ExecArgument])
    ExecStatement(name, args)
  }

  /**
   * Create a positional argument in a stored procedure call.
   */
  override def visitPositionalArgument(ctx: PositionalArgumentContext): ExecArgument =
    withOrigin(ctx) {
      val expr = typedVisit[Expression](ctx.expression)
      PositionalArgument(expr)
    }

  override def visitNamedArgument(ctx: NamedArgumentContext): ExecArgument = withOrigin(ctx) {
    val name = ctx.identifier().getText
    val expr = typedVisit[Expression](ctx.expression())
    NamedArgument(name, expr)
  }

  def visitConstant(ctx: ConstantContext): Literal = {
    delegate.parseExpression(ctx.getText).asInstanceOf[Literal]
  }

  override def visitExpression(ctx: ExpressionContext): Expression = {
    // reconstruct the SQL string and parse it using the main Spark parser
    // while we can avoid the logic to build Spark expressions, we still have to parse them
    // we cannot call ctx.getText directly since it will not render spaces correctly
    // that's why we need to recurse down the tree in reconstructSqlString
    val sqlString = reconstructSqlString(ctx)
    delegate.parseExpression(sqlString)
  }

  private def reconstructSqlString(ctx: ParserRuleContext): String = {
    ctx.children.asScala.map {
      case c: ParserRuleContext => reconstructSqlString(c)
      case t: TerminalNode => t.getText
    }.mkString(" ")
  }

  private def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }
}
