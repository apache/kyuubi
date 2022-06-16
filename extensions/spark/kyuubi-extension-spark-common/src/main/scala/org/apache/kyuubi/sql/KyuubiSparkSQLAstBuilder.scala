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

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ListBuffer

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.ParseTree
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.kyuubi.sql.KyuubiSparkSQLParser._
import org.apache.kyuubi.sql.zorder.{OptimizeZorderStatement, OptimizeZorderStatementBase, Zorder, ZorderBase}

abstract class KyuubiSparkSQLAstBuilderBase extends KyuubiSparkSQLBaseVisitor[AnyRef] {
  def buildZorder(child: Seq[Expression]): ZorderBase
  def buildOptimizeZorderStatement(
      tableIdentifier: Seq[String],
      query: LogicalPlan): OptimizeZorderStatementBase

  /**
   * Create an expression from the given context. This method just passes the context on to the
   * visitor and only takes care of typing (We assume that the visitor returns an Expression here).
   */
  protected def expression(ctx: ParserRuleContext): Expression = typedVisit(ctx)

  protected def multiPart(ctx: ParserRuleContext): Seq[String] = typedVisit(ctx)

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = {
    visit(ctx.statement()).asInstanceOf[LogicalPlan]
  }

  override def visitOptimizeZorder(
      ctx: OptimizeZorderContext): UnparsedPredicateOptimize = withOrigin(ctx) {
    val tableIdent = multiPart(ctx.multipartIdentifier())
    val table = UnresolvedRelation(tableIdent)

    val predicate = Option(ctx.whereClause().partitionPredicate).map(extractRawText(_))

    val zorderCols = ctx.zorderClause().order.asScala
      .map(visitMultipartIdentifier)
      .map(UnresolvedAttribute(_))
      .toSeq

    val orderExpr =
      if (zorderCols.length == 1) {
        zorderCols.head
      } else {
        buildZorder(zorderCols)
      }
    UnparsedPredicateOptimize(tableIdent, table, predicate, orderExpr)
  }

  override def visitPassThrough(ctx: PassThroughContext): LogicalPlan = null

  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] =
    withOrigin(ctx) {
      ctx.parts.asScala.map(_.getText)
    }

  override def visitZorderClause(ctx: ZorderClauseContext): Seq[UnresolvedAttribute] =
    withOrigin(ctx) {
      val res = ListBuffer[UnresolvedAttribute]()
      ctx.multipartIdentifier().forEach { identifier =>
        res += UnresolvedAttribute(identifier.parts.asScala.map(_.getText))
      }
      res
    }

  private def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }

  private def extractRawText(exprContext: ParserRuleContext): String = {
    // Extract the raw expression which will be parsed later
    exprContext.getStart.getInputStream.getText(new Interval(
      exprContext.getStart.getStartIndex,
      exprContext.getStop.getStopIndex))
  }

}

class KyuubiSparkSQLAstBuilder extends KyuubiSparkSQLAstBuilderBase {
  override def buildZorder(child: Seq[Expression]): ZorderBase = {
    Zorder(child)
  }

  override def buildOptimizeZorderStatement(
      tableIdentifier: Seq[String],
      query: LogicalPlan): OptimizeZorderStatementBase = {
    OptimizeZorderStatement(tableIdentifier, query)
  }
}

trait UnparsedExpressionLogicalPlan extends LogicalPlan {
  override def output: Seq[Attribute] = throw new UnsupportedOperationException()

  override def children: Seq[LogicalPlan] = throw new UnsupportedOperationException()

  protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan =
    throw new UnsupportedOperationException()
}

case class UnparsedPredicateOptimize(
    tableIdent: Seq[String],
    table: LogicalPlan,
    tablePredicate: Option[String],
    orderExpr: Expression) extends UnparsedExpressionLogicalPlan {}
