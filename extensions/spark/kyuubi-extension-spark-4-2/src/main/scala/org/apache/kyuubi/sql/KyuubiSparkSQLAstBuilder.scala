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

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.ParseTree
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, Sort}

import org.apache.kyuubi.sql.KyuubiSparkSQLParser._
import org.apache.kyuubi.sql.zorder.{OptimizeZorderStatement, Zorder}

class KyuubiSparkSQLAstBuilder extends KyuubiSparkSQLBaseVisitor[AnyRef] with SQLConfHelper {

  def buildOptimizeStatement(
      unparsedPredicateOptimize: UnparsedPredicateOptimize,
      parseExpression: String => Expression): LogicalPlan = {

    val UnparsedPredicateOptimize(tableIdent, tablePredicate, orderExpr) =
      unparsedPredicateOptimize

    val predicate = tablePredicate.map(parseExpression)
    verifyPartitionPredicates(predicate)
    val table = UnresolvedRelation(tableIdent)
    val tableWithFilter = predicate match {
      case Some(expr) => Filter(expr, table)
      case None => table
    }
    val query =
      Sort(
        SortOrder(orderExpr, Ascending, NullsLast, Seq.empty) :: Nil,
        conf.getConf(KyuubiSQLConf.ZORDER_GLOBAL_SORT_ENABLED),
        Project(Seq(UnresolvedStar(None)), tableWithFilter))
    OptimizeZorderStatement(tableIdent, query)
  }

  private def verifyPartitionPredicates(predicates: Option[Expression]): Unit = {
    predicates.foreach {
      case p if !isLikelySelective(p) =>
        throw new KyuubiSQLExtensionException(s"unsupported partition predicates: ${p.sql}")
      case _ =>
    }
  }

  /**
   * Forked from Apache Spark's org.apache.spark.sql.catalyst.expressions.PredicateHelper
   * The `PredicateHelper.isLikelySelective()` is available since Spark-3.3, forked for Spark
   * that is lower than 3.3.
   *
   * Returns whether an expression is likely to be selective
   */
  private def isLikelySelective(e: Expression): Boolean = e match {
    case Not(expr) => isLikelySelective(expr)
    case And(l, r) => isLikelySelective(l) || isLikelySelective(r)
    case Or(l, r) => isLikelySelective(l) && isLikelySelective(r)
    case _: StringRegexExpression => true
    case _: BinaryComparison => true
    case _: In | _: InSet => true
    case _: StringPredicate => true
    case BinaryPredicate(_) => true
    case _: MultiLikeBase => true
    case _ => false
  }

  private object BinaryPredicate {
    def unapply(expr: Expression): Option[Expression] = expr match {
      case _: Contains => Option(expr)
      case _: StartsWith => Option(expr)
      case _: EndsWith => Option(expr)
      case _ => None
    }
  }

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

    val predicate = Option(ctx.whereClause())
      .map(_.partitionPredicate)
      .map(extractRawText(_))

    val zorderCols = ctx.zorderClause().order.asScala
      .map(visitMultipartIdentifier)
      .map(UnresolvedAttribute(_))
      .toSeq

    val orderExpr =
      if (zorderCols.length == 1) {
        zorderCols.head
      } else {
        Zorder(zorderCols)
      }
    UnparsedPredicateOptimize(tableIdent, predicate, orderExpr)
  }

  override def visitPassThrough(ctx: PassThroughContext): LogicalPlan = null

  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] =
    withOrigin(ctx) {
      ctx.parts.asScala.map(typedVisit[String]).toSeq
    }

  override def visitIdentifier(ctx: IdentifierContext): String = {
    withOrigin(ctx) {
      ctx.strictIdentifier() match {
        case quotedContext: QuotedIdentifierAlternativeContext =>
          typedVisit[String](quotedContext)
        case _ => ctx.getText
      }
    }
  }

  override def visitQuotedIdentifier(ctx: QuotedIdentifierContext): String = {
    withOrigin(ctx) {
      ctx.BACKQUOTED_IDENTIFIER().getText.stripPrefix("`").stripSuffix("`").replace("``", "`")
    }
  }

  override def visitZorderClause(ctx: ZorderClauseContext): Seq[UnresolvedAttribute] =
    withOrigin(ctx) {
      val res = ListBuffer[UnresolvedAttribute]()
      ctx.multipartIdentifier().forEach { identifier =>
        res += UnresolvedAttribute(identifier.parts.asScala.map(typedVisit[String]).toSeq)
      }
      res.toSeq
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

/**
 * a logical plan contains an unparsed expression that will be parsed by spark.
 */
trait UnparsedExpressionLogicalPlan extends LogicalPlan {
  override def output: Seq[Attribute] = throw new UnsupportedOperationException()

  override def children: Seq[LogicalPlan] = throw new UnsupportedOperationException()

  override def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan =
    throw new UnsupportedOperationException()
}

case class UnparsedPredicateOptimize(
    tableIdent: Seq[String],
    tablePredicate: Option[String],
    orderExpr: Expression) extends UnparsedExpressionLogicalPlan {}
