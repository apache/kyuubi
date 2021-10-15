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

package org.apache.kyuubi.sql.zorder

import java.util.Locale
import javax.xml.bind.DatatypeConverter

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.{ParseTree, TerminalNode}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{And, Ascending, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, Not, NullsLast, Or, SortOrder}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.parser.ParserUtils.{string, stringWithoutUnescape, withOrigin}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, Sort}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{getZoneId, stringToDate, stringToTimestamp}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.hive.HiveAnalysis.conf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.kyuubi.sql.zorder.ZorderSqlExtensionsParser.{BigDecimalLiteralContext, BigIntLiteralContext, BooleanLiteralContext, DecimalLiteralContext, DoubleLiteralContext, IntegerLiteralContext, LogicalBinaryContext, MultipartIdentifierContext, NullLiteralContext, NumberContext, OptimizeZorderContext, PassThroughContext, QueryContext, SingleStatementContext, SmallIntLiteralContext, StringLiteralContext, TinyIntLiteralContext, TypeConstructorContext, ZorderClauseContext}

class ZorderSqlAstBuilder extends ZorderSqlExtensionsBaseVisitor[AnyRef] {
  /**
   * Create an expression from the given context. This method just passes the context on to the
   * visitor and only takes care of typing (We assume that the visitor returns an Expression here).
   */
  protected def expression(ctx: ParserRuleContext): Expression = typedVisit(ctx)

  protected def multiPart(ctx: ParserRuleContext): Seq[String] = typedVisit(ctx)

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = {
    visit(ctx.statement()).asInstanceOf[LogicalPlan]
  }

  override def visitPassThrough(ctx: PassThroughContext): LogicalPlan = null

  override def visitOptimizeZorder(
      ctx: OptimizeZorderContext): OptimizeZorderStatement = withOrigin(ctx) {
    val tableIdent = multiPart(ctx.multipartIdentifier())
    val table = UnresolvedRelation(tableIdent)

    val whereClause = if (ctx.whereClause() == null) {
      None
    } else {
      Option(expression(ctx.whereClause().booleanExpression()))
    }

    val tableWithFilter = whereClause match {
      case Some(expr) => Filter(expr, table)
      case None => table
    }

    val zorderCols = ctx.zorderClause().order.asScala
      .map(visitMultipartIdentifier)
      .map(UnresolvedAttribute(_))
      .toSeq

    val orderExpr = if (zorderCols.length == 1) {
      zorderCols.head
    } else {
      Zorder(zorderCols)
    }
    val query =
      Sort(
        SortOrder(orderExpr, Ascending, NullsLast, Seq.empty) :: Nil,
        true,
        Project(Seq(UnresolvedStar(None)), tableWithFilter))

    OptimizeZorderStatement(tableIdent, query)
  }

  override def visitQuery(ctx: QueryContext): Expression = withOrigin(ctx) {
    val left = new UnresolvedAttribute(multiPart(ctx.multipartIdentifier()))
    val right = expression(ctx.constant())
    val operator = ctx.comparisonOperator().getChild(0).asInstanceOf[TerminalNode]
    operator.getSymbol.getType match {
      case ZorderSqlExtensionsParser.EQ =>
        EqualTo(left, right)
      case ZorderSqlExtensionsParser.NSEQ =>
        EqualNullSafe(left, right)
      case ZorderSqlExtensionsParser.NEQ | ZorderSqlExtensionsParser.NEQJ =>
        Not(EqualTo(left, right))
      case ZorderSqlExtensionsParser.LT =>
        LessThan(left, right)
      case ZorderSqlExtensionsParser.LTE =>
        LessThanOrEqual(left, right)
      case ZorderSqlExtensionsParser.GT =>
        GreaterThan(left, right)
      case ZorderSqlExtensionsParser.GTE =>
        GreaterThanOrEqual(left, right)
    }
  }

  override def visitLogicalBinary(ctx: LogicalBinaryContext): Expression = withOrigin(ctx) {
    val expressionType = ctx.operator.getType
    val expressionCombiner = expressionType match {
      case ZorderSqlExtensionsParser.AND => And.apply _
      case ZorderSqlExtensionsParser.OR => Or.apply _
    }

    // Collect all similar left hand contexts.
    val contexts = ArrayBuffer(ctx.right)
    var current = ctx.left
    def collectContexts: Boolean = current match {
      case lbc: LogicalBinaryContext if lbc.operator.getType == expressionType =>
        contexts += lbc.right
        current = lbc.left
        true
      case _ =>
        contexts += current
        false
    }
    while (collectContexts) {
      // No body - all updates take place in the collectContexts.
    }

    // Reverse the contexts to have them in the same sequence as in the SQL statement & turn them
    // into expressions.
    val expressions = contexts.reverseMap(expression)

    // Create a balanced tree.
    def reduceToExpressionTree(low: Int, high: Int): Expression = high - low match {
      case 0 =>
        expressions(low)
      case 1 =>
        expressionCombiner(expressions(low), expressions(high))
      case x =>
        val mid = low + x / 2
        expressionCombiner(
          reduceToExpressionTree(low, mid),
          reduceToExpressionTree(mid + 1, high))
    }
    reduceToExpressionTree(0, expressions.size - 1)
  }

  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext):
    Seq[String] = withOrigin(ctx) {
      ctx.parts.asScala.map(_.getText)
  }

  override def visitZorderClause(ctx: ZorderClauseContext):
    Seq[UnresolvedAttribute] = withOrigin(ctx) {
      val res = ListBuffer[UnresolvedAttribute]()
      ctx.multipartIdentifier().forEach { identifier =>
        res += UnresolvedAttribute(identifier.parts.asScala.map(_.getText))
      }
      res
  }

  /**
   * Create a NULL literal expression.
   */
  override def visitNullLiteral(ctx: NullLiteralContext): Literal = withOrigin(ctx) {
    Literal(null)
  }

  /**
   * Create a Boolean literal expression.
   */
  override def visitBooleanLiteral(ctx: BooleanLiteralContext): Literal = withOrigin(ctx) {
    if (ctx.getText.toBoolean) {
      Literal.TrueLiteral
    } else {
      Literal.FalseLiteral
    }
  }

  /**
   * Create a typed Literal expression. A typed literal has the following SQL syntax:
   * {{{
   *   [TYPE] '[VALUE]'
   * }}}
   * Currently Date, Timestamp, Interval and Binary typed literals are supported.
   */
  override def visitTypeConstructor(ctx: TypeConstructorContext): Literal = withOrigin(ctx) {
    val value = string(ctx.STRING)
    val valueType = ctx.identifier.getText.toUpperCase(Locale.ROOT)

    def toLiteral[T](f: UTF8String => Option[T], t: DataType): Literal = {
      f(UTF8String.fromString(value)).map(Literal(_, t)).getOrElse {
        throw new ParseException(s"Cannot parse the $valueType value: $value", ctx)
      }
    }
    try {
      valueType match {
        case "DATE" =>
          toLiteral(stringToDate(_, getZoneId(SQLConf.get.sessionLocalTimeZone)), DateType)
        case "TIMESTAMP" =>
          val zoneId = getZoneId(SQLConf.get.sessionLocalTimeZone)
          toLiteral(stringToTimestamp(_, zoneId), TimestampType)
        case "INTERVAL" =>
          val interval = try {
            IntervalUtils.stringToInterval(UTF8String.fromString(value))
          } catch {
            case e: IllegalArgumentException =>
              val ex = new ParseException("Cannot parse the INTERVAL value: " + value, ctx)
              ex.setStackTrace(e.getStackTrace)
              throw ex
          }
          Literal(interval, CalendarIntervalType)
        case "X" =>
          val padding = if (value.length % 2 != 0) "0" else ""

          Literal(DatatypeConverter.parseHexBinary(padding + value))
        case other =>
          throw new ParseException(s"Literals of type '$other' are currently not" +
            " supported.", ctx)
      }
    } catch {
      case e: IllegalArgumentException =>
        val message = Option(e.getMessage).getOrElse(s"Exception parsing $valueType")
        throw new ParseException(message, ctx)
    }
  }

  /**
   * Create a String literal expression.
   */
  override def visitStringLiteral(ctx: StringLiteralContext): Literal = withOrigin(ctx) {
    Literal(createString(ctx))
  }

  /**
   * Create a decimal literal for a regular decimal number.
   */
  override def visitDecimalLiteral(ctx: DecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(BigDecimal(ctx.getText).underlying())
  }

  /** Create a numeric literal expression. */
  private def numericLiteral(
      ctx: NumberContext,
      rawStrippedQualifier: String,
      minValue: BigDecimal,
      maxValue: BigDecimal,
      typeName: String)(converter: String => Any): Literal = withOrigin(ctx) {
    try {
      val rawBigDecimal = BigDecimal(rawStrippedQualifier)
      if (rawBigDecimal < minValue || rawBigDecimal > maxValue) {
        throw new ParseException(s"Numeric literal ${rawStrippedQualifier} does not " +
          s"fit in range [${minValue}, ${maxValue}] for type ${typeName}", ctx)
      }
      Literal(converter(rawStrippedQualifier))
    } catch {
      case e: NumberFormatException =>
        throw new ParseException(e.getMessage, ctx)
    }
  }

  /**
   * Create a Byte Literal expression.
   */
  override def visitTinyIntLiteral(ctx: TinyIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Byte.MinValue, Byte.MaxValue, ByteType.simpleString)(_.toByte)
  }

  /**
   * Create an integral literal expression. The code selects the most narrow integral type
   * possible, either a BigDecimal, a Long or an Integer is returned.
   */
  override def visitIntegerLiteral(ctx: IntegerLiteralContext): Literal = withOrigin(ctx) {
    BigDecimal(ctx.getText) match {
      case v if v.isValidInt =>
        Literal(v.intValue)
      case v if v.isValidLong =>
        Literal(v.longValue)
      case v => Literal(v.underlying())
    }
  }

  /**
   * Create a Short Literal expression.
   */
  override def visitSmallIntLiteral(ctx: SmallIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Short.MinValue, Short.MaxValue, ShortType.simpleString)(_.toShort)
  }

  /**
   * Create a Long Literal expression.
   */
  override def visitBigIntLiteral(ctx: BigIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Long.MinValue, Long.MaxValue, LongType.simpleString)(_.toLong)
  }

  /**
   * Create a Double Literal expression.
   */
  override def visitDoubleLiteral(ctx: DoubleLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier,
      Double.MinValue, Double.MaxValue, DoubleType.simpleString)(_.toDouble)
  }

  /**
   * Create a BigDecimal Literal expression.
   */
  override def visitBigDecimalLiteral(ctx: BigDecimalLiteralContext): Literal = {
    val raw = ctx.getText.substring(0, ctx.getText.length - 2)
    try {
      Literal(BigDecimal(raw).underlying())
    } catch {
      case e: AnalysisException =>
        throw new ParseException(e.message, ctx)
    }
  }

  /**
   * Create a String from a string literal context. This supports multiple consecutive string
   * literals, these are concatenated, for example this expression "'hello' 'world'" will be
   * converted into "helloworld".
   *
   * Special characters can be escaped by using Hive/C-style escaping.
   */
  private def createString(ctx: StringLiteralContext): String = {
    if (conf.escapedStringLiterals) {
      ctx.STRING().asScala.map(stringWithoutUnescape).mkString
    } else {
      ctx.STRING().asScala.map(string).mkString
    }
  }

  private def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }
}
