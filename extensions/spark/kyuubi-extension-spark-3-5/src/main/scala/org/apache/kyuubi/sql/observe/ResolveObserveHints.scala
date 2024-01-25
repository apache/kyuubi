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
package org.apache.kyuubi.sql.observe

import java.util.Locale
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{MultiAlias, UnresolvedAlias}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, Generator, NamedExpression, StringLiteral}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{CollectMetrics, LogicalPlan, UnresolvedHint}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_HINT
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.aggregate.TypedAggregateExpression

import org.apache.kyuubi.sql.KyuubiSQLConf.OBSERVE_HINT_ENABLE

/**
 * A rule to resolve the OBSERVE hint.
 * OBSERVE hint usage like: /*+ OBSERVE('name', exprs) */
 */
object ResolveObserveHints extends Rule[LogicalPlan] {

  private val OBSERVE_HINT_NAME = "OBSERVE"

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(OBSERVE_HINT_ENABLE)) {
      return plan
    }
    plan.resolveOperatorsWithPruning(
      _.containsPattern(UNRESOLVED_HINT)) {
      case hint @ UnresolvedHint(hintName, _, _) => hintName.toUpperCase(Locale.ROOT) match {
          case OBSERVE_HINT_NAME =>
            val (name, exprs) = hint.parameters match {
              case Seq(StringLiteral(name), exprs @ _*) => (name, exprs)
              case Seq(exprs @ _*) => (nextObserverName(), exprs)
            }

            val invalidParams = exprs.filter(!_.isInstanceOf[Expression])
            if (invalidParams.nonEmpty) {
              val hintName = hint.name.toUpperCase(Locale.ROOT)
              throw invalidHintParameterError(hintName, invalidParams)
            }

            // named exprs, copy from org.apache.spark.sql.Column.named method
            val namedExprs = exprs.map {
              case expr: NamedExpression => expr
              // Leave an unaliased generator with an empty list of names since the analyzer will
              // generate the correct defaults after the nested expression's type has been resolved.
              case g: Generator => MultiAlias(g, Nil)

              // If we have a top level Cast, there is a chance to give it a better alias,
              // if there is a NamedExpression under this Cast.
              case c: Cast =>
                c.transformUp {
                  case c @ Cast(_: NamedExpression, _, _, _) => UnresolvedAlias(c)
                } match {
                  case ne: NamedExpression => ne
                  case _ => UnresolvedAlias(c, Some(generateAlias))
                }

              case expr: Expression => UnresolvedAlias(expr, Some(generateAlias))
            }

            CollectMetrics(name, namedExprs, hint.child)
          case _ => hint
        }
    }
  }

  private val id = new AtomicLong(0)
  private def nextObserverName(): String = s"OBSERVER_${id.getAndIncrement()}"

  private def invalidHintParameterError(hintName: String, invalidParams: Seq[Any]): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1047",
      messageParameters = Map(
        "hintName" -> hintName,
        "invalidParams" -> invalidParams.mkString(", ")))
  }

  private def generateAlias(e: Expression): String = {
    e match {
      case a: AggregateExpression if a.aggregateFunction.isInstanceOf[TypedAggregateExpression] =>
        a.aggregateFunction.toString
      case expr => toPrettySQL(expr)
    }
  }
}
