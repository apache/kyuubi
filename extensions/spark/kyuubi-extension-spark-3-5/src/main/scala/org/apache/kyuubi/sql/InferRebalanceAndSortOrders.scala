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

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, Expression, NamedExpression, UnaryExpression}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Generate, LogicalPlan, Project, Sort, SubqueryAlias, View, Window}

/**
 * Infer the columns for Rebalance and Sort to improve the compression ratio.
 *
 * For example
 * {{{
 *   INSERT INTO TABLE t PARTITION(p='a')
 *   SELECT * FROM t1 JOIN t2 on t1.c1 = t2.c1
 * }}}
 * the inferred columns are: t1.c1
 */
object InferRebalanceAndSortOrders {

  type PartitioningAndOrdering = (Seq[Expression], Seq[Expression])

  private def getAliasMap(named: Seq[NamedExpression]): Map[Expression, Attribute] = {
    @tailrec
    def throughUnary(e: Expression): Expression = e match {
      case u: UnaryExpression if u.deterministic =>
        throughUnary(u.child)
      case _ => e
    }

    named.flatMap {
      case a @ Alias(child, _) =>
        Some((throughUnary(child).canonicalized, a.toAttribute))
      case _ => None
    }.toMap
  }

  def infer(plan: LogicalPlan): Option[PartitioningAndOrdering] = {
    def candidateKeys(
        input: LogicalPlan,
        output: AttributeSet = AttributeSet.empty): Option[PartitioningAndOrdering] = {
      input match {
        case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, _, _, _, _, _) =>
          joinType match {
            case LeftSemi | LeftAnti | LeftOuter => Some((leftKeys, leftKeys))
            case RightOuter => Some((rightKeys, rightKeys))
            case Inner | FullOuter =>
              if (output.isEmpty) {
                Some((leftKeys ++ rightKeys, leftKeys ++ rightKeys))
              } else {
                assert(leftKeys.length == rightKeys.length)
                val keys = leftKeys.zip(rightKeys).flatMap { case (left, right) =>
                  if (left.references.subsetOf(output)) {
                    Some(left)
                  } else if (right.references.subsetOf(output)) {
                    Some(right)
                  } else {
                    None
                  }
                }
                Some((keys, keys))
              }
            case _ => None
          }
        case agg: Aggregate =>
          val aliasMap = getAliasMap(agg.aggregateExpressions)
          Some((
            agg.groupingExpressions.map(p => aliasMap.getOrElse(p.canonicalized, p)),
            agg.groupingExpressions.map(o => aliasMap.getOrElse(o.canonicalized, o))))
        case s: Sort => Some((s.order.map(_.child), s.order.map(_.child)))
        case p: Project =>
          val aliasMap = getAliasMap(p.projectList)
          candidateKeys(p.child, p.references).map { case (partitioning, ordering) =>
            (
              partitioning.map(p => aliasMap.getOrElse(p.canonicalized, p)),
              ordering.map(o => aliasMap.getOrElse(o.canonicalized, o)))
          }
        case f: Filter => candidateKeys(f.child, output)
        case s: SubqueryAlias => candidateKeys(s.child, output)
        case v: View => candidateKeys(v.child, output)
        case g: Generate => candidateKeys(g.child, AttributeSet(g.requiredChildOutput))
        case w: Window =>
          val aliasMap = getAliasMap(w.windowExpressions)
          Some((
            w.partitionSpec.map(p => aliasMap.getOrElse(p.canonicalized, p)),
            w.orderSpec.map(_.child).map(o => aliasMap.getOrElse(o.canonicalized, o))))

        case _ => None
      }
    }

    candidateKeys(plan).map { case (partitioning, ordering) =>
      (
        partitioning.filter(_.references.subsetOf(plan.outputSet)),
        ordering.filter(_.references.subsetOf(plan.outputSet)))
    }
  }
}
