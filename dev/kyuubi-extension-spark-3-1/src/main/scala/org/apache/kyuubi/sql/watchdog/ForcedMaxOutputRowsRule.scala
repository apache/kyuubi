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

package org.apache.kyuubi.sql.watchdog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.AnalysisContext
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Distinct, Filter, Limit, LogicalPlan, Project, RepartitionByExpression, Sort, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.command.DataWritingCommand

import org.apache.kyuubi.sql.KyuubiSQLConf

object ForcedMaxOutputRowsConstraint {
  val CHILD_AGGREGATE: TreeNodeTag[String] = TreeNodeTag[String]("__kyuubi_child_agg__")
  val CHILD_AGGREGATE_FLAG: String = "__kyuubi_child_agg__"
}

/*
* Add ForcedMaxOutputRows rule for output rows limitation
* to avoid huge output rows of non_limit query unexpectedly
* mainly applied to cases as below:
*
* case 1:
* {{{
*   SELECT [c1, c2, ...]
* }}}
*
* case 2:
* {{{
*   WITH CTE AS (
*   ...)
* SELECT [c1, c2, ...] FROM CTE ...
* }}}
*
* The Logical Rule add a GlobalLimit node before root project
* */
case class ForcedMaxOutputRowsRule(session: SparkSession) extends Rule[LogicalPlan] {

  private def isChildAggregate(a: Aggregate): Boolean = a
    .aggregateExpressions.exists(p => p.getTagValue(ForcedMaxOutputRowsConstraint.CHILD_AGGREGATE)
    .contains(ForcedMaxOutputRowsConstraint.CHILD_AGGREGATE_FLAG))

  private def isView: Boolean = {
    val nestedViewDepth = AnalysisContext.get.nestedViewDepth
    nestedViewDepth > 0
  }

  private def canInsertLimitInner(p: LogicalPlan): Boolean = p match {

    case Aggregate(_, Alias(_, "havingCondition")::Nil, _) => false
    case agg: Aggregate => !isChildAggregate(agg)
    case _: RepartitionByExpression => true
    case _: Distinct => true
    case _: Filter => true
    case _: Project => true
    case Limit(_, _) => true
    case _: Sort => true
    case Union(children, _, _) =>
      if (children.exists(_.isInstanceOf[DataWritingCommand])) {
        false
      } else {
        true
      }
    case _ => false

  }

  private def canInsertLimit(p: LogicalPlan, maxOutputRowsOpt: Option[Int]): Boolean = {

    maxOutputRowsOpt match {
      case Some(forcedMaxOutputRows) => canInsertLimitInner(p) &&
        !p.maxRows.exists(_ <= forcedMaxOutputRows) &&
        !isView
      case None => false
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val maxOutputRowsOpt = conf.getConf(KyuubiSQLConf.WATCHDOG_FORCED_MAXOUTPUTROWS)
    plan match {
      case p if p.resolved && canInsertLimit(p, maxOutputRowsOpt) => Limit(
        maxOutputRowsOpt.get, plan)
      case _ => plan
    }
  }

}

case class MarkAggregateOrderRule(session: SparkSession) extends Rule[LogicalPlan] {

  private def markChildAggregate(a: Aggregate): Unit = {
    // mark child aggregate
    a.aggregateExpressions.filter(_.resolved).foreach(_.setTagValue(
      ForcedMaxOutputRowsConstraint.CHILD_AGGREGATE,
      ForcedMaxOutputRowsConstraint.CHILD_AGGREGATE_FLAG)
    )
  }

  private def findAndMarkChildAggregate(plan: LogicalPlan): LogicalPlan = plan match {
    /*
    * The case mainly process order not aggregate column but grouping column as below
    * SELECT c1, COUNT(*) as cnt
    * FROM t1
    * GROUP BY c1
    * ORDER BY c1
    * */
    case a: Aggregate if a.aggregateExpressions
      .exists(x => x.resolved && x.name.equals("aggOrder")) => markChildAggregate(a)
      plan

    case _ => plan.children.foreach(_.foreach {
        case agg: Aggregate => markChildAggregate(agg)
        case _ => Unit
      }
    )
      plan
  }

  override def apply(plan: LogicalPlan): LogicalPlan = conf.getConf(
    KyuubiSQLConf.WATCHDOG_FORCED_MAXOUTPUTROWS
  ) match {
    case Some(_) => findAndMarkChildAggregate(plan)
    case _ => plan
  }
}
