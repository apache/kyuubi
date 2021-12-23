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
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag

import org.apache.kyuubi.sql.KyuubiSQLConf

object ForcedMaxOutputRowsConstraint {
  val CHILD_AGGREGATE: TreeNodeTag[String] = TreeNodeTag[String]("__kyuubi_child_agg__")
  val CHILD_AGGREGATE_FLAG: String = "__kyuubi_child_agg__"
}

case class ForcedMaxOutputRowsRule(sparkSession: SparkSession) extends ForcedMaxOutputRowsBase {
  override protected def isChildAggregate(a: Aggregate): Boolean =
    a.aggregateExpressions.exists(p =>
      p.getTagValue(ForcedMaxOutputRowsConstraint.CHILD_AGGREGATE)
        .contains(ForcedMaxOutputRowsConstraint.CHILD_AGGREGATE_FLAG))
}

/**
 * After SPARK-35712, we don't need mark child aggregate for spark 3.2.x or higher version,
 * for more detail, please see https://github.com/apache/spark/pull/32470
 */
case class MarkAggregateOrderRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  private def markChildAggregate(a: Aggregate): Unit = {
    // mark child aggregate
    a.aggregateExpressions.filter(_.resolved).foreach(_.setTagValue(
      ForcedMaxOutputRowsConstraint.CHILD_AGGREGATE,
      ForcedMaxOutputRowsConstraint.CHILD_AGGREGATE_FLAG))
  }

  protected def findAndMarkChildAggregate(plan: LogicalPlan): LogicalPlan = plan match {
    /*
     * The case mainly process order not aggregate column but grouping column as below
     * SELECT c1, COUNT(*) as cnt
     * FROM t1
     * GROUP BY c1
     * ORDER BY c1
     * */
    case a: Aggregate
        if a.aggregateExpressions
          .exists(x => x.resolved && x.name.equals("aggOrder")) =>
      markChildAggregate(a)
      plan
    case _ =>
      plan.children.foreach(_.foreach {
        case agg: Aggregate => markChildAggregate(agg)
        case _ => Unit
      })
      plan
  }

  override def apply(plan: LogicalPlan): LogicalPlan = conf.getConf(
    KyuubiSQLConf.WATCHDOG_FORCED_MAXOUTPUTROWS) match {
    case Some(_) => findAndMarkChildAggregate(plan)
    case _ => plan
  }
}
