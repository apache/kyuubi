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

package org.apache.kyuubi.plugin.spark.authz.rule

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

import org.apache.kyuubi.plugin.spark.authz.rule.permanentview.PermanentViewMarker

/**
 * Transforming up [[PermanentViewMarker]]
 */
case class RuleEliminatePermanentViewMarker(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  def eliminatePVM(plan: LogicalPlan): LogicalPlan = {
    plan.transformUp {
      case pvm: PermanentViewMarker =>
        val ret = pvm.child.transformAllExpressions {
          case s: SubqueryExpression => s.withNewPlan(eliminatePVM(s.plan))
        }
        // For each SubqueryExpression's PVM, we should mark as resolved to
        // avoid check privilege of PVM's internal Subquery.
        Authorization.markAllNodesAuthChecked(ret)
        ret
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    var matched = false
    val eliminatedPVM = plan.transformUp {
      case pvm: PermanentViewMarker =>
        matched = true
        pvm.child.transformAllExpressions {
          case s: SubqueryExpression => s.withNewPlan(eliminatePVM(s.plan))
        }
    }
    if (matched) {
      Authorization.markAllNodesAuthChecked(eliminatedPVM)
      val optimized = sparkSession.sessionState.optimizer.execute(eliminatedPVM)
      Authorization.markAllNodesAuthChecked(optimized)
    } else {
      eliminatedPVM
    }
  }
}
