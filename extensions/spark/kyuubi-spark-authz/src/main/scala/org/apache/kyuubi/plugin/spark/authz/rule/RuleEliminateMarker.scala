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

import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

import org.apache.kyuubi.plugin.spark.authz.rule.datamasking.{DataMaskingStage0Marker, DataMaskingStage1Marker}
import org.apache.kyuubi.plugin.spark.authz.rule.rowfilter.RowFilterMarker

object RuleEliminateMarker extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUp { case p =>
      p.transformExpressionsUp {
        case p: SubqueryExpression =>
          p.withNewPlan(apply(p.plan))
      } match {
        case marker0: DataMaskingStage0Marker => marker0.child
        case marker1: DataMaskingStage1Marker => marker1.child
        case rf: RowFilterMarker => rf.child
        case other => other
      }
    }
  }
}
