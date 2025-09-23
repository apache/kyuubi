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

package org.apache.kyuubi.plugin.spark.authz.rule.plan

import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Rule to add [[ChildOutputHolder]] used to fixed logic plan's child node's output,
 * now used for following cases:
 *
 *  1. Aggregate(count(*)/count(1)), it's child node will be pruned in Spark optimizer
 *     rule [[org.apache.spark.sql.catalyst.optimizer.ColumnPruning]].
 */
object RuleChildOutputMarker extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case a @ Aggregate(_, _, child)
          if !child.isInstanceOf[ChildOutputHolder] &&
            child.outputSet.intersect(a.references).isEmpty =>
        a.copy(child = ChildOutputHolder(child, child.output))
    }
  }
}
