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

package org.apache.kyuubi.plugin.spark.authz.rule.datamasking

import org.apache.spark.authz.AuthzConf.dataMaskingEnabled
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}

import org.apache.kyuubi.plugin.spark.authz.rule.RuleHelper
import org.apache.kyuubi.plugin.spark.authz.serde._

/**
 * See [[RuleApplyDataMaskingStage0]] also.
 *
 * This is the second step for data masking. It will fulfill the missing attributes that
 * have a related masker expression buffered by DataMaskingStage0Marker.
 */
case class RuleApplyDataMaskingStage1(spark: SparkSession) extends RuleHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!dataMaskingEnabled(conf)) {
      plan
    } else {
      plan match {
        case marker0: DataMaskingStage0Marker => marker0
        case marker1: DataMaskingStage1Marker => marker1
        case cmd if isKnownTableCommand(cmd) =>
          val tableCommandSpec = getTableCommandSpec(cmd)
          val queries = tableCommandSpec.queries(cmd)
          cmd.mapChildren {
            case marker0: DataMaskingStage0Marker => marker0
            case marker1: DataMaskingStage1Marker => marker1
            case query if queries.contains(query) && query.resolved =>
              applyDataMasking(query)
            case o => o
          }
        case cmd: Command if cmd.childrenResolved =>
          cmd.mapChildren(applyDataMasking)
        case cmd: Command => cmd
        case other if other.resolved => applyDataMasking(other)
        case other => other
      }
    }
  }

  private def applyDataMasking(plan: LogicalPlan): LogicalPlan = {
    assert(plan.resolved, "the current masking approach relies on a resolved plan")
    def replaceOriginExprWithMasker(plan: LogicalPlan): LogicalPlan = plan match {
      case m: DataMaskingStage0Marker => m
      case m: DataMaskingStage1Marker => m
      case p =>
        val maskerExprs = p.collect {
          case marker: DataMaskingStage0Marker if marker.resolved => marker.exprToMaskers()
        }.flatten.toMap
        if (maskerExprs.isEmpty) {
          p
        } else {
          val t = p.transformExpressionsUp {
            case e: NamedExpression => maskerExprs.getOrElse(e.exprId, e)
          }
          t.withNewChildren(t.children.map(replaceOriginExprWithMasker))
        }
    }
    val newPlan = replaceOriginExprWithMasker(plan)

    if (newPlan == plan) {
      plan
    } else {
      DataMaskingStage1Marker(newPlan)
    }
  }
}
