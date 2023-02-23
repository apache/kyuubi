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

package org.apache.kyuubi.plugin.spark.authz.ranger.datamasking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}

import org.apache.kyuubi.plugin.spark.authz.ObjectType
import org.apache.kyuubi.plugin.spark.authz.OperationType.QUERY
import org.apache.kyuubi.plugin.spark.authz.ranger._
import org.apache.kyuubi.plugin.spark.authz.serde._

/**
 * The data masking rule
 */
case class RuleApplyDataMaskingStage0(spark: SparkSession) extends RuleHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val newPlan = mapChildren(plan) {
      case p: DataMaskingStage0Marker => p
      case p: DataMaskingStage1Marker => p
      case scan if isKnownScan(scan) && scan.resolved =>
        val tables = getScanSpec(scan).tables(scan, spark)
        tables.headOption.map(applyMasking(scan, _)).getOrElse(scan)
      case other => apply(other)
    }
    newPlan
  }

  private def applyMasking(
      plan: LogicalPlan,
      table: Table): LogicalPlan = {
    val newOutput = plan.output.map { attr =>
      val are =
        AccessResource(ObjectType.COLUMN, table.database.orNull, table.table, attr.name)
      val art = AccessRequest(are, ugi, QUERY, AccessType.SELECT)
      val maskExprStr = SparkRangerAdminPlugin.getMaskingExpr(art)
      maskExprStr.map(parse).map(Alias(_, attr.name)()).getOrElse(attr)
    }
    if (newOutput == plan.output) {
      plan
    } else {
      DataMaskingStage0Marker(Project(newOutput, plan), plan)
    }
  }
}
