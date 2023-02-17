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

package org.apache.kyuubi.plugin.spark.authz.ranger

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

import org.apache.kyuubi.plugin.spark.authz.ObjectType._
import org.apache.kyuubi.plugin.spark.authz.OperationType.OperationType
import org.apache.kyuubi.plugin.spark.authz.ranger.AccessType._
import org.apache.kyuubi.plugin.spark.authz.serde._
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.plugin.spark.authz.util.DataMaskingMarker

class RuleApplyDataMasking(spark: SparkSession) extends Rule[LogicalPlan] {
  private val parse = spark.sessionState.sqlParser.parseExpression _

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val ugi = getAuthzUgi(spark.sparkContext)
    val opType = operationType(plan)

    plan match {
      case marker: DataMaskingMarker => marker
      case cmd if isKnownTableCommand(cmd) =>
        val tableCommandSpec = getTableCommandSpec(cmd)
        val queries = tableCommandSpec.queries(cmd)
        cmd.mapChildren {
          case marker: DataMaskingMarker => marker
          case c if queries.contains(c) && c.resolved =>
            applyDataMasking(c, ugi, opType)
          case o => o
        }
      case cmd: Command => cmd
      case other if other.resolved => applyDataMasking(other, ugi, opType)
      case other => other.mapChildren(apply)
    }
  }

  private def applyDataMasking(
      plan: LogicalPlan,
      ugi: UserGroupInformation,
      opType: OperationType): LogicalPlan = {
    val scans = plan.collect {
      case scan if isKnownScan(scan) && scan.resolved => scan
    }
    if (scans.isEmpty) return plan

    val tableOptions = scans.map(s => getScanSpec(s).tables(s, spark).headOption)
    val idToTables = scans.zip(tableOptions).flatMap { case (scan, tableOpt) =>
      scan.output.map(attr => (attr.exprId, tableOpt))
    }.toMap

    val newPlan = plan.transformExpressionsUp {
      case e: NamedExpression
          if idToTables.contains(e.exprId) &&
            idToTables(e.exprId).isDefined =>
        val table = idToTables(e.exprId).get
        val are = AccessResource(COLUMN, table.database.orNull, table.table, e.name)
        val art = AccessRequest(are, ugi, opType, SELECT)
        val maskExprStr = SparkRangerAdminPlugin.getMaskingExpr(art)
        if (maskExprStr.isEmpty) {
          e
        } else {
          val maskExpr = parse(maskExprStr.get)
          Alias(maskExpr, e.name)()
        }
    }
    if (newPlan == plan) {
      plan
    } else {
      val childrenRestored = restoreChildren(newPlan)
      DataMaskingMarker(childrenRestored, plan)
    }
  }

  /**
   * Try to restore the unmasked plan and keep the top one later
   * @param plan the original logical plan
   */
  private def restoreChildren(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithSubqueries {
      case marker: DataMaskingMarker => marker.restored
    }
  }
}
