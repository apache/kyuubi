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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

import org.apache.kyuubi.plugin.spark.authz.ObjectType
import org.apache.kyuubi.plugin.spark.authz.serde._
import org.apache.kyuubi.plugin.spark.authz.util.{PermanentViewMarker, RowFilterAndDataMaskingMarker}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._

class RuleApplyRowFilterAndDataMasking(spark: SparkSession) extends Rule[LogicalPlan] {
  private def mapChildren(plan: LogicalPlan)(f: LogicalPlan => LogicalPlan): LogicalPlan = {
    val newChildren = plan match {
      case cmd if isKnownTableCommand(cmd) =>
        val tableCommandSpec = getTableCommandSpec(cmd)
        val queries = tableCommandSpec.queries(cmd)
        cmd.children.map {
          case c if queries.contains(c) => f(c)
          case other => other
        }
      case _ =>
        plan.children.map(f)
    }
    plan.withNewChildren(newChildren)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    mapChildren(plan) {
      case p: RowFilterAndDataMaskingMarker => p
      case scan if isKnownScan(scan) && scan.resolved =>
        val tables = getScanSpec(scan).tables(scan, spark)
        tables.headOption.map(applyFilterAndMasking(scan, _)).getOrElse(scan)
      case other => apply(other)
    }
  }

  private def applyFilterAndMasking(
      plan: LogicalPlan,
      table: Table): LogicalPlan = {
    val ugi = getAuthzUgi(spark.sparkContext)
    val opType = operationType(plan)
    val parse = spark.sessionState.sqlParser.parseExpression _
    val are = AccessResource(ObjectType.TABLE, table.database.orNull, table.table, null)
    val art = AccessRequest(are, ugi, opType, AccessType.SELECT)
    val filterExprStr = SparkRangerAdminPlugin.getFilterExpr(art)
    val newOutput = plan.output.map { attr =>
      val are =
        AccessResource(ObjectType.COLUMN, table.database.orNull, table.table, attr.name)
      val art = AccessRequest(are, ugi, opType, AccessType.SELECT)
      val maskExprStr = SparkRangerAdminPlugin.getMaskingExpr(art)
      if (maskExprStr.isEmpty) {
        attr
      } else {
        val maskExpr = parse(maskExprStr.get)
        plan match {
          case _: PermanentViewMarker =>
            Alias(maskExpr, attr.name)(exprId = attr.exprId)
          case _ =>
            Alias(maskExpr, attr.name)()
        }
      }
    }

    if (filterExprStr.isEmpty) {
      Project(newOutput, RowFilterAndDataMaskingMarker(plan))
    } else {
      val filterExpr = parse(filterExprStr.get)
      Project(newOutput, Filter(filterExpr, RowFilterAndDataMaskingMarker(plan)))
    }
  }
}
