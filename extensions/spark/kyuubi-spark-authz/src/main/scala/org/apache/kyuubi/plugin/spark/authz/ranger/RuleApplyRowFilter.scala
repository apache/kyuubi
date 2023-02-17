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
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

import org.apache.kyuubi.plugin.spark.authz.ObjectType
import org.apache.kyuubi.plugin.spark.authz.serde._
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils.getAuthzUgi
import org.apache.kyuubi.plugin.spark.authz.util.RowFilterMarker

class RuleApplyRowFilter(spark: SparkSession) extends Rule[LogicalPlan] {
  private val parse = spark.sessionState.sqlParser.parseExpression _

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
      case p: RowFilterMarker => p
      case scan if isKnownScan(scan) && scan.resolved =>
        val tables = getScanSpec(scan).tables(scan, spark)
        tables.headOption.map(applyFilter(scan, _)).getOrElse(scan)
      case other => apply(other)
    }
  }
  private def applyFilter(
      plan: LogicalPlan,
      table: Table): LogicalPlan = {
    val ugi = getAuthzUgi(spark.sparkContext)
    val opType = operationType(plan)
    val are = AccessResource(ObjectType.TABLE, table.database.orNull, table.table, null)
    val art = AccessRequest(are, ugi, opType, AccessType.SELECT)
    val filterExpr = SparkRangerAdminPlugin.getFilterExpr(art).map(parse)
    filterExpr.foldLeft(plan)((p, e) => Filter(e, RowFilterMarker(p)))
  }
}
