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
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

import org.apache.kyuubi.plugin.spark.authz.{ObjectType, OperationType}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.plugin.spark.authz.util.RowFilterMarker

class RuleApplyRowFilter(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformUp {
      case hiveTableRelation if hasResolvedHiveTable(hiveTableRelation) =>
        val table = getHiveTable(hiveTableRelation)
        applyFilter(hiveTableRelation, table, spark)
      case logicalRelation if hasResolvedDatasourceTable(logicalRelation) =>
        val table = getDatasourceTable(logicalRelation)
        if (table.isEmpty) {
          logicalRelation
        } else {
          applyFilter(logicalRelation, table.get, spark)
        }
    }
  }

  private def applyFilter(
      plan: LogicalPlan,
      table: CatalogTable,
      spark: SparkSession): LogicalPlan = {
    val identifier = table.identifier
    val ugi = getAuthzUgi(spark.sparkContext)
    val opType = OperationType(plan.nodeName)
    val are = AccessResource(ObjectType.TABLE, identifier.database.orNull, identifier.table, null)
    val art = AccessRequest(are, ugi, opType, AccessType.SELECT)
    val filterExprStr = SparkRangerAdminPlugin.getFilterExpr(art)
    if (filterExprStr.isEmpty) {
      plan
    } else {
      val filterExpr = spark.sessionState.sqlParser.parseExpression(filterExprStr.get)
      Filter(filterExpr, RowFilterMarker(plan))
    }
  }
}
