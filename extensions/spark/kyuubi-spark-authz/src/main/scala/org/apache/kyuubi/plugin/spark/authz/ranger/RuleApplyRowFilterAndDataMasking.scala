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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Identifier

import org.apache.kyuubi.plugin.spark.authz.{ObjectType, OperationType}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.plugin.spark.authz.util.RowFilterAndDataMaskingMarker

class RuleApplyRowFilterAndDataMasking(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Apply FilterAndMasking and wrap HiveTableRelation/LogicalRelation/DataSourceV2Relation with
    // RowFilterAndDataMaskingMarker if it is not wrapped yet.
    plan mapChildren {
      case p: RowFilterAndDataMaskingMarker => p
      case hiveTableRelation if hasResolvedHiveTable(hiveTableRelation) =>
        val table = getHiveTable(hiveTableRelation)
        applyFilterAndMasking(hiveTableRelation, table.identifier, spark)
      case logicalRelation if hasResolvedDatasourceTable(logicalRelation) =>
        val table = getDatasourceTable(logicalRelation)
        if (table.isEmpty) {
          logicalRelation
        } else {
          applyFilterAndMasking(logicalRelation, table.get.identifier, spark)
        }
      case datasourceV2Relation if hasResolvedDatasourceV2Table(datasourceV2Relation) =>
        val tableIdentifier = getDatasourceV2Identifier(datasourceV2Relation)
        if (tableIdentifier.isEmpty) {
          datasourceV2Relation
        } else {
          applyFilterAndMasking(datasourceV2Relation, tableIdentifier.get, spark)
        }
      case other => apply(other)
    }
  }

  private def applyFilterAndMasking(
      plan: LogicalPlan,
      identifier: Identifier,
      spark: SparkSession): LogicalPlan = {
    applyFilterAndMasking(plan, getTableIdentifierFromV2Identifier(identifier), spark)
  }

  private def applyFilterAndMasking(
      plan: LogicalPlan,
      identifier: TableIdentifier,
      spark: SparkSession): LogicalPlan = {
    val ugi = getAuthzUgi(spark.sparkContext)
    val opType = OperationType(plan.nodeName)
    val parse = spark.sessionState.sqlParser.parseExpression _
    val are = AccessResource(ObjectType.TABLE, identifier.database.orNull, identifier.table, null)
    val art = AccessRequest(are, ugi, opType, AccessType.SELECT)
    val filterExprStr = SparkRangerAdminPlugin.getFilterExpr(art)
    val newOutput = plan.output.map { attr =>
      val are =
        AccessResource(ObjectType.COLUMN, identifier.database.orNull, identifier.table, attr.name)
      val art = AccessRequest(are, ugi, opType, AccessType.SELECT)
      val maskExprStr = SparkRangerAdminPlugin.getMaskingExpr(art)
      if (maskExprStr.isEmpty) {
        attr
      } else {
        val maskExpr = parse(maskExprStr.get)
        Alias(maskExpr, attr.name)()
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
