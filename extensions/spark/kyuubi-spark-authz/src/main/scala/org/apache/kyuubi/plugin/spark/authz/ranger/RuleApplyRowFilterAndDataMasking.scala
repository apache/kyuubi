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

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNode

import org.apache.kyuubi.plugin.spark.authz.{ObjectType, OperationType}
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.plugin.spark.authz.util.RowFilterAndDataMaskingMarker

class RuleApplyRowFilterAndDataMasking(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Wrap HiveTableRelation/LogicalRelation with RowFilterAndDataMaskingMarker if it is not
    // wrapped yet.
    // Not using TreeNode#transformUpWithPruning as transformUpWithPruning is not present until
    // Spark 3.2
    if (plan.isInstanceOf[RowFilterAndDataMaskingMarker]) {
      plan
    } else {
      val afterRuleOnChildren = plan.mapChildren(apply)
      val newNode =
        if (plan.fastEquals(afterRuleOnChildren)) {
          plan match {
            case hiveTableRelation if hasResolvedHiveTable(hiveTableRelation) =>
              val table = getHiveTable(hiveTableRelation)
              applyFilterAndMasking(hiveTableRelation, table, spark)
            case logicalRelation if hasResolvedDatasourceTable(logicalRelation) =>
              val table = getDatasourceTable(logicalRelation)
              if (table.isEmpty) {
                logicalRelation
              } else {
                applyFilterAndMasking(logicalRelation, table.get, spark)
              }
            case _ => plan
          }
        } else {
          afterRuleOnChildren
        }
      if (plan.eq(newNode)) {
        plan
      } else {
        // TreeNode#tags is not present until Spark 3.0
        if (isSparkVersionAtLeast("3.0")) {
          val tags = getPlanTags(newNode)
          val originTags = getPlanTags(plan)
          if (tags.isEmpty) {
            tags ++= originTags
          }
        }
        newNode
      }
    }
  }

  private def applyFilterAndMasking(
      plan: LogicalPlan,
      table: CatalogTable,
      spark: SparkSession): LogicalPlan = {
    val identifier = table.identifier
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

  private def getPlanTags(plan: LogicalPlan): mutable.Map[Any, Any] = {
    val clazz = classOf[TreeNode[_]]
    Try {
      val field = clazz.getDeclaredField("tags")
      field.setAccessible(true)
      field.get(plan)
    } match {
      case Success(value) => value.asInstanceOf[mutable.Map[Any, Any]]
      case Failure(e) =>
        val candidates = clazz.getDeclaredFields.map(_.getName).mkString("[", ",", "]")
        throw new RuntimeException(s"tags not in $candidates", e)
    }
  }
}
