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

package org.apache.kyuubi.sql.zorder

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeSet, BinaryComparison, Contains, EndsWith, Expression, In, InSet, MultiLikeBase, Not, Or, StartsWith, StringPredicate, StringRegexExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule

import org.apache.kyuubi.sql.KyuubiSQLExtensionException

/**
 * Resolve `OptimizeZorderStatement` to `OptimizeZorderCommand`
 */
abstract class ResolveZorderBase extends Rule[LogicalPlan] {
  def session: SparkSession
  def buildOptimizeZorderCommand(
      catalogTable: CatalogTable,
      query: LogicalPlan): OptimizeZorderCommandBase

  protected def checkQueryAllowed(query: LogicalPlan): Unit = query foreach {
    case Filter(condition, SubqueryAlias(_, tableRelation: HiveTableRelation)) =>
      if (tableRelation.partitionCols.isEmpty) {
        throw new KyuubiSQLExtensionException("Filters are only supported for partitioned table")
      }

      if (!isLikelySelective(condition)) {
        throw new KyuubiSQLExtensionException(s"unsupported partition predicates: ${condition.sql}")
      }

      val partitionKeyIds = AttributeSet(tableRelation.partitionCols)
      if (condition.references.isEmpty || !condition.references.subsetOf(partitionKeyIds)) {
        throw new KyuubiSQLExtensionException("Only partition column filters are allowed")
      }

    case _ =>
  }

  protected def getTableIdentifier(tableIdent: Seq[String]): TableIdentifier = tableIdent match {
    case Seq(tbl) => TableIdentifier.apply(tbl)
    case Seq(db, tbl) => TableIdentifier.apply(tbl, Some(db))
    case _ => throw new KyuubiSQLExtensionException(
        "only support session catalog table, please use db.table instead")
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case statement: OptimizeZorderStatement if statement.query.resolved =>
      checkQueryAllowed(statement.query)
      val tableIdentifier = getTableIdentifier(statement.tableIdentifier)
      val catalogTable = session.sessionState.catalog.getTableMetadata(tableIdentifier)
      buildOptimizeZorderCommand(catalogTable, statement.query)

    case _ => plan
  }

  /**
   * The `PredicateHelper.isLikelySelective()` is available since Spark-3.3, forked for Spark
   * that is lower than 3.3.
   * Forked from Apache Spark's org.apache.spark.sql.catalyst.expressions.PredicateHelper
   *
   * Returns whether an expression is likely to be selective
   */
  private def isLikelySelective(e: Expression): Boolean = e match {
    case Not(expr) => isLikelySelective(expr)
    case And(l, r) => isLikelySelective(l) || isLikelySelective(r)
    case Or(l, r) => isLikelySelective(l) && isLikelySelective(r)
    case _: StringRegexExpression => true
    case _: BinaryComparison => true
    case _: In | _: InSet => true
    case _: StringPredicate => true
    case BinaryPredicate(_) => true
    case _: MultiLikeBase => true
    case _ => false
  }

  private object BinaryPredicate {
    def unapply(expr: Expression): Option[Expression] = expr match {
      case _: Contains => Option(expr)
      case _: StartsWith => Option(expr)
      case _: EndsWith => Option(expr)
      case _ => None
    }
  }
}

/**
 * Resolve `OptimizeZorderStatement` to `OptimizeZorderCommand`
 */
case class ResolveZorder(session: SparkSession) extends ResolveZorderBase {
  override def buildOptimizeZorderCommand(
      catalogTable: CatalogTable,
      query: LogicalPlan): OptimizeZorderCommandBase = {
    OptimizeZorderCommand(catalogTable, query)
  }
}
