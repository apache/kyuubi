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
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule

import org.apache.kyuubi.sql.KyuubiSQLExtensionException

/**
 * Resolve `OptimizeZorderStatement` to `OptimizeZorderCommand`
 */
case class ResolveZorder(session: SparkSession) extends Rule[LogicalPlan] {
  private def checkQueryAllowed(query: LogicalPlan): Unit = query foreach {
    case Filter(condition, SubqueryAlias(_, tableRelation: HiveTableRelation)) =>
      if (tableRelation.partitionCols.isEmpty) {
        throw new KyuubiSQLExtensionException("Filters are only supported for partitioned table")
      }

      val partitionKeyIds = AttributeSet(tableRelation.partitionCols)
      if (condition.references.isEmpty || !condition.references.subsetOf(partitionKeyIds)) {
        throw new KyuubiSQLExtensionException("Only partition column filters are allowed")
      }

    case _ =>
  }

  private def getTableIdentifier(tableIdent: Seq[String]): TableIdentifier = tableIdent match {
    case Seq(tbl) => TableIdentifier.apply(tbl)
    case Seq(db, tbl) => TableIdentifier.apply(tbl, Some(db))
    case _ => throw new KyuubiSQLExtensionException(
      "only support session catalog table, please use db.table instead")
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case OptimizeZorderStatement(tableIdent, query) if query.resolved =>
      checkQueryAllowed(query)
      val tableIdentifier = getTableIdentifier(tableIdent)
      val catalogTable = session.sessionState.catalog.getTableMetadata(tableIdentifier)
      OptimizeZorderCommand(catalogTable, query)

    case _ => plan
  }
}
