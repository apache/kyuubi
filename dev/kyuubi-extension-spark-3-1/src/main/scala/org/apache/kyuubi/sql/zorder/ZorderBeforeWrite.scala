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
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.plans.logical.{Filter, InsertIntoStatement, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule

case class ZorderBeforeWrite(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case o @ OptimizeZorderCommand(child) =>
        var relation: Option[HiveTableRelation] = None
        var partitionSpec: Map[String, Option[String]] = Map.empty
        val newChild = child.resolveOperatorsUp {
          case f @ Filter(condition,
            SubqueryAlias(_, tableRelation: HiveTableRelation)) if f.resolved =>
            if (!tableRelation.isPartitioned) {
              throw new ZorderException("Filters are only supported for partitioned table")
            }

            val partitionKeyIds = AttributeSet(tableRelation.partitionCols)
            if (condition.references.isEmpty || !condition.references.subsetOf(partitionKeyIds)) {
              throw new ZorderException("Only partition column filters are allowed")
            }

            val partitions = tableRelation.partitionCols.map { p =>
              p.name -> None
            }
            partitionSpec = Map(partitions: _*)
            f
          case r: HiveTableRelation =>
            relation = Option(r)
            r
        }

        if (o.resolved) {
          relation match {
            case Some(table) =>
              InsertIntoStatement(table, partitionSpec, Nil, newChild, true, false)
            case None => plan
          }
        } else {
          plan
        }

      case _ => plan
    }
  }
}
