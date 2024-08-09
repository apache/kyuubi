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
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan, Repartition, RepartitionByExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.CreateTable

import org.apache.kyuubi.sql.KyuubiSQLConf

case class RemoveRepartitionBeforeInsertInto(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (conf.getConf(KyuubiSQLConf.REMOVE_CUSTOMIZE_REPARTITION_BEFORE_INSERT)) {
      plan resolveOperators {
        case i @ InsertIntoStatement(_, _, _, Repartition(_, _, child), _, _, _) =>
          i.copy(query = child)
        case i @ InsertIntoStatement(_, _, _, RepartitionByExpression(_, child, _, _), _, _, _) =>
          i.copy(query = child)
        case c @ CreateTable(_, _, Some(Repartition(_, _, child))) =>
          c.copy(query = Some(child))
        case c @ CreateTable(_, _, Some(RepartitionByExpression(_, child, _, _))) =>
          c.copy(query = Some(child))
      }
    } else {
      plan
    }
  }
}
