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

package org.apache.kyuubi.plugin.spark.authz.rule

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SQLExecution.EXECUTION_ID_KEY
import org.apache.spark.sql.execution.command.ExplainCommand

import org.apache.kyuubi.plugin.spark.authz.rule.Authorization._
import org.apache.kyuubi.plugin.spark.authz.rule.permanentview.PermanentViewMarker

abstract class Authorization(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      // ExplainCommand run will execute the plan, should avoid check privilege for the plan.
      case ExplainCommand(_, _) =>
        EXPLAIN_COMMAND_EXECUTION_ID = Some(executionId(spark))
        plan
      case plan if EXPLAIN_COMMAND_EXECUTION_ID.contains(executionId(spark)) => plan
      case plan if isAuthChecked(plan) => plan // do nothing if checked privileges already.
      case p =>
        checkPrivileges(spark, p)
        markAuthChecked(p)
    }
  }

  def checkPrivileges(spark: SparkSession, plan: LogicalPlan): Unit
}

object Authorization {

  val KYUUBI_AUTHZ_TAG = TreeNodeTag[Unit]("__KYUUBI_AUTHZ_TAG")
  var EXPLAIN_COMMAND_EXECUTION_ID: Option[String] = None

  private def markAllNodesAuthChecked(plan: LogicalPlan): LogicalPlan = {
    plan.transformDown { case p =>
      p.setTagValue(KYUUBI_AUTHZ_TAG, ())
      p
    }
  }

  protected def markAuthChecked(plan: LogicalPlan): LogicalPlan = {
    plan.setTagValue(KYUUBI_AUTHZ_TAG, ())
    plan transformDown {
      case pvm: PermanentViewMarker =>
        markAllNodesAuthChecked(pvm)
      case subquery: Subquery =>
        markAllNodesAuthChecked(subquery)
    }
  }

  protected def isAuthChecked(plan: LogicalPlan): Boolean = {
    plan match {
      case subquery: Subquery => isAuthChecked(subquery.child)
      case p => p.getTagValue(KYUUBI_AUTHZ_TAG).nonEmpty
    }
  }

  private def executionId(sparkSession: SparkSession): String = {
    sparkSession.sparkContext.getLocalProperty(EXECUTION_ID_KEY)
  }
}
