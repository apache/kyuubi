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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SQLExecution.EXECUTION_ID_KEY

import org.apache.kyuubi.plugin.spark.authz.rule.Authorization._
import org.apache.kyuubi.plugin.spark.authz.util.ReservedKeys._

abstract class Authorization(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
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

  def markAllNodesAuthChecked(plan: LogicalPlan): LogicalPlan = {
    plan.transformDown { case p =>
      p.setTagValue(KYUUBI_AUTHZ_TAG, ())
      p
    }
  }

  def markAuthChecked(plan: LogicalPlan): LogicalPlan = {
    plan.setTagValue(KYUUBI_AUTHZ_TAG, ())
    plan
  }

  protected def isAuthChecked(plan: LogicalPlan): Boolean = {
    plan.getTagValue(KYUUBI_AUTHZ_TAG).nonEmpty
  }

  def setExplainCommandExecutionId(sparkSession: SparkSession): Unit = {
    sparkSession.sparkContext.setLocalProperty(
      KYUUBI_EXPLAIN_COMMAND_EXECUTION_ID,
      executionId(sparkSession))
  }

  def isExplainCommandChild(sparkSession: SparkSession): Boolean = {
    if (null == executionId(sparkSession)) {
      false
    } else {
      executionId(sparkSession).equals(
        sparkSession.sparkContext.getLocalProperty(KYUUBI_EXPLAIN_COMMAND_EXECUTION_ID))
    }
  }

  private def executionId(sparkSession: SparkSession): String = {
    sparkSession.sparkContext.getLocalProperty(EXECUTION_ID_KEY)
  }
}
