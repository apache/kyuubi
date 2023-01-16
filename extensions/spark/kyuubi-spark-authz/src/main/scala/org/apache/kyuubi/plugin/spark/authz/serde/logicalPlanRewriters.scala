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

package org.apache.kyuubi.plugin.spark.authz.serde

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand

import org.apache.kyuubi.plugin.spark.authz.ranger.{FilteredShowColumnsCommand, FilteredShowFunctionsCommand, FilteredShowTablesCommand}

trait LogicalPlanRewriter extends Rewriter {

  def rewrite(spark: SparkSession, plan: LogicalPlan): LogicalPlan
}

object LogicalPlanRewriter {
  val logicalPlanRewriters: Map[String, LogicalPlanRewriter] =
    loadRewritersToMap[LogicalPlanRewriter]
}

class ShowTablesCommandRewriter extends LogicalPlanRewriter {

  override def rewrite(spark: SparkSession, plan: LogicalPlan): LogicalPlan = {
    FilteredShowTablesCommand(plan.asInstanceOf[RunnableCommand])
  }
}

class ShowFunctionsCommandRewriter extends LogicalPlanRewriter {

  override def rewrite(spark: SparkSession, plan: LogicalPlan): LogicalPlan = {
    FilteredShowFunctionsCommand(plan.asInstanceOf[RunnableCommand])
  }
}

class ShowColumnsCommandRewriter extends LogicalPlanRewriter {

  override def rewrite(spark: SparkSession, plan: LogicalPlan): LogicalPlan = {
    FilteredShowColumnsCommand(plan.asInstanceOf[RunnableCommand])
  }
}
