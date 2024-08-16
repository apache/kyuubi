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
package org.apache.kyuubi.engine.spark.operation.planonly

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{CommandExecutionMode, QueryExecution, SparkPlan}

import org.apache.kyuubi.engine.spark.plugin.SparkPlans

class SparkPlansImpl(queryExecution: QueryExecution) extends SparkPlans {
  assert(
    queryExecution.mode == CommandExecutionMode.SKIP,
    "Query execution mode must be SKIP to use PlanOnlyMode.")

  override def parsedPlan: LogicalPlan = queryExecution.logical

  override def analyzedPlan: LogicalPlan = queryExecution.analyzed

  override def optimizedPlan: LogicalPlan = queryExecution.optimizedPlan

  override def sparkPlan: SparkPlan = queryExecution.sparkPlan

  override def executedPlan: SparkPlan = queryExecution.executedPlan

}

object SparkPlansImpl {

  def apply(queryExecution: QueryExecution): SparkPlansImpl = {
    new SparkPlansImpl(queryExecution)
  }

  def apply(spark: SparkSession, parsedPlan: LogicalPlan): SparkPlansImpl = {
    val qe = spark.sessionState.executePlan(parsedPlan, CommandExecutionMode.SKIP)
    apply(qe)
  }
}
