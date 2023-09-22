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

package org.apache.spark.sql.execution.metric

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.kyuubi.SparkDatasetHelper

import org.apache.kyuubi.engine.spark.WithSparkSQLEngine

trait SparkMetricsTestUtils {
  this: WithSparkSQLEngine =>

  private lazy val statusStore = spark.sharedState.statusStore
  private def currentExecutionIds(): Set[Long] = {
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)
    statusStore.executionsList.map(_.executionId).toSet
  }

  protected def getSparkPlanMetrics(df: DataFrame): Map[Long, (String, Map[String, Any])] = {
    val previousExecutionIds = currentExecutionIds()
    SparkDatasetHelper.executeCollect(df)
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)
    val executionIds = currentExecutionIds().diff(previousExecutionIds)
    assert(executionIds.size === 1)
    val executionId = executionIds.head
    val metricValues = statusStore.executionMetrics(executionId)
    SparkPlanGraph(SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan)).allNodes
      .map { node =>
        val nodeMetrics = node.metrics.map { metric =>
          val metricValue = metricValues(metric.accumulatorId)
          (metric.name, metricValue)
        }.toMap
        (node.id, node.name -> nodeMetrics)
      }.toMap
  }
}
