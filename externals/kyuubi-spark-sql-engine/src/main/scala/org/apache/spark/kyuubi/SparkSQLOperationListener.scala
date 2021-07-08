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

package org.apache.spark.kyuubi

import org.apache.spark.SparkContext.SPARK_JOB_GROUP_ID
import org.apache.spark.scheduler.{SparkListenerEvent, SparkListenerJobStart, StatsReportListener}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}

class SparkSQLOperationListener extends StatsReportListener{

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case sqlExecutionStart: SparkListenerSQLExecutionStart =>
        // TODO: SparkPlanInfo need to get
        val executionId = sqlExecutionStart.executionId
        val startTime = sqlExecutionStart.time
        val physicalPlan = sqlExecutionStart.physicalPlanDescription
        // Store this SQL's physicalPlan
        SparkSQLMetrics.addPhysicalPlanForExecutionId(executionId, physicalPlan)
        // Store this SQL's state and the time
        SparkSQLMetrics.addStartTimeForExecutionId(executionId, startTime)
      case sqlExecutionEnd: SparkListenerSQLExecutionEnd =>
        val executionId = sqlExecutionEnd.executionId
        val finishTime = sqlExecutionEnd.time
        SparkSQLMetrics.addFinishTimeForExecutionId(executionId, finishTime)
      case _ =>
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    // TODO: Some metrics need to get: stageIds, stageInfos, this job's startTime and so on
    val operationId = jobStart.properties.getProperty(SPARK_JOB_GROUP_ID)
    val executionId = jobStart.properties.getProperty("spark.sql.execution.id").toLong
    // Add executionId and physicalPlan into kStatement
    // It only needs to be executed once
    if (!SparkSQLMetrics.operationIsExist(executionId)) {
      SparkSQLMetrics.addExecutionInfoIntoKs(executionId, operationId)
    }
  }
}
