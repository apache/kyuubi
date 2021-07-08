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

import java.util.Properties

import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.operation.Operation
import org.apache.kyuubi.operation.log.OperationLog

/**
 * A [[SparkListener]] based on spark's DeveloperApi [[StatsReportListener]], used to appending
 * statement/operation level logs to the OperationLog which belong to the same `spark.jobGroup.id`
 *
 * @param operation the corresponding operation
 */
class SQLOperationListener(
    operation: Operation, spark: SparkSession) extends StatsReportListener with Logging {

  private val operationId: String = operation.getHandle.identifier.toString
  private val activeJobs = new java.util.HashSet[Int]()
  private val activeStages = new java.util.HashSet[Int]()
  private var executionId: Option[Long] = None

  // For broadcast, Spark will introduce a new runId as SPARK_JOB_GROUP_ID, see:
  // https://github.com/apache/spark/pull/24595, So we will miss these logs.
  // TODO: Fix this until the below ticket resolved
  // https://issues.apache.org/jira/browse/SPARK-34064
  private def sameGroupId(properties: Properties): Boolean = {
    properties != null && properties.getProperty("kyuubi.statement.id") == operationId
  }

  private def withOperationLog(f : => Unit): Unit = {
    try {
      operation.getOperationLog.foreach(OperationLog.setCurrentOperationLog)
      f
    } finally {
      OperationLog.removeCurrentOperationLog()
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = activeJobs.synchronized {
    if (sameGroupId(jobStart.properties)) {
      val jobId = jobStart.jobId
      val stageSize = jobStart.stageInfos.size
      if (executionId.isEmpty) {
        executionId = Option(jobStart.properties.getProperty("spark.sql.execution.id"))
          .map(_.toLong)
      }
      withOperationLog {
        activeJobs.add(jobId)
        info(s"Query [$operationId]: Job $jobId started with $stageSize stages," +
          s" ${activeJobs.size()} active jobs running")
      }
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = activeJobs.synchronized {
    val jobId = jobEnd.jobId
    if (activeJobs.remove(jobId)) {
      val hint = jobEnd.jobResult match {
        case JobSucceeded => "succeeded"
        case _ => "failed" // TODO: Handle JobFailed(exception: Exception)
      }
      withOperationLog {
        info(s"Query [$operationId]: Job $jobId $hint, ${activeJobs.size()} active jobs running")
      }
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    activeStages.synchronized {
      if (sameGroupId(stageSubmitted.properties)) {
        val stageInfo = stageSubmitted.stageInfo
        val stageId = stageInfo.stageId
        activeStages.add(stageId)
        withOperationLog {
          info(s"Query [$operationId]: Stage $stageId started with ${stageInfo.numTasks} tasks," +
            s" ${activeStages.size()} active stages running")
        }
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val stageId = stageInfo.stageId
    activeStages.synchronized {
      if (activeStages.remove(stageId)) {
        withOperationLog(super.onStageCompleted(stageCompleted))
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = activeStages.synchronized {
    if (activeStages.contains(taskEnd.stageId)) super.onTaskEnd(taskEnd)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case sqlExecutionEnd: SparkListenerSQLExecutionEnd if
          executionId.contains(sqlExecutionEnd.executionId) =>
        spark.sparkContext.removeSparkListener(this)
      case _ =>
    }
  }
}
