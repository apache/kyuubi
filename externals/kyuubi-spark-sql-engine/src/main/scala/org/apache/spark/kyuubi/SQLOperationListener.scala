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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
import org.apache.spark.ui.UIUtils.formatDuration

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SPARK_SHOW_PROGRESS, ENGINE_SPARK_SHOW_PROGRESS_TIME_FORMAT, ENGINE_SPARK_SHOW_PROGRESS_UPDATE_INTERVAL}
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_STATEMENT_ID_KEY
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.{getSessionConf, SPARK_SQL_EXECUTION_ID_KEY}
import org.apache.kyuubi.engine.spark.operation.ExecuteStatement
import org.apache.kyuubi.operation.Operation

/**
 * A [[SparkListener]] based on spark's DeveloperApi [[StatsReportListener]], used to appending
 * statement/operation level logs to the OperationLog which belong to the same `spark.jobGroup.id`
 *
 * @param operation the corresponding operation
 */
class SQLOperationListener(
    operation: Operation,
    spark: SparkSession) extends StatsReportListener with Logging {

  private val operationId: String = operation.getHandle.identifier.toString
  private lazy val activeJobs = new ConcurrentHashMap[Int, SparkJobInfo]()
  private lazy val activeStages = new ConcurrentHashMap[SparkStageAttempt, SparkStageInfo]()
  private var executionId: Option[Long] = None

  private lazy val consoleProgressBar =
    if (getSessionConf(ENGINE_SPARK_SHOW_PROGRESS, spark)) {
      Some(new SparkConsoleProgressBar(
        operation,
        activeJobs,
        activeStages,
        getSessionConf(ENGINE_SPARK_SHOW_PROGRESS_UPDATE_INTERVAL, spark),
        getSessionConf(ENGINE_SPARK_SHOW_PROGRESS_TIME_FORMAT, spark)))
    } else {
      None
    }

  private val operationRunTime = new AtomicLong(0)
  private val operationCpuTime = new AtomicLong(0)

  def getOperationRunTime: Long = operationRunTime.get()

  def getOperationCpuTime: Long = operationCpuTime.get()

  def getExecutionId: Option[Long] = executionId

  // Prior SPARK-43952 (3.5.0), broadcast jobs uses a different group id, so we will
  // miss those logs. See more details in SPARK-20774 (3.0.0)
  private def sameGroupId(properties: Properties): Boolean = {
    properties != null && properties.getProperty(KYUUBI_STATEMENT_ID_KEY) == operationId
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    if (sameGroupId(jobStart.properties)) {
      val jobId = jobStart.jobId
      val stageIds = jobStart.stageInfos.map(_.stageId).toSet
      val stageSize = jobStart.stageInfos.size
      if (executionId.isEmpty) {
        executionId = Option(jobStart.properties.getProperty(SPARK_SQL_EXECUTION_ID_KEY))
          .map(_.toLong)
        consoleProgressBar
        operation match {
          case executeStatement: ExecuteStatement =>
            executeStatement.setCompiledStateIfNeeded()
          case _ =>
        }
      }
      activeJobs.put(
        jobId,
        new SparkJobInfo(stageSize, stageIds))
      operation.withOperationLog {
        info(s"Query [$operationId]: Job $jobId started with $stageSize stages," +
          s" ${activeJobs.size()} active jobs running")
      }
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobId = jobEnd.jobId
    if (activeJobs.remove(jobId) != null) {
      val hint = jobEnd.jobResult match {
        case JobSucceeded => "succeeded"
        case _ => "failed" // TODO: Handle JobFailed(exception: Exception)
      }
      operation.withOperationLog {
        info(s"Query [$operationId]: Job $jobId $hint, ${activeJobs.size()} active jobs running")
      }
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    activeStages.synchronized {
      if (sameGroupId(stageSubmitted.properties)) {
        val stageInfo = stageSubmitted.stageInfo
        val stageId = stageInfo.stageId
        val attemptNumber = stageInfo.attemptNumber()
        val stageAttempt = SparkStageAttempt(stageId, attemptNumber)
        activeStages.put(
          stageAttempt,
          new SparkStageInfo(stageId, stageInfo.numTasks))
        operation.withOperationLog {
          info(s"Query [$operationId]: Stage $stageId.$attemptNumber started " +
            s"with ${stageInfo.numTasks} tasks, ${activeStages.size()} active stages running")
        }
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val stageId = stageInfo.stageId
    val stageAttempt = SparkStageAttempt(stageInfo.stageId, stageInfo.attemptNumber())
    activeStages.synchronized {
      if (activeStages.remove(stageAttempt) != null) {
        stageInfo.getStatusString match {
          case "succeeded" =>
            activeJobs.asScala.foreach { case (_, jobInfo) =>
              if (jobInfo.stageIds.contains(stageId)) {
                jobInfo.numCompleteStages.getAndIncrement()
              }
            }
          case _ => // do nothing, failed stage not counted to numCompleteStages
        }
        val taskMetrics = stageInfo.taskMetrics
        if (taskMetrics != null) {
          info(s"stageId=${stageCompleted.stageInfo.stageId}, " +
            s"stageRunTime=${formatDuration(taskMetrics.executorRunTime)}, " +
            s"stageCpuTime=${formatDuration(taskMetrics.executorCpuTime / 1000000)}")
          operationRunTime.getAndAdd(taskMetrics.executorRunTime)
          operationCpuTime.getAndAdd(taskMetrics.executorCpuTime)
        }
        operation.withOperationLog(super.onStageCompleted(stageCompleted))
      }
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = activeStages.synchronized {
    val stageAttempt = SparkStageAttempt(taskStart.stageId, taskStart.stageAttemptId)
    if (activeStages.containsKey(stageAttempt)) {
      activeStages.get(stageAttempt).numActiveTasks.getAndIncrement()
      super.onTaskStart(taskStart)
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = activeStages.synchronized {
    val stageAttempt = SparkStageAttempt(taskEnd.stageId, taskEnd.stageAttemptId)
    if (activeStages.containsKey(stageAttempt)) {
      activeStages.get(stageAttempt).numActiveTasks.getAndDecrement()
      if (taskEnd.reason == org.apache.spark.Success) {
        activeStages.get(stageAttempt).numCompleteTasks.getAndIncrement()
      }
      super.onTaskEnd(taskEnd)
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case sqlExecutionEnd: SparkListenerSQLExecutionEnd
          if executionId.contains(sqlExecutionEnd.executionId) =>
        cleanup()
      case _ =>
    }
  }

  def cleanup(): Unit = {
    spark.sparkContext.removeSparkListener(this)
    if (executionId.isDefined) {
      consoleProgressBar.foreach(_.finish())
    }
  }
}
