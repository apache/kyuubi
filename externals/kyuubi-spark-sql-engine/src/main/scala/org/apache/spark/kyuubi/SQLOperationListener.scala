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

import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SPARK_SHOW_PROGRESS, ENGINE_SPARK_SHOW_PROGRESS_TIME_FORMAT, ENGINE_SPARK_SHOW_PROGRESS_UPDATE_INTERVAL}
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_STATEMENT_ID_KEY
import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.SPARK_SQL_EXECUTION_ID_KEY
import org.apache.kyuubi.engine.spark.operation.ExecuteStatement
import org.apache.kyuubi.operation.Operation
import org.apache.kyuubi.operation.log.OperationLog

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
  private lazy val activeJobs = new java.util.HashSet[Int]()
  private lazy val activeStages = new ConcurrentHashMap[StageAttempt, StageInfo]()
  private var executionId: Option[Long] = None

  private val conf: KyuubiConf = operation.getSession.sessionManager.getConf
  private lazy val consoleProgressBar =
    if (conf.get(ENGINE_SPARK_SHOW_PROGRESS)) {
      Some(new SparkConsoleProgressBar(
        operation,
        activeStages,
        conf.get(ENGINE_SPARK_SHOW_PROGRESS_UPDATE_INTERVAL),
        conf.get(ENGINE_SPARK_SHOW_PROGRESS_TIME_FORMAT)))
    } else {
      None
    }

  def getExecutionId: Option[Long] = executionId

  // For broadcast, Spark will introduce a new runId as SPARK_JOB_GROUP_ID, see:
  // https://github.com/apache/spark/pull/24595, So we will miss these logs.
  // TODO: Fix this until the below ticket resolved
  // https://issues.apache.org/jira/browse/SPARK-34064
  private def sameGroupId(properties: Properties): Boolean = {
    properties != null && properties.getProperty(KYUUBI_STATEMENT_ID_KEY) == operationId
  }

  private def withOperationLog(f: => Unit): Unit = {
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
        executionId = Option(jobStart.properties.getProperty(SPARK_SQL_EXECUTION_ID_KEY))
          .map(_.toLong)
        consoleProgressBar
        operation match {
          case executeStatement: ExecuteStatement =>
            executeStatement.setCompiledStateIfNeeded()
          case _ =>
        }
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
        val attemptNumber = stageInfo.attemptNumber()
        val stageAttempt = StageAttempt(stageId, attemptNumber)
        activeStages.put(
          stageAttempt,
          new StageInfo(stageId, stageInfo.numTasks))
        withOperationLog {
          info(s"Query [$operationId]: Stage $stageId.$attemptNumber started " +
            s"with ${stageInfo.numTasks} tasks, ${activeStages.size()} active stages running")
        }
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val stageAttempt = StageAttempt(stageInfo.stageId, stageInfo.attemptNumber())
    activeStages.synchronized {
      if (activeStages.remove(stageAttempt) != null) {
        withOperationLog(super.onStageCompleted(stageCompleted))
      }
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = activeStages.synchronized {
    val stageAttempt = StageAttempt(taskStart.stageId, taskStart.stageAttemptId)
    if (activeStages.containsKey(stageAttempt)) {
      activeStages.get(stageAttempt).numActiveTasks += 1
      super.onTaskStart(taskStart)
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = activeStages.synchronized {
    val stageAttempt = StageAttempt(taskEnd.stageId, taskEnd.stageAttemptId)
    if (activeStages.containsKey(stageAttempt)) {
      activeStages.get(stageAttempt).numActiveTasks -= 1
      if (taskEnd.reason == org.apache.spark.Success) {
        activeStages.get(stageAttempt).numCompleteTasks += 1
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
