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

import java.util

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap

import org.apache.spark.kyuubi.SparkProgressMonitor._
import org.apache.spark.sql.SparkSession
import org.apache.spark.status.api.v1.StageStatus

import org.apache.kyuubi.engine.spark.operation.progress.{SparkOperationProgressStatus, SparkStage, SparkStageProgress}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TJobExecutionStatus

class SparkProgressMonitor(spark: SparkSession, jobGroup: String) {

  private val statusStore = spark.sparkContext.statusStore

  private lazy val progressMap: Map[SparkStage, SparkStageProgress] = {
    val stages = statusStore.jobsList(null)
      .filter(_.jobGroup == Option(jobGroup))
      .flatMap(_.stageIds)
      .flatMap(stageId => statusStore.asOption(statusStore.lastStageAttempt(stageId)))
      .map(stage => {
        val sparkStage = SparkStage(stage.stageId, stage.attemptId)
        val completedTasksCount =
          if (stage.status == StageStatus.SKIPPED) {
            stage.numTasks
          } else {
            stage.numCompleteTasks
          }
        val sparkStageProgress = SparkStageProgress(
          stage.numTasks,
          completedTasksCount,
          stage.numActiveTasks,
          stage.numFailedTasks)
        (sparkStage, sparkStageProgress)
      })
    SortedMap(stages: _*)
  }

  def headers: util.List[String] = HEADERS

  def rows: util.List[util.List[String]] = {
    val progressRows = progressMap.map {
      case (stage, progress) =>
        val complete = progress.completedTasksCount
        val total = progress.totalTaskCount
        val running = progress.runningTaskCount
        val failed = progress.failedTaskCount
        var state =
          if (total > 0) {
            SparkOperationProgressStatus.PENDING
          } else {
            SparkOperationProgressStatus.FINISHED
          }
        if (complete > 0 || running > 0 || failed > 0) {
          state =
            if (complete < total) {
              SparkOperationProgressStatus.RUNNING
            } else {
              SparkOperationProgressStatus.FINISHED
            }
        }
        val attempt = String.valueOf(stage.attemptId)
        val stageName = "Stage-" + String.valueOf(stage.stageId)
        val nameWithProgress = getNameWithProgress(stageName, complete, total)
        val pending = total - complete - running
        util.Arrays.asList(
          nameWithProgress,
          attempt,
          state.toString,
          String.valueOf(total),
          String.valueOf(complete),
          String.valueOf(running),
          String.valueOf(pending),
          String.valueOf(failed),
          "")
    }.toList.asJavaCollection
    new util.ArrayList[util.List[String]](progressRows)
  }

  def footerSummary: String = {
    "STAGES: %02d/%02d".format(getCompletedStages, progressMap.keySet.size)
  }

  def progressedPercentage: Double = {
    var sumTotal = 0
    var sumComplete = 0
    progressMap.values.foreach { progress =>
      val complete = progress.completedTasksCount
      val total = progress.totalTaskCount
      sumTotal += total
      sumComplete += complete
    }
    if (sumTotal == 0) {
      1.0f
    } else {
      sumComplete.toFloat / sumTotal.toFloat
    }
  }

  def executionStatus: TJobExecutionStatus =
    if (getCompletedStages == progressMap.keySet.size) {
      TJobExecutionStatus.COMPLETE
    } else {
      TJobExecutionStatus.IN_PROGRESS
    }

  private def getNameWithProgress(s: String, complete: Int, total: Int): String = {
    if (s == null) return ""
    val percent =
      if (total == 0) 1.0f
      else complete.toFloat / total.toFloat
    // lets use the remaining space in column 1 as progress bar
    val spaceRemaining = COLUMN_1_WIDTH - s.length - 1
    var trimmedVName = s
    // if the vertex name is longer than column 1 width, trim it down
    if (s.length > COLUMN_1_WIDTH) {
      trimmedVName = s.substring(0, COLUMN_1_WIDTH - 2)
      trimmedVName += ".."
    } else trimmedVName += " "
    val toFill = (spaceRemaining * percent).toInt
    s"$trimmedVName${"." * toFill}"
  }

  private def getCompletedStages: Int = {
    var completed = 0
    progressMap.values.foreach { progress =>
      val complete = progress.completedTasksCount
      val total = progress.totalTaskCount
      if (total > 0 && complete == total) completed += 1
    }
    completed
  }

}

object SparkProgressMonitor {

  private val HEADERS: util.List[String] = util.Arrays.asList(
    "STAGES",
    "ATTEMPT",
    "STATUS",
    "TOTAL",
    "COMPLETED",
    "RUNNING",
    "PENDING",
    "FAILED",
    "")

  private val COLUMN_1_WIDTH = 16

}
