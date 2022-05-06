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
package org.apache.kyuubi.engine.spark.operation.progress

import java.util

import scala.collection.JavaConverters._

import org.apache.kyuubi.engine.spark.operation.progress.SparkProgressMonitor._
import org.apache.kyuubi.operation.OperationProgressStatus

class SparkProgressMonitor(progressMap: Map[SparkStage, SparkStageProgress]) {

  def headers: util.List[String] = HEADERS

  def rows: util.List[util.List[String]] = {
    val progressRows = progressMap.toSeq.sortBy(_._1).map {
      case (stage, progress) =>
        val complete = progress.completedTasksCount
        val total = progress.totalTaskCount
        val running = progress.runningTaskCount
        val failed = progress.failedTaskCount
        var state =
          if (total > 0) {
            OperationProgressStatus.PENDING
          } else {
            OperationProgressStatus.FINISHED
          }
        if (complete > 0 || running > 0 || failed > 0) {
          state =
            if (complete < total) {
              OperationProgressStatus.RUNNING
            } else {
              OperationProgressStatus.FINISHED
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
          String.valueOf(failed))
    }.toList.asJavaCollection
    new util.ArrayList[util.List[String]](progressRows)
  }

  def footerSummary: String = {
    "STAGES: %02d/%02d".format(getCompletedStages, progressMap.keySet.size)
  }

  def progressedPercentage: Double = {
    var sumTotal = 0
    var sumComplete = 0
    progressMap.toSeq.sortBy(_._1).map(_._2).foreach { progress =>
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

  def executionStatus: String =
    if (getCompletedStages == progressMap.keySet.size) {
      OperationProgressStatus.FINISHED.toString
    } else {
      OperationProgressStatus.RUNNING.toString
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
    val result = new StringBuilder(trimmedVName)
    val toFill = (spaceRemaining * percent).toInt
    for (i <- 0 until toFill) {
      result.append(".")
    }
    result.toString
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
    "FAILED")

  private val COLUMN_1_WIDTH = 16

}
