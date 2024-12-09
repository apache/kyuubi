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
package org.apache.kyuubi.engine.trino

import java.util

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap

import io.trino.client.{StageStats, StatementClient}

import org.apache.kyuubi.engine.trino.TrinoProgressMonitor.{COLUMN_1_WIDTH, HEADERS}
import org.apache.kyuubi.engine.trino.operation.progress.{TrinoStage, TrinoStageProgress}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TJobExecutionStatus

class TrinoProgressMonitor(trino: StatementClient) {

  private lazy val progressMap: Map[TrinoStage, TrinoStageProgress] = {
    if (trino != null) {
      val trinoStats = trino.getStats
      val stageQueue = scala.collection.mutable.Queue[StageStats]()
      val stages = scala.collection.mutable.ListBuffer[(TrinoStage, TrinoStageProgress)]()
      val rootStage = trinoStats.getRootStage
      if (rootStage != null) {
        stageQueue.enqueue(rootStage)
      }
      while (stageQueue.nonEmpty) {
        val stage = stageQueue.dequeue()
        val stageId = stage.getStageId
        val stageProgress = TrinoStageProgress(
          stage.getState,
          stage.getTotalSplits,
          stage.getCompletedSplits,
          stage.getRunningSplits,
          stage.getFailedTasks)
        stages.append((TrinoStage(stageId), stageProgress))
        val subStages = asScalaBuffer(stage.getSubStages)
        stageQueue ++= subStages.toSeq
      }
      SortedMap(stages.toSeq: _*)
    } else {
      SortedMap()
    }
  }

  def headers: util.List[String] = HEADERS

  def rows: util.List[util.List[String]] = {
    val progressRows = progressMap.map {
      case (stage, progress) =>
        val complete = progress.completedSplits
        val total = progress.totalSplits
        val running = progress.runningSplits
        val failed = progress.failedTasks
        val stageName = "Stage-" + stage.stageId
        val nameWithProgress = getNameWithProgress(stageName, complete, total)
        val pending = total - complete - running
        util.Arrays.asList(
          nameWithProgress,
          progress.state,
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
    if (trino != null && trino.getStats != null) {
      val progressPercentage = trino.getStats.getProgressPercentage
      progressPercentage.orElse(0.0d)
    } else {
      0.0d
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
      val complete = progress.completedSplits
      val total = progress.totalSplits
      if (total > 0 && complete == total) completed += 1
    }
    completed
  }

}

object TrinoProgressMonitor {

  private val HEADERS: util.List[String] = util.Arrays.asList(
    "STAGES",
    "STATUS",
    "TOTAL",
    "COMPLETED",
    "RUNNING",
    "PENDING",
    "FAILED",
    "")

  private val COLUMN_1_WIDTH = 16

}
