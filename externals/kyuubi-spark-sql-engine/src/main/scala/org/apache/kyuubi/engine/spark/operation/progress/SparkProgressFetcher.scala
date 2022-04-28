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

import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.operation.JobProgressUpdate

class SparkProgressFetcher(spark: SparkSession, jobGroup: String) {

  val statusTracker = spark.sparkContext.statusTracker

  def getJobProgressUpdate(startTime: Long): JobProgressUpdate = {
    val progressMap = getProgressMap
    val progressMonitor = new SparkProgressMonitor(progressMap, startTime)
    JobProgressUpdate(
      progressMonitor.headers,
      progressMonitor.rows,
      progressMonitor.footerSummary,
      progressMonitor.progressedPercentage,
      progressMonitor.startTime,
      progressMonitor.executionStatus)
  }

  private def getProgressMap(): Map[SparkStage, SparkStageProgress] = {
    val jobs = statusTracker.getJobIdsForGroup(jobGroup)
      .flatMap(statusTracker.getJobInfo(_))
    val stages = jobs.flatMap(_.stageIds()).flatMap(statusTracker.getStageInfo(_))
    stages.map(stage => {
      val sparkStage = SparkStage(stage.stageId(), stage.currentAttemptId())
      val sparkStageProgress = SparkStageProgress(
        stage.numTasks,
        stage.numCompletedTasks,
        stage.numActiveTasks,
        stage.numFailedTasks)
      (sparkStage, sparkStageProgress)
    }).toMap
  }

}
