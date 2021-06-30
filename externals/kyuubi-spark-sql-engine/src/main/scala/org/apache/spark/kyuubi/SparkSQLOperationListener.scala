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

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.spark.operation.ExecuteStatement

import org.apache.spark.scheduler._


class SparkSQLOperationListener(var operation: ExecuteStatement)
  extends SparkListener with Logging {

  /**
   * In this function, we can get this job's start-time
   *
   * @param jobStart
   */
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {

    val jobId = jobStart.jobId
    val seq = jobStart.stageIds

  }

  /**
   * In this function, we can get this job's finish-time
   *
   * @param jobEnd
   */
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {

    val jobId = jobEnd.jobId
    val jobFinishTime = jobEnd.time

  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {

    val stageInfo = stageSubmitted.stageInfo
  }

  override def onStageExecutorMetrics(executorMetrics: SparkListenerStageExecutorMetrics): Unit = {

    val execId = executorMetrics.execId
    val stageAttemptId = executorMetrics.stageAttemptId
    val stageId = executorMetrics.stageId
    val metrics = executorMetrics.executorMetrics
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {

    val info = stageCompleted.stageInfo
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {

    val info = executorAdded.executorInfo
  }

  override def onExecutorMetricsUpdate(
                                        executorMetricsUpdate: SparkListenerExecutorMetricsUpdate
                                      ): Unit = {

    val execId = executorMetricsUpdate.execId
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {

    val executorId = executorRemoved.executorId
  }
}