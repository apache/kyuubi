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

package org.apache.kyuubi.engine.spark.monitor

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.scheduler.SparkListenerJobEnd

import org.apache.kyuubi.engine.spark.monitor.entity.{KyuubiJobInfo, KyuubiStatementInfo}

object KyuubiStatementMonitor {

  // Key is statementId
  private val operationStatementMap = new ConcurrentHashMap[String, KyuubiStatementInfo]()
  // First key is statementId, second key is jobId.
  // From this map. you can get all jobs info by statementId.
  // The second map is used for saving jobEndInfo into KyuubiJobInfo by jobId.
  private val operationJobsMap = new ConcurrentHashMap[String, ConcurrentHashMap[Int, KyuubiJobInfo]]()

  // Store the relationship between jobId and operationId
  // We should remove the data when this job was ended
  private val jobOperationMap = new ConcurrentHashMap[Int, String]()

  def addStatementDetailForOperationId(
      operationId: String, kyuubiStatementInfo: KyuubiStatementInfo): Unit = {
    operationStatementMap.putIfAbsent(operationId, kyuubiStatementInfo)
  }

  def addJobInfoForOperationId(operationId: String, kyuubiJobInfo: KyuubiJobInfo): Unit = {
    if (operationJobsMap.get(operationId) == null) {
      operationJobsMap.putIfAbsent(operationId, new ConcurrentHashMap[Int, KyuubiJobInfo]())
    }
    operationJobsMap.get(operationId).putIfAbsent(kyuubiJobInfo.jobId, kyuubiJobInfo)
    jobOperationMap.put(kyuubiJobInfo.jobId, operationId)
  }

  def addJobEndInfo(jobEnd: SparkListenerJobEnd): Unit = {
    val jobId = jobEnd.jobId
    val operationId = jobOperationMap.remove(jobId)
    if (!operationId.isEmpty) {
      operationJobsMap.get(operationId).get(jobId).endTime = Option(jobEnd.time)
      operationJobsMap.get(operationId).get(jobId).jobResult = jobEnd.jobResult
    }
  }
}
