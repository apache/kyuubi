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

import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap}

import org.apache.spark.scheduler.SparkListenerJobEnd

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.spark.monitor.entity.{KyuubiJobInfo, KyuubiStatementInfo}

// TODO: Thread Safe need to consider
object KyuubiStatementMonitor extends Logging{

  // TODO: Just for test. We will remove them in the future
  private val maxCapacity: Int = 10
  private val maxSize: Int = 7

  /**
   * This blockingQueue store kyuubiStatementInfo.
   *
   * Notice:
   *    1. When we remove items from this queue, we should ensure those statements have finished.
   *       If not, we should put them into this queue again.
   *    2. There have two kinds of threshold to trigger when to remove items from this queue:
   *      a. time
   *      b. this queue's current size
   */
  // TODO: Capacity should make configurable
  private val kyuubiStatementQueue = new ArrayBlockingQueue[KyuubiStatementInfo](maxCapacity)

  /**
   * This map store the relationship between jobId and jobInfo.
   * When the job has finished, all we can get is jobId from the object-jobEnd.
   * So we need to maintain a mapping relationship to store endTime and jobResult
   * when this job has finished.
   *
   * Key is jobId, value is KyuubiJobInfo.
   *
   * Notice:
   *    1. There have two kinds of threshold to trigger when to remove and dump items from this map:
   *      a. time
   *      b. this map's current size
   */
  // TODO: Capacity should make configurable
  private val kyuubiJobIdToJobInfoMap = new ConcurrentHashMap[Int, KyuubiJobInfo](maxCapacity)

  /**
   * This method is used for putting kyuubiStatementInfo into blockingQueue(statementQueue).
   * Every time we put an item into this queue, we should judge this queue's current size at first.
   * If the size is less than threshold, we need to remove items from this queue.
   *
   * @param kyuubiStatementInfo
   */
  // TODO: Lack size type threshold and time type threshold
  def putStatementInfoIntoQueue(kyuubiStatementInfo: KyuubiStatementInfo): Unit = {
    if (kyuubiStatementQueue.size() >= maxSize) {
      removeAndDumpStatementInfoFromQueue()
    }
    val isSuccess = kyuubiStatementQueue.add(kyuubiStatementInfo)
    info(s"Add kyuubiStatementInfo into queue is [$isSuccess], " +
      s"statementId is [${kyuubiStatementInfo.statementId}]")
  }

  /**
   * This method is used for removing kyuubiStatementInfo from blockingQueue(statementQueue)
   * and dumpping them to a file by threshold.
   */
  // TODO: Need ensure those items have finished. If not, we should put them into this queue again.
  private def removeAndDumpStatementInfoFromQueue(): Unit = {
    // TODO: Just for test
    kyuubiStatementQueue.clear()
  }

  /**
   * This method is used for putting kyuubiJobInfo into hashMap(kyuubiJobIdToJobInfoMap)
   * and storing the mapping relationship between jobId and jobInfo.
   * The reason that we need to maintain a mapping relationship
   * is we need to store endTime and jobResult
   * when this job has finished but the object-jobEnd has nothing but jobId.
   *
   * @param kyuubiJobInfo
   */
  // TODO: Lack size type threshold and time type threshold
  def putJobInfoIntoMap(kyuubiJobInfo: KyuubiJobInfo): Unit = {
    if (kyuubiJobIdToJobInfoMap.size() >= maxSize) {
      removeAndDumpJobInfoFromMap()
    }
    // Put kyuubiJobInfo into kyuubiJobIdToJobInfoMap
    kyuubiJobIdToJobInfoMap.put(kyuubiJobInfo.jobId, kyuubiJobInfo)
  }

  /**
   * This method is used for removing kyuubiJobInfo from hashMap(kyuubiJobIdToJobInfoMap)
   * and dumpping them to a file by threshold.
   */
  private def removeAndDumpJobInfoFromMap(): Unit = {
    // TODO: Just for test
    kyuubiJobIdToJobInfoMap.clear()
  }

  /**
   * This method is used for inserting endTime and jobResult.
   * Those fields can only get when this job has finished.
   *
   * Notice:
   *    1. endTime and jobResult should consider the thread safe.
   *
   * @param jobEnd
   */
  def insertEndTimeAndJobResult(jobEnd: SparkListenerJobEnd): Unit = {
    val jobInfo = kyuubiJobIdToJobInfoMap.get(jobEnd.jobId)
    synchronized(jobInfo) {
      jobInfo.endTime = jobEnd.time
      jobInfo.jobResult = jobEnd.jobResult
    }
    info(s"Job finished. Query [${jobInfo.statementId}]: JobId is [${jobInfo.jobId}]")
  }
}
