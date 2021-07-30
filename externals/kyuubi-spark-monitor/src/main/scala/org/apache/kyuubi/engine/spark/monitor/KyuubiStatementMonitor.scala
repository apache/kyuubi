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

import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.monitor.entity.{KyuubiJobInfo, KyuubiStatementInfo}

// TODO: Thread Safe need to consider
object KyuubiStatementMonitor extends Logging{

  private val classToStructTyeMap = new java.util.HashMap[Class[_], StructType]()

  import org.apache.kyuubi.config.KyuubiConf._

  private val sparkSession = SparkSession.active
  private val kyuubiConf: KyuubiConf = KyuubiConf()
  private val maxCapacity = kyuubiConf.get(SPARK_MONITOR_STATEMENTINFO_QUEUE_CAPACITY)
  private val timeThreshold = kyuubiConf.get(SPARK_MONITOR_TIME_THRESHOLD)  // seconds
  private val dumpEnable = kyuubiConf.get(SPARK_MONITOR_DUMP_LOCAL_ENABLE)
  private val dumpLocalDir = kyuubiConf.get(SPARK_MONITOR_DUMP_LOCAL_DIR) match {
    case null =>
      if (Utils.isTesting) {
        "file://" + System.getProperty("user.dir") + "/target/dump_data"
      } else {
        "file://" + System.getProperty("user.dir") + "/dump_data"
      }
    case dir: String =>
      "file://" + dir
  }

  /**
   * The max size that trigger to dump data is 0.7*maxCapacity
   */
  private val maxSize: Int = scala.math.floor(0.7*maxCapacity).intValue()

  if (dumpEnable) {
    val dumpDataByTimeThresholdThreadPool = new ScheduledThreadPoolExecutor(1)
    dumpDataByTimeThresholdThreadPool.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        dumpStatementInfoFromQueue()
      }
    }, 60, timeThreshold, TimeUnit.SECONDS)
  }

  /**
   * This blockingQueue store kyuubiStatementInfo.
   *
   * Notice:
   *    1. There have two kinds of threshold to trigger when to remove items from this queue:
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
  private val kyuubiJobIdToJobInfoMap = new ConcurrentHashMap[Int, KyuubiJobInfo](10)

  /**
   * This method is used for putting kyuubiStatementInfo into blockingQueue(statementQueue).
   * Every time we put an item into this queue, we should judge this queue's current size at first.
   * If the size is less than threshold, we need to remove items from this queue.
   *
   * @param kyuubiStatementInfo
   */
  // TODO: Lack size type threshold and time type threshold
  def putStatementInfoIntoQueue(kyuubiStatementInfo: KyuubiStatementInfo): Unit = {
    if (dumpEnable) {
      if (kyuubiStatementQueue.size() >= maxSize) {
        dumpStatementInfoFromQueue()
      }
      if (!kyuubiStatementQueue.contains(kyuubiStatementInfo)) {
        val isSuccess = kyuubiStatementQueue.add(kyuubiStatementInfo)
        info(s"Add kyuubiStatementInfo into queue is [$isSuccess], " +
          s"statementId is [${kyuubiStatementInfo.statementId}]")
      }
    }
  }

  /**
   * This method is used for dumpping kyuubiStatementInfo to a file by threshold.
   */
  private def dumpStatementInfoFromQueue(): Unit = kyuubiStatementQueue.synchronized {
    var statementInfoList: List[KyuubiStatementInfo] = List()
    val size = kyuubiStatementQueue.size()
    (0 to size-1).foreach { _ =>
      statementInfoList = kyuubiStatementQueue.poll() :: statementInfoList
    }
    var schema = classToStructTyeMap.get(classOf[KyuubiStatementInfo])
    if (schema == null) {
      schema = ScalaReflection.schemaFor[KyuubiStatementInfo].dataType.asInstanceOf[StructType]
      classToStructTyeMap.put(classOf[KyuubiStatementInfo], schema)
    }
    val rowList = new java.util.ArrayList[Row]()
    for (kyuubiStatementInfo <- statementInfoList) {
      rowList.add(Row.fromTuple(kyuubiStatementInfo))
    }
    sparkSession.createDataFrame(rowList, schema)
      .write.mode(SaveMode.Append)
      .format("parquet")
      .save(dumpLocalDir + "/statement_info.parquet")
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
    if (kyuubiJobIdToJobInfoMap.size() >= 7) {
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
  def insertJobEndTimeAndResult(jobEnd: SparkListenerJobEnd): Unit = {
    val jobInfo = kyuubiJobIdToJobInfoMap.get(jobEnd.jobId)
    if (jobInfo != null) {
      jobInfo.endTime = jobEnd.time
      jobInfo.jobResult = jobEnd.jobResult
      debug(s"Job finished. Query [${jobInfo.statementId}]: JobId is [${jobInfo.jobId}]")
    } else {
      warn(s"JobStartEvent is lost. JobId is [${jobInfo.jobId}]")
    }
  }
}
