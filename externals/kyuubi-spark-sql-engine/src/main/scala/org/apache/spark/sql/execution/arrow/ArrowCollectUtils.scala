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

package org.apache.spark.sql.execution.arrow

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.execution.CollectLimitExec

object ArrowCollectUtils extends SQLConfHelper {

  type Batch = (Array[Byte], Long)

  /**
   * Forked from `org.apache.spark.sql.execution.SparkPlan#executeTake()`, the algorithm can be
   * summarized in the following steps:
   * 1. If the limit specified in the CollectLimitExec object is 0, the function returns an empty
   *    array of batches.
   * 2. Otherwise, execute the child query plan of the CollectLimitExec object to obtain an RDD of
   *    data to collect.
   * 3. Use an iterative approach to collect data in batches until the specified limit is reached.
   *    In each iteration, it selects a subset of the partitions of the RDD to scan and tries to
   *    collect data from them.
   * 4. For each partition subset, we use the runJob method of the Spark context to execute a
   *    closure that scans the partition data and converts it to Arrow batches.
   * 5. Check if the collected data reaches the specified limit. If not, it selects another subset
   *    of partitions to scan and repeats the process until the limit is reached or all partitions
   *    have been scanned.
   * 6. Return an array of all the collected Arrow batches.
   *
   * Note that:
   * 1. The returned Arrow batches row count >= limit, if the input df has more than the `limit`
   *    row count
   * 2. We don't implement the `takeFromEnd` logical
   *
   * @return
   */
  def takeAsArrowBatches(
      collectLimitExec: CollectLimitExec,
      maxRecordsPerBatch: Long,
      maxEstimatedBatchSize: Long,
      timeZoneId: String): Array[Batch] = {
    val n = collectLimitExec.limit
    val schema = collectLimitExec.schema
    if (n == 0) {
      return new Array[Batch](0)
    } else {
      val limitScaleUpFactor = Math.max(conf.limitScaleUpFactor, 2)
      // TODO: refactor and reuse the code from RDD's take()
      val childRDD = collectLimitExec.child.execute()
      val buf = new ArrayBuffer[Batch]
      var bufferedRowSize = 0L
      val totalParts = childRDD.partitions.length
      var partsScanned = 0
      while (bufferedRowSize < n && partsScanned < totalParts) {
        // The number of partitions to try in this iteration. It is ok for this number to be
        // greater than totalParts because we actually cap it at totalParts in runJob.
        var numPartsToTry = limitInitialNumPartitions
        if (partsScanned > 0) {
          // If we didn't find any rows after the previous iteration, multiply by
          // limitScaleUpFactor and retry. Otherwise, interpolate the number of partitions we need
          // to try, but overestimate it by 50%. We also cap the estimation in the end.
          if (buf.isEmpty) {
            numPartsToTry = partsScanned * limitScaleUpFactor
          } else {
            val left = n - bufferedRowSize
            // As left > 0, numPartsToTry is always >= 1
            numPartsToTry = Math.ceil(1.5 * left * partsScanned / bufferedRowSize).toInt
            numPartsToTry = Math.min(numPartsToTry, partsScanned * limitScaleUpFactor)
          }
        }

        val partsToScan =
          partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)

        val sc = collectLimitExec.session.sparkContext
        val res = sc.runJob(
          childRDD,
          (it: Iterator[InternalRow]) => {
            val batches = ArrowConvertersHelper.toBatchIterator(
              it,
              schema,
              maxRecordsPerBatch,
              maxEstimatedBatchSize,
              n,
              timeZoneId)
            batches.map(b => b -> batches.rowCountInLastBatch).toArray
          },
          partsToScan)

        var i = 0
        while (bufferedRowSize < n && i < res.length) {
          var j = 0
          val batches = res(i)
          while (j < batches.length && n > bufferedRowSize) {
            val batch = batches(j)
            val (_, batchSize) = batch
            buf += batch
            bufferedRowSize += batchSize
            j += 1
          }
          i += 1
        }
        partsScanned += partsToScan.size
      }

      buf.toArray
    }
  }

  /**
   * Spark introduced the config `spark.sql.limit.initialNumPartitions` since 3.4.0. see SPARK-40211
   */
  def limitInitialNumPartitions: Int = {
    conf.getConfString("spark.sql.limit.initialNumPartitions", "1")
      .toInt
  }
}
