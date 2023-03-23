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

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.execution.CollectLimitExec
import org.apache.spark.sql.types.StructType

object ArrowCollectLimitExec extends SQLConfHelper {

  type Batch = (Array[Byte], Long)

  def takeAsArrowBatches(
      collectLimitExec: CollectLimitExec,
      schema: StructType,
      maxRecordsPerBatch: Long,
      maxEstimatedBatchSize: Long,
      timeZoneId: String): Array[Batch] = {
    val n = collectLimitExec.limit
    // TODO
    val takeFromEnd = false
    if (n == 0) {
      return new Array[Batch](0)
    } else {
      val limitScaleUpFactor = Math.max(conf.limitScaleUpFactor, 2)
      //    // TODO: refactor and reuse the code from RDD's take()
      val childRDD = collectLimitExec.child.execute()
      val buf = if (takeFromEnd) new ListBuffer[Batch] else new ArrayBuffer[Batch]
      var bufferedRowSize = 0L
      val totalParts = childRDD.partitions.length
      var partsScanned = 0
      while (bufferedRowSize < n && partsScanned < totalParts) {
        // The number of partitions to try in this iteration. It is ok for this number to be
        // greater than totalParts because we actually cap it at totalParts in runJob.
//        var numPartsToTry = conf.limitInitialNumPartitions
        var numPartsToTry = 1
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

        val parts = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
        val partsToScan = if (takeFromEnd) {
          // Reverse partitions to scan. So, if parts was [1, 2, 3] in 200 partitions (0 to 199),
          // it becomes [198, 197, 196].
          parts.map(p => (totalParts - 1) - p)
        } else {
          parts
        }

        val sc = collectLimitExec.session.sparkContext
        val res = sc.runJob(
          childRDD,
          (it: Iterator[InternalRow]) => {
            val batches = ArrowConvertersHelper.toBatchWithSchemaIterator(
              it,
              schema,
              maxRecordsPerBatch,
              maxEstimatedBatchSize,
              collectLimitExec.limit,
              timeZoneId)
            batches.map(b => b -> batches.rowCountInLastBatch).toArray
          },
          partsToScan)

        var i = 0
        if (takeFromEnd) {
//                while (buf.length < n && i < res.length) {
//                  val rows = decodeUnsafeRows(res(i)._2)
//                  if (n - buf.length >= res(i)._1) {
//                    buf.prepend(rows.toArray[InternalRow]: _*)
//                  } else {
//                    val dropUntil = res(i)._1 - (n - buf.length)
//                    // Same as Iterator.drop but this only takes a long.
//                    var j: Long = 0L
//                    while (j < dropUntil) { rows.next(); j += 1L}
//                    buf.prepend(rows.toArray[InternalRow]: _*)
//                  }
//                  i += 1
//                }
        } else {
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
        }
        partsScanned += partsToScan.size
      }

      buf.toArray
    }
  }
}
