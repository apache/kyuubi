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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.channels.Channels

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.{ArrowStreamWriter, ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.{IpcOption, MessageSerializer}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, SQLConfHelper}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.CollectLimitExec
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

object KyuubiArrowConverters extends SQLConfHelper with Logging {

  type Batch = (Array[Byte], Long)

  /**
   * this method is to slice the input Arrow record batch byte array `bytes`, starting from `start`
   * and taking `length` number of elements.
   */
  def slice(
      schema: StructType,
      timeZoneId: String,
      bytes: Array[Byte],
      start: Int,
      length: Int): Array[Byte] = {
    val in = new ByteArrayInputStream(bytes)
    val out = new ByteArrayOutputStream(bytes.length)

    var vectorSchemaRoot: VectorSchemaRoot = null
    var slicedVectorSchemaRoot: VectorSchemaRoot = null

    val sliceAllocator = ArrowUtils.rootAllocator.newChildAllocator(
      "slice",
      0,
      Long.MaxValue)
    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, sliceAllocator)
    try {
      val recordBatch = MessageSerializer.deserializeRecordBatch(
        new ReadChannel(Channels.newChannel(in)),
        sliceAllocator)
      val vectorLoader = new VectorLoader(vectorSchemaRoot)
      vectorLoader.load(recordBatch)
      recordBatch.close()
      slicedVectorSchemaRoot = vectorSchemaRoot.slice(start, length)

      val unloader = new VectorUnloader(slicedVectorSchemaRoot)
      val writeChannel = new WriteChannel(Channels.newChannel(out))
      val batch = unloader.getRecordBatch()
      MessageSerializer.serialize(writeChannel, batch)
      batch.close()
      out.toByteArray()
    } finally {
      in.close()
      out.close()
      if (vectorSchemaRoot != null) {
        vectorSchemaRoot.getFieldVectors.asScala.foreach(_.close())
        vectorSchemaRoot.close()
      }
      if (slicedVectorSchemaRoot != null) {
        slicedVectorSchemaRoot.getFieldVectors.asScala.foreach(_.close())
        slicedVectorSchemaRoot.close()
      }
      sliceAllocator.close()
    }
  }

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

        // TODO: SparkPlan.session introduced in SPARK-35798, replace with SparkPlan.session once we
        // drop Spark-3.1.x support.
        val sc = SparkSession.active.sparkContext
        val res = sc.runJob(
          childRDD,
          (it: Iterator[InternalRow]) => {
            val batches = toBatchIterator(
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
  private def limitInitialNumPartitions: Int = {
    conf.getConfString("spark.sql.limit.initialNumPartitions", "1")
      .toInt
  }

  /**
   * Different from [[org.apache.spark.sql.execution.arrow.ArrowConverters.toBatchIterator]],
   * each output arrow batch contains this batch row count.
   */
  private def toBatchIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Long,
      maxEstimatedBatchSize: Long,
      limit: Long,
      timeZoneId: String): ArrowBatchIterator = {
    new ArrowBatchIterator(
      rowIter,
      schema,
      maxRecordsPerBatch,
      maxEstimatedBatchSize,
      limit,
      timeZoneId,
      TaskContext.get)
  }

  /**
   * This class ArrowBatchIterator is derived from
   * [[org.apache.spark.sql.execution.arrow.ArrowConverters.ArrowBatchWithSchemaIterator]],
   * with two key differences:
   *   1. there is no requirement to write the schema at the batch header
   *   2. iteration halts when `rowCount` equals `limit`
   */
  private[sql] class ArrowBatchIterator(
      rowIter: Iterator[InternalRow],
      schema: StructType,
      maxRecordsPerBatch: Long,
      maxEstimatedBatchSize: Long,
      limit: Long,
      timeZoneId: String,
      context: TaskContext)
    extends Iterator[Array[Byte]] {

    protected val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    private val allocator =
      ArrowUtils.rootAllocator.newChildAllocator(
        s"to${this.getClass.getSimpleName}",
        0,
        Long.MaxValue)

    private val root = VectorSchemaRoot.create(arrowSchema, allocator)
    protected val unloader = new VectorUnloader(root)
    protected val arrowWriter = ArrowWriter.create(root)

    Option(context).foreach {
      _.addTaskCompletionListener[Unit] { _ =>
        root.close()
        allocator.close()
      }
    }

    override def hasNext: Boolean = (rowIter.hasNext && rowCount < limit) || {
      root.close()
      allocator.close()
      false
    }

    var rowCountInLastBatch: Long = 0
    var rowCount: Long = 0

    override def next(): Array[Byte] = {
      val out = new ByteArrayOutputStream()
      val writeChannel = new WriteChannel(Channels.newChannel(out))

      rowCountInLastBatch = 0
      var estimatedBatchSize = 0L
      Utils.tryWithSafeFinally {

        // Always write the first row.
        while (rowIter.hasNext && (
            // For maxBatchSize and maxRecordsPerBatch, respect whatever smaller.
            // If the size in bytes is positive (set properly), always write the first row.
            rowCountInLastBatch == 0 && maxEstimatedBatchSize > 0 ||
              // If the size in bytes of rows are 0 or negative, unlimit it.
              estimatedBatchSize <= 0 ||
              estimatedBatchSize < maxEstimatedBatchSize ||
              // If the size of rows are 0 or negative, unlimit it.
              maxRecordsPerBatch <= 0 ||
              rowCountInLastBatch < maxRecordsPerBatch ||
              rowCount < limit)) {
          val row = rowIter.next()
          arrowWriter.write(row)
          estimatedBatchSize += (row match {
            case ur: UnsafeRow => ur.getSizeInBytes
            // Trying to estimate the size of the current row
            case _: InternalRow => schema.defaultSize
          })
          rowCountInLastBatch += 1
          rowCount += 1
        }
        arrowWriter.finish()
        val batch = unloader.getRecordBatch()
        MessageSerializer.serialize(writeChannel, batch)

        // Always write the Ipc options at the end.
        ArrowStreamWriter.writeEndOfStream(writeChannel, IpcOption.DEFAULT)

        batch.close()
      } {
        arrowWriter.reset()
      }

      out.toByteArray
    }
  }

  // for testing
  def fromBatchIterator(
      arrowBatchIter: Iterator[Array[Byte]],
      schema: StructType,
      timeZoneId: String,
      context: TaskContext): Iterator[InternalRow] = {
    ArrowConverters.fromBatchIterator(arrowBatchIter, schema, timeZoneId, context)
  }
}
