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

import java.io.ByteArrayOutputStream
import java.nio.channels.Channels

import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.{ArrowStreamWriter, WriteChannel}
import org.apache.arrow.vector.ipc.message.{IpcOption, MessageSerializer}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

object ArrowConvertersHelper extends Logging {

  /**
   * Different from [[org.apache.spark.sql.execution.arrow.ArrowConvertersHelper.toBatchIterator]],
   * each output arrow batch contains this batch row count.
   */
  def toBatchIterator(
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
      var estimatedBatchSize = 0
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
