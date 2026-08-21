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

package org.apache.kyuubi.server.flight

import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.kyuubi.Logging
import org.apache.kyuubi.metrics.{MetricsConstants, MetricsSystem}
import org.apache.kyuubi.operation.{FetchOrientation, OperationHandle}
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TRowSet

/**
 * Page-oriented Flight result iterator. Retains only the current backend page /
 * Arrow batch and never materializes an entire query result.
 */
class FlightResultIterator(
    backend: BackendService,
    operation: OperationHandle,
    schema: Schema,
    allocator: BufferAllocator,
    pageSize: Int,
    isCancelled: () => Boolean)
  extends AutoCloseable with Logging {

  private val root = VectorSchemaRoot.create(schema, allocator)
  private val loader = new VectorLoader(root)
  private val closed = new AtomicBoolean(false)
  private var started = false
  private var finished = false

  def start(listener: ServerStreamListener): VectorSchemaRoot = {
    if (!started) {
      listener.start(root)
      started = true
    }
    root
  }

  /**
   * Fetch and load the next non-empty page into [[root]].
   * @return true when a page was loaded, false when the stream is exhausted
   */
  def nextBatch(): Boolean = {
    ensureOpen()
    if (finished || isCancelled()) {
      return false
    }

    while (!finished && !isCancelled()) {
      val rowSet = backend.fetchResults(
        operation,
        FetchOrientation.FETCH_NEXT,
        pageSize,
        fetchLog = false).getResults
      if (KyuubiFlightArrowUtils.isEmpty(rowSet)) {
        finished = true
        return false
      }

      root.clear()
      val loadedRows =
        if (KyuubiFlightArrowUtils.isArrowRowSet(rowSet)) {
          loadArrowBatch(rowSet)
        } else {
          KyuubiFlightArrowUtils.populateRootFromRowSet(root, rowSet)
          root.getRowCount
        }

      if (loadedRows > 0) {
        MetricsSystem.tracing { ms =>
          ms.markMeter(MetricsConstants.FLIGHT_SQL_STREAM_BATCHES)
          ms.markMeter(MetricsConstants.FLIGHT_SQL_STREAM_ROWS, loadedRows)
        }
        return true
      }

      // Zero decoded rows means the backend page is exhausted (including an empty Arrow
      // IPC batch). Do not keep fetching.
      finished = true
      return false
    }
    false
  }

  def currentRoot: VectorSchemaRoot = root

  def cancel(): Unit = {
    try backend.cancelOperation(operation)
    catch {
      case NonFatal(e) => warn(s"Failed to cancel Flight SQL operation $operation", e)
    }
  }

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      try root.close()
      catch {
        case NonFatal(e) => warn("Failed to close Flight result VectorSchemaRoot", e)
      }
    }
  }

  private def loadArrowBatch(rowSet: TRowSet): Int = {
    val batch = KyuubiFlightArrowUtils.decodeBatch(rowSet, allocator)
    try {
      loader.load(batch)
      root.setRowCount(batch.getLength)
      val bytes = KyuubiFlightArrowUtils.arrowBatchBytes(rowSet)
      if (bytes > 0) {
        MetricsSystem.tracing(_.markMeter(MetricsConstants.FLIGHT_SQL_STREAM_BYTES, bytes))
      }
      batch.getLength
    } finally {
      batch.close()
    }
  }

  private def ensureOpen(): Unit = {
    if (closed.get()) {
      throw new IllegalStateException("FlightResultIterator is closed")
    }
  }
}
