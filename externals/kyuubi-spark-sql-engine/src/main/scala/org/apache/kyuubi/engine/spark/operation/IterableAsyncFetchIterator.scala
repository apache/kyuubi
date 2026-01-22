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

package org.apache.kyuubi.engine.spark.operation

import java.util.concurrent._

import scala.reflect.ClassTag

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.spark.schema.SparkTRowSetGenerator
import org.apache.kyuubi.operation.FetchIterator
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TProtocolVersion, TRowSet};

class IterableAsyncFetchIterator[A: ClassTag](
    iterable: Iterable[A],
    prefetchExecutor: ExecutorService,
    resultSchema: StructType,
    protocolVersion: TProtocolVersion,
    asyncFetchTimeout: Long,
    rowsetsQueueSize: Int) extends FetchIterator[A] with Logging {
  val results = new LinkedBlockingQueue[(TRowSet, Int)](rowsetsQueueSize)
  private var future: Future[Boolean] = _
  private val tRowSetGenerator = new SparkTRowSetGenerator()
  var rowSetSize: Int = -1

  private var iter: Iterator[A] = iterable.iterator
  private var iterEx: Iterator[Row] = iter.asInstanceOf[Iterator[Row]]

  private var fetchStart: Long = 0
  private var position: Long = 0

  private def startPrefetchThread(): Unit = {
    val task = new Callable[Boolean] {
      override def call(): Boolean = {
        var succeeded = true
        try {
          var isEmptyIter = false
          while (!isEmptyIter) {
            val taken = iterEx.take(rowSetSize)
            val rows = taken.toArray
            val rowSet = tRowSetGenerator.toTRowSet(
              rows.toSeq,
              resultSchema,
              protocolVersion)
            results.put(rowSet, rows.length)
            isEmptyIter = (rows.length == 0)
          }
        } catch {
          case e: Throwable =>
            error(s"An exception occurred in the prefetch thread. message: ${e.getMessage}")
            e.printStackTrace(System.err)
            results.clear()
            succeeded = false
        }
        succeeded
      }
    }
  }

  def takeRowSet(numRows: Int): TRowSet = {
    if (rowSetSize < 0) {
      rowSetSize = numRows
      startPrefetchThread()
    } else {
      if (numRows != rowSetSize) {
        throw new Exception("The current size of row set is different from " +
          "that of the initial call.")
      }
    }
    var result: (TRowSet, Int) = results.poll(3000, TimeUnit.MILLISECONDS)
    if (result == null && !future.isDone) {
      warn("Queue of prefetched results is empty, the prefetch thread my be stuck," +
        s"retry to retrieve result with a timeout of ${asyncFetchTimeout} ms")
      result = results.poll(asyncFetchTimeout, TimeUnit.MILLISECONDS)
    }
    if (result == null) {
      if (future.isDone) {
        val prefetchSucceeded = future.get()
        if (!prefetchSucceeded) {
          throw new Exception("An exception occurred in the prefetch thread.")
        } else {
          result = results.poll()
          if (result == null) {
            error("Thrift client fetched more than one empty rowSet!")
            result = (
              tRowSetGenerator.toTRowSet(
                Seq.empty[Row],
                resultSchema,
                protocolVersion),
              0)
          }
        }
      } else {
        future.cancel(true)
        throw new Exception("the prefetch thread is stuck.")
      }
    }
    position += result._2
    result._1
  }

  /**
   * Begin a fetch block, forward from the current position.
   * Resets the fetch start offset.
   */
  override def fetchNext(): Unit = fetchStart = position

  /**
   * Begin a fetch block, moving the iterator to the given position.
   * Resets the fetch start offset.
   *
   * @param pos index to move a position of iterator.
   */
  override def fetchAbsolute(pos: Long): Unit = {
    if (future != null) {
      future.cancel(true)
      future.get(5, TimeUnit.SECONDS)
      if (!future.isDone) {
        throw new Exception("Cancel the prefetch thread failed")
      }
      future = null
      results.clear()
    }
    val newPos = pos max 0
    resetPosition()
    while (position < newPos && hasNextInternal) {
      nextInternal()
    }
    rowSetSize = -1
  }

  override def getFetchStart: Long = fetchStart

  override def getPosition: Long = position

  override def hasNext: Boolean = {
    throw new Exception("Unsupported function: IterableAsyncFetchIterator.hasNext")
  }
  def hasNextInternal: Boolean = iter.hasNext
  override def next(): A = {
    throw new Exception("Unsupported function: IterableAsyncFetchIterator.next")
  }
  def nextInternal(): A = {
    position += 1
    iter.next()
  }

  private def resetPosition(): Unit = {
    if (position != 0) {
      iter = iterable.iterator
      iterEx = iter.asInstanceOf[Iterator[Row]]
      position = 0
      fetchStart = 0
    }
  }
}
