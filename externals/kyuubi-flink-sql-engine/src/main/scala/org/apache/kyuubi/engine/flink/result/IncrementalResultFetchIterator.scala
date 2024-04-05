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

package org.apache.kyuubi.engine.flink.result

import java.util
import java.util.concurrent.Executors

import scala.collection.convert.ImplicitConversions._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.Duration

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.conversion.DataStructureConverters
import org.apache.flink.table.gateway.api.results.ResultSet.ResultType
import org.apache.flink.table.gateway.service.result.ResultFetcher
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.flink.shim.FlinkResultSet
import org.apache.kyuubi.operation.FetchIterator
import org.apache.kyuubi.util.reflect.DynFields

class IncrementalResultFetchIterator(
    resultFetcher: ResultFetcher,
    maxRows: Int = 1000000,
    resultFetchTimeout: Duration = Duration.Inf) extends FetchIterator[Row] with Logging {

  val schema: ResolvedSchema = resultFetcher.getResultSchema

  val dataTypes: util.List[DataType] = schema.getColumnDataTypes

  var token: Long = 0

  var pos: Long = 0

  var fetchStart: Long = 0

  var bufferedRows: Array[Row] = new Array[Row](0)

  var hasNext: Boolean = true

  val FETCH_INTERVAL_MS: Long = 1000

  val isQueryResult: Boolean =
    DynFields.builder
      .hiddenImpl(classOf[ResultFetcher], "isQueryResult")
      .build[Boolean](resultFetcher).get()

  val effectiveMaxRows: Int = if (isQueryResult) maxRows else Int.MaxValue

  private val executor = Executors.newSingleThreadScheduledExecutor(
    new ThreadFactoryBuilder().setNameFormat("flink-result-iterator-%d").setDaemon(true).build)

  implicit private val executionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(executor)

  /**
   * Begin a fetch block, forward from the current position.
   *
   * Throws TimeoutException if no data is fetched within the timeout.
   */
  override def fetchNext(): Unit = {
    if (!hasNext) {
      return
    }
    val future = Future(() -> {
      var fetched = false
      // if no timeout is set, this would block until some rows are fetched
      debug(s"Fetching from result store with timeout $resultFetchTimeout ms")
      while (!fetched && !Thread.interrupted()) {
        val rs = resultFetcher.fetchResults(token, effectiveMaxRows - bufferedRows.length)
        val flinkRs = new FlinkResultSet(rs)
        flinkRs.getResultType match {
          case ResultType.EOS =>
            debug("EOS received, no more data to fetch.")
            fetched = true
            hasNext = false
          case ResultType.NOT_READY =>
            // if flink jobs are not ready, continue to retry
            debug("Result not ready, retrying...")
          case ResultType.PAYLOAD =>
            val fetchedData = flinkRs.getData
            // if no data fetched, continue to retry
            if (!fetchedData.isEmpty) {
              debug(s"Fetched ${fetchedData.length} rows from result store.")
              fetched = true
              bufferedRows ++= fetchedData.map(rd => convertToRow(rd, dataTypes.toList))
              fetchStart = pos
            } else {
              debug("No data fetched, retrying...")
            }
          case _ =>
            throw new RuntimeException(s"Unexpected result type: ${flinkRs.getResultType}")
        }
        if (hasNext) {
          val nextToken = flinkRs.getNextToken
          if (nextToken == null) {
            hasNext = false
          } else {
            token = nextToken
          }
        }
        Thread.sleep(FETCH_INTERVAL_MS)
      }
    })
    Await.result(future, resultFetchTimeout)
  }

  /**
   * Begin a fetch block, moving the iterator to the given position.
   * Resets the fetch start offset.
   *
   * @param pos index to move a position of iterator.
   */
  override def fetchAbsolute(pos: Long): Unit = {
    val effectivePos = Math.max(pos, 0)
    if (effectivePos < bufferedRows.length) {
      this.fetchStart = effectivePos
      return
    }
    throw new IllegalArgumentException(s"Cannot skip to an unreachable position $effectivePos.")
  }

  override def getFetchStart: Long = fetchStart

  override def getPosition: Long = pos

  /**
   * @return returns row if any and null if no more rows can be fetched.
   */
  override def next(): Row = {
    if (pos < bufferedRows.length) {
      debug(s"Fetching from buffered rows at pos $pos.")
      val row = bufferedRows(pos.toInt)
      pos += 1
      if (pos >= effectiveMaxRows) {
        hasNext = false
      }
      row
    } else {
      // block until some rows are fetched or TimeoutException is thrown
      fetchNext()
      if (hasNext) {
        val row = bufferedRows(pos.toInt)
        pos += 1
        if (pos >= effectiveMaxRows) {
          hasNext = false
        }
        row
      } else {
        null
      }
    }
  }

  def close(): Unit = {
    resultFetcher.close()
    executor.shutdown()
  }

  private[this] def convertToRow(r: RowData, dataTypes: List[DataType]): Row = {
    val converter = DataStructureConverters.getConverter(DataTypes.ROW(dataTypes: _*))
    converter.toExternal(r).asInstanceOf[Row]
  }
}
