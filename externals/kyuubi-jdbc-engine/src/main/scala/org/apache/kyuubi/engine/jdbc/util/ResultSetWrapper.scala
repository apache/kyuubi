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
package org.apache.kyuubi.engine.jdbc.util

import java.sql.{ResultSet, ResultSetMetaData, Statement}

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.engine.jdbc.schema.Row
import org.apache.kyuubi.operation.FetchIterator

class ResultSetWrapper(statement: Statement)
  extends Iterator[Row] {

  private var currentResult = statement.getResultSet

  private lazy val metadata = currentResult.getMetaData

  override def hasNext: Boolean = {
    if (currentResult == null) return false
    val result = currentResult.next()
    if (!result) {
      val hasMoreResults = statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT)
      if (hasMoreResults) {
        currentResult = statement.getResultSet
        currentResult.next()
      } else {
        currentResult = null
        false
      }
    } else {
      result
    }
  }

  override def next(): Row = {
    toRow()
  }

  def toArray(): Array[Row] = {
    val result = ArrayBuffer[Row]()
    while (currentResult.next()) {
      val row = toRow()
      result += row
    }
    result.toArray
  }

  private def toRow(): Row = {
    val buffer = ArrayBuffer[Any]()
    for (i <- 1 to metadata.getColumnCount) {
      val value = currentResult.getObject(i)
      buffer += value
    }
    Row(buffer.toList)
  }

  def getMetadata(): ResultSetMetaData = {
    this.metadata
  }
}

/**
 * Wraps a single [[ResultSet]] (e.g. one returned by a `DatabaseMetaData.getXxx(...)` call).
 * Unlike [[ResultSetWrapper]], this class does not consult the parent [[Statement]] for
 * additional result sets - `DatabaseMetaData` calls always return exactly one.
 */
class ResultSetFetchIterator(resultSet: ResultSet) extends FetchIterator[Row] with AutoCloseable {

  private lazy val metadata: ResultSetMetaData = resultSet.getMetaData

  private lazy val columnCount: Int = metadata.getColumnCount

  private var fetchStart: Long = 0

  private var position: Long = 0

  private var prefetched: Boolean = false

  private var prefetchedHasNext: Boolean = false

  private var closed: Boolean = false

  override def fetchNext(): Unit = fetchStart = position

  override def fetchAbsolute(pos: Long): Unit = {
    throw new UnsupportedOperationException(
      "FETCH_FIRST/FETCH_PRIOR is not supported for streaming metadata results")
  }

  override def getFetchStart: Long = fetchStart

  override def getPosition: Long = position

  override def hasNext: Boolean = {
    if (closed) {
      false
    } else {
      if (!prefetched) {
        prefetchedHasNext = resultSet.next()
        prefetched = true
      }
      prefetchedHasNext
    }
  }

  override def next(): Row = {
    if (!hasNext) {
      throw new NoSuchElementException("No more rows in metadata ResultSet")
    }
    prefetched = false
    position += 1
    val buffer = ArrayBuffer[Any]()
    for (i <- 1 to columnCount) {
      buffer += resultSet.getObject(i)
    }
    Row(buffer.toList)
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      val statement =
        try {
          resultSet.getStatement
        } catch {
          case _: Throwable => null
        }
      try {
        resultSet.close()
      } finally {
        if (statement != null) {
          statement.close()
        }
      }
    }
  }

  def getMetadata: ResultSetMetaData = this.metadata
}
