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

import java.sql.{ResultSetMetaData, Statement}

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.engine.jdbc.schema.Row

class ResultSetWrapper(statement: Statement)
  extends Iterator[Row] {

  private var currentResult = statement.getResultSet

  private lazy val metadata = currentResult.getMetaData

  override def hasNext: Boolean = {
    val result = currentResult.next()
    if (!result) {
      val hasMoreResults = statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT)
      if (hasMoreResults) {
        currentResult = statement.getResultSet
        currentResult.next()
      } else {
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
