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
package org.apache.kyuubi

import java.sql.ResultSet

import scala.collection.mutable.ArrayBuffer

import com.jakewharton.fliptables.FlipTable

object TestUtils {
  def displayResultSet(resultSet: ResultSet): Unit = {
    if (resultSet == null) throw new NullPointerException("resultSet == null")
    val resultSetMetaData = resultSet.getMetaData
    val columnCount: Int = resultSetMetaData.getColumnCount
    val headers = (1 to columnCount).map(resultSetMetaData.getColumnName).toArray
    val data = ArrayBuffer.newBuilder[Array[String]]
    while (resultSet.next) {
      data += (1 to columnCount).map(resultSet.getString).toArray
    }
    // scalastyle:off println
    println(FlipTable.of(headers, data.result().toArray))
    // scalastyle:on println
  }
}
