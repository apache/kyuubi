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

package org.apache.kyuubi.util

import java.sql.{Connection, PreparedStatement, ResultSet, ResultSetMetaData}
import java.util
import javax.sql.DataSource

import scala.util.control.NonFatal

import com.jakewharton.fliptables.FlipTable

import org.apache.kyuubi.Logging

object JdbcUtils extends Logging {

  def close(c: AutoCloseable): Unit = {
    if (c != null) {
      try {
        c.close()
      } catch {
        case NonFatal(t) => warn(s"Error on closing", t)
      }
    }
  }

  def withCloseable[R, C <: AutoCloseable](c: C)(block: C => R): R = {
    try {
      block(c)
    } finally {
      close(c)
    }
  }

  def withConnection[R](block: Connection => R)(implicit ds: DataSource): R = {
    withCloseable(ds.getConnection)(block)
  }

  def execute(
      sqlTemplate: String)(
      setParameters: PreparedStatement => Unit = _ => {})(
      implicit ds: DataSource): Boolean = withConnection { conn =>
    withCloseable(conn.prepareStatement(sqlTemplate)) { pStmt =>
      setParameters(pStmt)
      pStmt.execute()
    }
  }

  def executeUpdate(
      sqlTemplate: String)(
      setParameters: PreparedStatement => Unit = _ => {})(
      implicit ds: DataSource): Int = withConnection { conn =>
    withCloseable(conn.prepareStatement(sqlTemplate)) { pStmt =>
      setParameters(pStmt)
      pStmt.executeUpdate()
    }
  }

  def executeQuery[R](
      sqlTemplate: String)(
      setParameters: PreparedStatement => Unit = _ => {})(
      processResultSet: ResultSet => R)(
      implicit ds: DataSource): R = withConnection { conn =>
    withCloseable(conn.prepareStatement(sqlTemplate)) { pStmt =>
      setParameters(pStmt)
      withCloseable(pStmt.executeQuery()) { rs =>
        processResultSet(rs)
      }
    }
  }

  def executeQueryWithRowMapper[R](
      sqlTemplate: String)(
      setParameters: PreparedStatement => Unit = _ => {})(
      rowMapper: ResultSet => R)(
      implicit ds: DataSource): Seq[R] = withConnection { conn =>
    withCloseable(conn.prepareStatement(sqlTemplate)) { pStmt =>
      setParameters(pStmt)
      withCloseable(pStmt.executeQuery()) { rs =>
        val builder = Seq.newBuilder[R]
        while (rs.next()) builder += rowMapper(rs)
        builder.result
      }
    }
  }

  def queryAndRenderResultSet(sql: String)(implicit ds: DataSource): String =
    withConnection { conn =>
      withCloseable(conn.prepareStatement(sql).executeQuery()) { rs =>
        renderResultSet(rs)
      }
    }

  private def renderResultSet(resultSet: ResultSet): String = {
    if (resultSet == null) throw new NullPointerException("resultSet == null")
    val headers: util.List[String] = new util.ArrayList[String]
    val resultSetMetaData: ResultSetMetaData = resultSet.getMetaData
    val columnCount: Int = resultSetMetaData.getColumnCount
    for (column <- 0 until columnCount) {
      headers.add(resultSetMetaData.getColumnName(column + 1))
    }
    val data: util.List[Array[String]] = new util.ArrayList[Array[String]]
    while ({
      resultSet.next
    }) {
      val rowData: Array[String] = new Array[String](columnCount)
      for (column <- 0 until columnCount) {
        rowData(column) = resultSet.getString(column + 1)
      }
      data.add(rowData)
    }
    val headerArray: Array[String] = headers.toArray(new Array[String](headers.size))
    val dataArray: Array[Array[String]] = data.toArray(new Array[Array[String]](data.size))
    FlipTable.of(headerArray, dataArray)
  }
}
