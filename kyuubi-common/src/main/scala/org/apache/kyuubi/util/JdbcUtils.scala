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

import java.sql.{Connection, PreparedStatement, ResultSet}
import javax.sql.DataSource

import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils

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

  def mapResultSet[R](rs: ResultSet)(rowMapper: ResultSet => R): Seq[R] = {
    val builder = Seq.newBuilder[R]
    while (rs.next()) builder += rowMapper(rs)
    builder.result
  }

  def redactPassword(password: Option[String]): String = {
    password match {
      case Some(s) if StringUtils.isNotBlank(s) => s"${"*" * s.length}(length:${s.length})"
      case _ => "(empty)"
    }
  }

  def isDuplicatedKeyDBErr(cause: Throwable): Boolean = {
    val duplicatedKeyKeywords = Seq(
      "Duplicate entry", // MySQL
      "duplicate key value violates unique constraint", // PostgreSQL
      "A UNIQUE constraint failed" // SQLite
    )
    duplicatedKeyKeywords.exists(cause.getMessage.contains)
  }
}
