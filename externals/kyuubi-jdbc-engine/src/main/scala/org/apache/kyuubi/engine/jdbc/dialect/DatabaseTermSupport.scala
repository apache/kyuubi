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
package org.apache.kyuubi.engine.jdbc.dialect

import java.sql.{Connection, SQLFeatureNotSupportedException}

/**
 * Mixin for dialects whose driver exposes the database as either the JDBC catalog or the
 * JDBC schema. Writes both sides, reads the first non-empty one. Used by ClickHouse and the
 * MySQL family (MySQL / Doris / StarRocks).
 */
trait DatabaseTermSupport extends JdbcDialect {

  override def setSchema(conn: Connection, schema: String): Unit = {
    setDatabase(conn, schema)
  }

  override def setCatalog(conn: Connection, catalog: String): Unit = {
    setDatabase(conn, catalog)
  }

  override def getCurrentSchema(conn: Connection): String = {
    readDatabase(conn.getSchema, conn.getCatalog)
  }

  override def getCatalog(conn: Connection): String = {
    readDatabase(conn.getCatalog, conn.getSchema)
  }

  // Symmetric with setDatabase: tolerate drivers that reject one of the getters outright.
  private def readDatabase(preferred: => String, fallback: => String): String = {
    val first =
      try Option(preferred).filter(_.nonEmpty)
      catch { case _: SQLFeatureNotSupportedException => None }
    first.getOrElse {
      try fallback
      catch { case _: SQLFeatureNotSupportedException => null }
    }
  }

  // Suppress only the "setter not implemented" signal; real failures (missing db, permission
  // denied) propagate. If neither setter accepts, throw rather than silently no-op.
  protected def setDatabase(conn: Connection, database: String): Unit = {
    var accepted = false
    try {
      conn.setCatalog(database)
      accepted = true
    } catch {
      case _: SQLFeatureNotSupportedException =>
    }
    try {
      conn.setSchema(database)
      accepted = true
    } catch {
      case _: SQLFeatureNotSupportedException =>
    }
    if (!accepted) {
      throw new SQLFeatureNotSupportedException(
        s"Neither Connection.setCatalog nor Connection.setSchema is supported by this " +
          s"driver; cannot switch to database '$database'")
    }
  }
}
