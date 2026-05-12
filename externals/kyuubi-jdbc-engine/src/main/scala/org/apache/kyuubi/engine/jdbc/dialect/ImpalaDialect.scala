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

import java.sql.Connection

import org.apache.kyuubi.engine.jdbc.impala.{ImpalaSchemaHelper, ImpalaTRowSetGenerator}
import org.apache.kyuubi.engine.jdbc.schema.{JdbcTRowSetGenerator, SchemaHelper}
import org.apache.kyuubi.util.JdbcUtils.withCloseable

class ImpalaDialect extends JdbcDialect {

  // The JDBC engine talks to Impalad via `KyuubiHiveDriver` (see `ImpalaConnectionProvider`,
  // chosen for its fixed `getMoreResults()` behavior). `KyuubiConnection#setSchema` ships a
  // Kyuubi-private session config (`kyuubi.operation.set.current.database`) that Impalad
  // does not recognize and rejects with "Invalid query option". Issue `USE` directly so the
  // database switch lands as plain Impala SQL the backend understands.
  override def setSchema(conn: Connection, schema: String): Unit = {
    val escaped = schema.replace("`", "``")
    withCloseable(conn.createStatement()) { stmt =>
      stmt.execute(s"USE `$escaped`")
    }
  }

  // Symmetric to `setSchema`: `KyuubiConnection#getSchema` ships a Kyuubi-private session
  // config (`kyuubi.operation.get.current.database`) that Impalad rejects. Read the current
  // database via plain SQL.
  override def getCurrentSchema(conn: Connection): String = {
    withCloseable(conn.createStatement()) { stmt =>
      withCloseable(stmt.executeQuery("SELECT current_database()")) { rs =>
        if (rs.next()) rs.getString(1) else null
      }
    }
  }

  override def getTRowSetGenerator(): JdbcTRowSetGenerator = new ImpalaTRowSetGenerator

  override def getSchemaHelper(): SchemaHelper = new ImpalaSchemaHelper

  override def name(): String = "impala"
}
