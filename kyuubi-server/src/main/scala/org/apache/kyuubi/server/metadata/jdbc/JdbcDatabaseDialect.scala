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

package org.apache.kyuubi.server.metadata.jdbc

import org.apache.kyuubi.Logging

trait JdbcDatabaseDialect {
  def limitClause(limit: Int, offset: Int): String
  def deleteFromLimitClause(limit: Int): String
  def insertOrReplace(
      table: String,
      cols: Seq[String],
      keyCol: String,
      keyVal: String): String
}

class GenericDatabaseDialect extends JdbcDatabaseDialect with Logging {
  override def limitClause(limit: Int, offset: Int): String = {
    s"LIMIT $limit OFFSET $offset"
  }

  override def deleteFromLimitClause(limit: Int): String = {
    warn("Generic dialect does not support LIMIT in DELETE statements")
    ""
  }

  override def insertOrReplace(
      table: String,
      cols: Seq[String],
      keyCol: String,
      keyVal: String): String = {
    s"""
       |INSERT INTO $table (${cols.mkString(",")})
       |SELECT ${cols.map(_ => "?").mkString(",")}
       |WHERE NOT EXISTS (
       |  SELECT 1 FROM $table WHERE $keyCol = '$keyVal')
       |)
       |""".stripMargin
  }
}

class SQLiteDatabaseDialect extends GenericDatabaseDialect {
  override def insertOrReplace(
      table: String,
      cols: Seq[String],
      keyCol: String,
      keyVal: String): String = {
    s"""
       |INSERT OR REPLACE INTO $table (${cols.mkString(",")})
       |VALUES (${cols.map(_ => "?").mkString(",")})
       |""".stripMargin
  }
}
class MySQLDatabaseDialect extends GenericDatabaseDialect {
  override def insertOrReplace(
      table: String,
      cols: Seq[String],
      keyCol: String,
      keyVal: String): String = {
    s"""
       |INSERT INTO $table (${cols.mkString(",")})
       |VALUES (${cols.map(_ => "?").mkString(",")}) AS new
       |ON DUPLICATE KEY UPDATE
       |${cols.filterNot(_ == keyCol).map(c => s"$c = new.$c").mkString(",")}
       |""".stripMargin
  }

  override def deleteFromLimitClause(limit: Int): String = {
    s"LIMIT $limit"
  }
}
class PostgreSQLDatabaseDialect extends GenericDatabaseDialect {
  override def insertOrReplace(
      table: String,
      cols: Seq[String],
      keyCol: String,
      keyVal: String): String = {
    s"""
       |INSERT INTO $table (${cols.mkString(",")})
       |VALUES (${cols.map(_ => "?").mkString(",")})
       |ON CONFLICT ($keyCol)
       |DO UPDATE SET
       |${cols.filterNot(_ == keyCol).map(c => s"$c = EXCLUDED.$c").mkString(",")}
       |""".stripMargin
  }
}
