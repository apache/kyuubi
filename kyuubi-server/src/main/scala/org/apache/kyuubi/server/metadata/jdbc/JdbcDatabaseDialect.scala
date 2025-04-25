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

trait JdbcDatabaseDialect {
  def limitClause(limit: Int, offset: Int): String
  def insertOrReplace(
      table: String,
      colsToInsert: Seq[String],
      colsToReplace: Seq[String],
      keyCol: String): Option[String] = None
}

class GenericDatabaseDialect extends JdbcDatabaseDialect {
  override def limitClause(limit: Int, offset: Int): String = {
    s"LIMIT $limit OFFSET $offset"
  }
}

class SQLiteDatabaseDialect extends GenericDatabaseDialect {
  override def insertOrReplace(
      table: String,
      colsToInsert: Seq[String],
      colsToReplace: Seq[String],
      keyCol: String): Option[String] = {
    Some(
      s"""
         |INSERT OR REPLACE INTO $table (${colsToInsert.mkString(",")})
         |VALUES (${colsToInsert.map(_ => "?").mkString(",")})
         |""".stripMargin)
  }
}
class MySQLDatabaseDialect extends GenericDatabaseDialect {
  override def insertOrReplace(
      table: String,
      colsToInsert: Seq[String],
      colsToReplace: Seq[String],
      keyCol: String): Option[String] = {
    Some(
      s"""
         |INSERT INTO $table (${colsToInsert.mkString(",")})
         |VALUES (${colsToInsert.map(_ => "?").mkString(",")})
         |ON DUPLICATE KEY UPDATE
         |${colsToReplace.map(c => s"$c = VALUES($c)").mkString(",")}
         |""".stripMargin)
  }
}
class PostgreSQLDatabaseDialect extends GenericDatabaseDialect {
  override def insertOrReplace(
      table: String,
      colsToInsert: Seq[String],
      colsToReplace: Seq[String],
      keyCol: String): Option[String] = {
    Some(
      s"""
         |INSERT INTO $table (${colsToInsert.mkString(",")})
         |VALUES (${colsToInsert.map(_ => "?").mkString(",")})
         |ON CONFLICT ($keyCol)
         |DO UPDATE SET
         |${colsToReplace.map(c => s"$c = EXCLUDED.$c").mkString(",")}
         |""".stripMargin)
  }
}
