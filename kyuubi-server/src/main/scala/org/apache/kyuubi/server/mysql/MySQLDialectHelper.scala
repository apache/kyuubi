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

package org.apache.kyuubi.server.mysql

import io.netty.channel.ChannelHandlerContext

import org.apache.kyuubi.server.mysql.constant._

object MySQLDialectHelper {

  def convertQuery(origin: String): String = origin.toLowerCase.trim match {
    case "select @@version_comment limit 1" =>
      s"select '${MySQLServerDefines.KYUUBI_SERVER_DESCRIPTION}' as `@@version_comment`"
    case "select database()" =>
      "select current_database() as `database()`"
    case "select database(), user() limit 1" =>
      """select
        |  current_database() as `database()`,
        |  session_user() as `user()`
        |""".stripMargin
    // scalastyle:off line.size.limit
    case "select @@character_set_client, @@character_set_connection, @@character_set_server, @@character_set_database limit 1" =>
      """select
        |  'utf8mb4' as `@@character_set_client`,
        |  'utf8mb4' as `@@character_set_connection`,
        |  'utf8mb4' as `@@character_set_server`,
        |  'utf8mb4' as `@@character_set_database`
        |""".stripMargin
    // scalastyle:on line.size.limit
    // mysql-connector-java:[5-8] version initialized query
    case sql
        if sql.contains("select @@session.auto_increment_increment") ||
          sql.contains("select  @@session.auto_increment_increment") =>
      convertInitSQL(sql)
    case "set names utf8mb4" =>
      "SET NAMES=utf8mb4"
    case "select @@session.transaction_read_only" =>
      "select '0' as `@@session.transaction_read_only`"
    case _ => origin
  }

  def shouldExecuteLocal(sql: String): Boolean = {
    sql.trim.toLowerCase == "select 'kyuubi'" || sql.trim.toLowerCase.startsWith("kill query ")
  }

  def localExecuteStatement(ctx: ChannelHandlerContext, sql: String): MySQLQueryResult = {
    // A mock result, for testing
    if (sql.trim.toLowerCase == "select 'kyuubi'") {
      return MySQLQueryResult(
        MySQLField("kyuubi", MySQLDataType.VAR_STRING) :: Nil,
        Seq(Seq("KYUUBI")))
    }
    throw MySQLErrorCode.ER_NOT_SUPPORTED_YET.toKyuubiSQLException
  }

  private val regexInitSQL = "@@(session\\.)?([^ ]+)".r

  private def convertInitSQL(originSQL: String): String = {
    regexInitSQL.findAllMatchIn(originSQL).map(m => {
      val key = m.group(2)
      if (serverVariables.contains(key)) {
        s"'${serverVariables(key)}' AS $key"
      } else {
        return originSQL
      }
    }).mkString("SELECT ", ",", "")
  }

  private val serverVariables: Map[String, String] =
    Map(
      "auto_increment_increment" -> "1",
      "character_set_client" -> "utf8mb4",
      "character_set_connection" -> "utf8mb4",
      "character_set_results" -> "utf8mb4",
      "character_set_server" -> "utf8mb4",
      "collation_connection" -> "utf8mb4_general_ci",
      "collation_server" -> "utf8mb4_general_ci",
      "have_query_cache" -> "YES",
      "init_connect" -> "",
      "interactive_timeout" -> "28800",
      "license" -> "Apache License 2.0",
      "lower_case_table_names" -> "0",
      "max_allowed_packet" -> "4194304",
      "net_buffer_length" -> "16384",
      "net_write_timeout" -> "60",
      "performance_schema" -> "0",
      "query_cache_size" -> "1048576",
      "query_cache_type" -> "OFF",
      "sql_mode" -> ("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE," +
        "NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"),
      "system_time_zone" -> "UTC",
      "time_zone" -> "SYSTEM",
      "transaction_isolation" -> "REPEATABLE-READ",
      "tx_isolation" -> "REPEATABLE-READ",
      "wait_timeout" -> "28800")
}
