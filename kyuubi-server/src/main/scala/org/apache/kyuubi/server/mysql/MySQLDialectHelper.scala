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
    // mysql-connector-java:8 initialized query
    case sql
        if sql.contains("select  @@session.auto_increment_increment as auto_increment_increment, @@character_set_client as character_set_client, @@character_set_connection as character_set_connection, @@character_set_results as character_set_results, @@character_set_server as character_set_server, @@collation_server as collation_server, @@collation_connection as collation_connection, @@init_connect as init_connect, @@interactive_timeout as interactive_timeout, @@license as license, @@lower_case_table_names as lower_case_table_names, @@max_allowed_packet as max_allowed_packet, @@net_write_timeout as net_write_timeout, @@performance_schema as performance_schema, @@query_cache_size as query_cache_size, @@query_cache_type as query_cache_type, @@sql_mode as sql_mode, @@system_time_zone as system_time_zone, @@time_zone as time_zone, @@transaction_isolation as transaction_isolation, @@wait_timeout as wait_timeout") =>
      """SELECT
        |  1                    AS auto_increment_increment,
        |  'utf8mb4'            AS character_set_client,
        |  'utf8mb4'            AS character_set_connection,
        |  'utf8mb4'            AS character_set_results,
        |  'utf8mb4'            AS character_set_server,
        |  'utf8mb4_general_ci' AS collation_server,
        |  'utf8mb4_general_ci' AS collation_connection,
        |  ''                   AS init_connect,
        |  28800                AS interactive_timeout,
        |  'Apache License 2.0' AS license,
        |  0                    AS lower_case_table_names,
        |  4194304              AS max_allowed_packet,
        |  60                   AS net_write_timeout,
        |  0                    AS performance_schema,
        |  1048576              AS query_cache_size,
        |  'OFF'                AS query_cache_type,
        |  'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION' AS sql_mode,
        |  'UTC'                AS system_time_zone,
        |  'SYSTEM'             AS time_zone,
        |  'REPEATABLE-READ'    AS transaction_isolation,
        |  '28800'              AS wait_timeout
        |""".stripMargin
    // scalastyle:on line.size.limit
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
}
