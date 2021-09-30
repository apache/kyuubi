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

import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.operation.FetchOrientation
import org.apache.kyuubi.operation.OperationState._
import org.apache.kyuubi.server.mysql.MySQLCommandHandler._
import org.apache.kyuubi.server.mysql.constant.{MySQLDataType, MySQLServerDefines}
import org.apache.kyuubi.server.mysql.constant.MySQLCtxAttrKey._
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.session.SessionHandle

object MySQLCommandHandler {
  val connIdCounter = new AtomicInteger
  val connIdToSessHandle = new ConcurrentHashMap[Int, SessionHandle]
}

class MySQLCommandHandler(be: BackendService, execPool: ThreadPoolExecutor)
  extends SimpleChannelInboundHandler[MySQLCommandPacket] with Logging {

  private implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(execPool)

  @volatile private var closed: Boolean = false

  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    closeSession(ctx)
    super.channelInactive(ctx)
  }

  // handle process exception, generally should send error packet
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    val connectionId = ctx.channel.attr(CONNECTION_ID).get
    val errPacket = MySQLErrPacket(cause)
    error(s"Connection: $connectionId, $errPacket")
    if (ctx.channel.isActive) {
      ctx.writeAndFlush(errPacket)
    } else {
      warn(s"Ignore error packet for inactivated connection: $connectionId")
    }
  }

  override def channelRead0(ctx: ChannelHandlerContext, packet: MySQLCommandPacket): Unit = {
    Future {
      if (ctx.channel.attr(SESSION_HANDLE).get == null) synchronized {
        if (ctx.channel.attr(SESSION_HANDLE).get == null) {
          val sessionHandle = openSession(ctx)
          ctx.channel.attr(SESSION_HANDLE).set(sessionHandle)
          val connectionId = ctx.channel.attr(CONNECTION_ID).get
          connIdToSessHandle.put(connectionId, sessionHandle)
        }
      }

      packet match {
        case pkt: MySQLComPingPacket => handlePing(ctx, pkt)
        case pkt: MySQLComInitDbPacket => handleInitDb(ctx, pkt)
        case pkt: MySQLComQuitPacket => handleQuit(ctx, pkt)
        case pkt: MySQLComFieldListPacket => handleFieldList(ctx, pkt)
        case pkt: MySQLComQueryPacket => handleQuery(ctx, pkt)
        case bad => throw new UnsupportedOperationException(bad.getClass.getSimpleName)
      }
    } onComplete {
      case Success(responsePackets) =>
        responsePackets.foreach(ctx.channel.write)
        ctx.channel.flush()
      case Failure(cause) =>
        exceptionCaught(ctx, cause)
    }
  }

  private def openSession(ctx: ChannelHandlerContext): SessionHandle = synchronized {
    try {
      val user = ctx.channel.attr(USER).get
      val remoteIp = ctx.channel.attr(REMOTE_IP).get
      // TODO parse SET command, save other variables at ChannelHandlerContext
      val sessionConf = Option(ctx.channel.attr(DATABASE).get) match {
        case Some(db) => Map("use:database" -> db)
        case None => Map.empty[String, String]
      }
      // v1 is sufficient now, upgrade version when needed
      val proto = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1
      val sessionHandle = be.openSession(proto, user, "", remoteIp, sessionConf)
      sessionHandle
    } catch {
      case rethrow: Exception =>
        warn(s"Error opening session: ${rethrow.getMessage}")
        throw rethrow
    }
  }

  private def closeSession(ctx: ChannelHandlerContext): Unit = synchronized {
    if (!closed) {
      val handle = ctx.channel.attr(SESSION_HANDLE).get
      info(s"Received request of closing $handle")
      try be.closeSession(handle)
      catch {
        case rethrow: Exception =>
          warn(s"Error closing session: ${rethrow.getMessage}")
          throw rethrow
      } finally {
        val connectionId = ctx.channel.attr(CONNECTION_ID).getAndSet(null)
        ctx.channel.attr(SESSION_HANDLE).set(null)
        connIdToSessHandle.remove(connectionId)
      }
      closed = true
      info(s"Finished closing $handle")
    }
  }

  private def handlePing(
    ctx: ChannelHandlerContext, pkg: MySQLComPingPacket): Seq[MySQLPacket] = {
    MySQLOKPacket(1) :: Nil
  }

  private def handleInitDb(
    ctx: ChannelHandlerContext, pkg: MySQLComInitDbPacket): Seq[MySQLPacket] = {
    beExecStmt(ctx, s"use ${pkg.database}")
    MySQLOKPacket(1) :: Nil
  }

  private def handleQuit(
    ctx: ChannelHandlerContext, pkg: MySQLComQuitPacket): Seq[MySQLPacket] = {
    closeSession(ctx)
    MySQLOKPacket(1) :: Nil
  }

  // query responses should be like:
  //    Array[MySQLColumnDefinition41Packet]
  //    MySQLEofPacket
  private def handleFieldList(
    ctx: ChannelHandlerContext, pkg: MySQLComFieldListPacket): Seq[MySQLPacket] = {
    // TODO
    MySQLEofPacket(1) :: Nil
  }

  private def handleQuery(
    ctx: ChannelHandlerContext, pkg: MySQLComQueryPacket): Seq[MySQLPacket] = {
    info(s"Receive query: ${pkg.sql}")
    execStmt(ctx, pkg.sql).toPackets
  }

  def execStmt(ctx: ChannelHandlerContext, sql: String): MySQLQueryResult = {
    val converted = convertSQL(sql)
    if (sql != converted) info(s"Converted to $converted")
    if (converted.trim.toLowerCase == "select 1"
      || converted.trim.toLowerCase.startsWith("kill query ")) {
      localExecStmt(ctx, converted)
    } else {
      beExecStmt(ctx, converted)
    }
  }

  private def convertSQL(origin: String): String = origin.toLowerCase.trim match {
    case "select @@version_comment limit 1" =>
      s"select '${MySQLServerDefines.KYUUBI_SERVER_DESCRIPTION}'"
    case "select database()" =>
      "select current_database()"
    case "select database(), user() limit 1" =>
      "select current_database(), session_user() database"
    // scalastyle:off line.size.limit
    case "select @@character_set_client, @@character_set_connection, @@character_set_server, @@character_set_database limit 1" =>
      "select 'utf8mb4', 'utf8mb4', 'utf8mb4', 'utf8mb4'"
    // mysql-connector-java:8 initialized query
    case sql if sql.contains("select  @@session.auto_increment_increment as auto_increment_increment, @@character_set_client as character_set_client, @@character_set_connection as character_set_connection, @@character_set_results as character_set_results, @@character_set_server as character_set_server, @@collation_server as collation_server, @@collation_connection as collation_connection, @@init_connect as init_connect, @@interactive_timeout as interactive_timeout, @@license as license, @@lower_case_table_names as lower_case_table_names, @@max_allowed_packet as max_allowed_packet, @@net_write_timeout as net_write_timeout, @@performance_schema as performance_schema, @@query_cache_size as query_cache_size, @@query_cache_type as query_cache_type, @@sql_mode as sql_mode, @@system_time_zone as system_time_zone, @@time_zone as time_zone, @@transaction_isolation as transaction_isolation, @@wait_timeout as wait_timeout") =>
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

  private def beExecStmt(ctx: ChannelHandlerContext, sql: String): MySQLQueryResult = {
    try {
      val ssHandle = ctx.channel.attr(SESSION_HANDLE).get
      val opHandle = be.executeStatement(ssHandle, sql, runAsync = false, queryTimeout = 0)
      val opStatus = be.getOperationStatus(opHandle)
      if (opStatus.state != FINISHED) {
        throw opStatus.exception
          .getOrElse(KyuubiSQLException(s"Error operator state ${opStatus.state}"))
      }
      val tableSchema = be.getResultSetMetadata(opHandle)
      val rowSet = be.fetchResults(
        opHandle, FetchOrientation.FETCH_NEXT, Int.MaxValue, fetchLog = false)
      MySQLQueryResult(tableSchema, rowSet)
    } catch {
      case rethrow: Exception =>
        warn("Error executing statement: ", rethrow)
        throw rethrow
    }
  }

  private def localExecStmt(ctx: ChannelHandlerContext, sql: String): MySQLQueryResult = {
    MySQLQueryResult(
      MySQLField("mock_result", MySQLDataType.VAR_STRING) :: Nil,
      Seq(
        Seq(MySQLServerDefines.KYUUBI_SERVER_DESCRIPTION),
        Seq("123"),
        Seq(null)
      )
    )
  }
}
