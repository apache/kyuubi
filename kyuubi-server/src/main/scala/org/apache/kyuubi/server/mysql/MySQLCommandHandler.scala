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

import java.net.InetAddress
import java.util.concurrent.{ConcurrentHashMap, ThreadPoolExecutor}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.config.KyuubiReservedKeys._
import org.apache.kyuubi.operation.FetchOrientation
import org.apache.kyuubi.operation.OperationState._
import org.apache.kyuubi.server.mysql.MySQLCommandHandler._
import org.apache.kyuubi.server.mysql.constant.MySQLCtxAttrKey._
import org.apache.kyuubi.service.BackendService
import org.apache.kyuubi.session.SessionHandle

object MySQLCommandHandler {
  val connIdCounter = new AtomicInteger
  val connIdToSessHandle = new ConcurrentHashMap[Int, SessionHandle]
}

class MySQLCommandHandler(
    serverAddr: InetAddress,
    connectionUrl: String,
    be: BackendService,
    execPool: ThreadPoolExecutor)
  extends SimpleChannelInboundHandler[MySQLCommandPacket] with Logging {

  implicit private val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(execPool)

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

  override def channelRead0(ctx: ChannelHandlerContext, packet: MySQLCommandPacket): Unit = Future {
    ensureSessionOpened(ctx)
    packet match {
      case pkt: MySQLComPingPacket => handlePing(ctx, pkt)
      case pkt: MySQLComInitDbPacket => handleInitDb(ctx, pkt)
      case pkt: MySQLComQuitPacket => handleQuit(ctx, pkt)
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

  def ensureSessionOpened(ctx: ChannelHandlerContext): Unit =
    if (ctx.channel.attr(SESSION_HANDLE).get == null) synchronized {
      if (ctx.channel.attr(SESSION_HANDLE).get == null) {
        val sessionHandle = openSession(ctx)
        ctx.channel.attr(SESSION_HANDLE).set(sessionHandle)
        val connectionId = ctx.channel.attr(CONNECTION_ID).get
        connIdToSessHandle.put(connectionId, sessionHandle)
      }
    }

  def openSession(ctx: ChannelHandlerContext): SessionHandle = synchronized {
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
      val sessionHandle = be.openSession(
        proto,
        user,
        "",
        remoteIp,
        Map(KYUUBI_CLIENT_IP_KEY -> remoteIp, KYUUBI_SERVER_IP_KEY -> serverAddr.getHostAddress) ++
          sessionConf ++ Map(
            KYUUBI_SESSION_CONNECTION_URL_KEY -> connectionUrl,
            KYUUBI_SESSION_REAL_USER_KEY -> user))
      sessionHandle
    } catch {
      case rethrow: Exception =>
        warn(s"Error opening session: ${rethrow.getMessage}")
        throw rethrow
    }
  }

  def closeSession(ctx: ChannelHandlerContext): Unit = synchronized {
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

  def handlePing(
      ctx: ChannelHandlerContext,
      pkg: MySQLComPingPacket): Seq[MySQLPacket] = {
    MySQLOKPacket(1) :: Nil
  }

  def handleInitDb(
      ctx: ChannelHandlerContext,
      pkg: MySQLComInitDbPacket): Seq[MySQLPacket] = {
    beExecuteStatement(ctx, s"use ${pkg.database}")
    MySQLOKPacket(1) :: Nil
  }

  def handleQuit(
      ctx: ChannelHandlerContext,
      pkg: MySQLComQuitPacket): Seq[MySQLPacket] = {
    closeSession(ctx)
    MySQLOKPacket(1) :: Nil
  }

  def handleQuery(
      ctx: ChannelHandlerContext,
      pkg: MySQLComQueryPacket): Seq[MySQLPacket] = {
    debug(s"Receive query: ${pkg.sql}")
    executeStatement(ctx, pkg.sql).toPackets
  }

  def executeStatement(ctx: ChannelHandlerContext, sql: String): MySQLQueryResult = {
    val newSQL = MySQLDialectHelper.convertQuery(sql)
    if (sql != newSQL) debug(s"Converted to $newSQL")

    if (MySQLDialectHelper.shouldExecuteLocal(newSQL)) {
      MySQLDialectHelper.localExecuteStatement(ctx, newSQL)
    } else {
      beExecuteStatement(ctx, newSQL)
    }
  }

  private def beExecuteStatement(ctx: ChannelHandlerContext, sql: String): MySQLQueryResult = {
    try {
      val ssHandle = ctx.channel.attr(SESSION_HANDLE).get
      val opHandle = be.executeStatement(
        ssHandle,
        sql,
        confOverlay = Map.empty,
        runAsync = false,
        queryTimeout = 0)
      val opStatus = be.getOperationStatus(opHandle)
      if (opStatus.state != FINISHED) {
        throw opStatus.exception
          .getOrElse(KyuubiSQLException(s"Error operator state ${opStatus.state}"))
      }
      val resultSetMetadata = be.getResultSetMetadata(opHandle)
      val rowSet = be.fetchResults(
        opHandle,
        FetchOrientation.FETCH_NEXT,
        Int.MaxValue,
        fetchLog = false)
      MySQLQueryResult(resultSetMetadata.getSchema, rowSet)
    } catch {
      case rethrow: Exception =>
        warn("Error executing statement: ", rethrow)
        throw rethrow
    }
  }
}
