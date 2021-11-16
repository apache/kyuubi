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

package org.apache.kyuubi.server.mysql.authentication

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

import scala.util.Random

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext

import org.apache.kyuubi.server.mysql._
import org.apache.kyuubi.server.mysql.authentication.MySQLAuthentication._
import org.apache.kyuubi.server.mysql.constant._

object MySQLAuthentication {

  private val seed: Array[Byte] = Array(
    // format: off
    'a', 'b', 'e', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
    'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    // format: on

  def randomBytes(length: Int): Array[Byte] = {
    val result = new Array[Byte](length)
    var i = 0
    while (i < length) {
      result(i) = seed(Random.nextInt(seed.length))
      i = i + 1
    }
    result
  }

  final val connIdCounter = new AtomicInteger
}

object MySQLConnectionPhase extends Enumeration {
  type MySQLConnectionPhase = Value

  val INITIAL_HANDSHAKE, AUTH_PHASE_FAST_PATH, AUTHENTICATION_METHOD_MISMATCH = Value
}

sealed abstract class MySQLAuthenticationMethod(val method: String)

object MySQLAuthenticationMethod {

  object OLD_PASSWORD extends MySQLAuthenticationMethod("mysql_old_password")

  // Currently, it's the ONLY supported authentication method
  object NATIVE_PASSWORD extends MySQLAuthenticationMethod("mysql_native_password")

  object CLEAR_TEXT extends MySQLAuthenticationMethod("mysql_clear_password")

  object WINDOWS_NATIVE extends MySQLAuthenticationMethod("authentication_windows_client")

  object SHA256 extends MySQLAuthenticationMethod("sha256_password")
}

case class AuthenticationResult(
    user: String,
    ip: String,
    database: String,
    finished: Boolean
)

object AuthenticationResult {
  def finished(username: String, ip: String, database: String): AuthenticationResult =
    new AuthenticationResult(username, ip, database, true)

  def continued: AuthenticationResult =
    new AuthenticationResult(null, null, null, false)

  def continued(username: String, ip: String, database: String): AuthenticationResult =
    new AuthenticationResult(username, ip, database, false)
}

class MySQLAuthenticationEngine {
  private final val authenticator = new MySQLNativePassword
  private final val currentSeqId = new AtomicInteger
  private var connectionPhase = MySQLConnectionPhase.INITIAL_HANDSHAKE
  private var authResponse: Array[Byte] = _
  private var authResult: AuthenticationResult = _

  def handshake(ctx: ChannelHandlerContext): Int = {
    val connectionId = connIdCounter.getAndIncrement
    connectionPhase = MySQLConnectionPhase.AUTH_PHASE_FAST_PATH
    ctx.writeAndFlush(MySQLHandshakePacket(connectionId, authenticator.pluginData))
    connectionId
  }

  def authenticate(ctx: ChannelHandlerContext, buf: ByteBuf): AuthenticationResult = {
    connectionPhase match {
      case MySQLConnectionPhase.AUTHENTICATION_METHOD_MISMATCH =>
        authenticationMethodMismatch(buf)
      case MySQLConnectionPhase.AUTH_PHASE_FAST_PATH =>
        authResult = authPhaseFastPath(ctx, buf)
        if (!authResult.finished) return authResult
      case _ => // never happen
    }
    val seqId = currentSeqId.incrementAndGet
    val responsePacket = authenticator
      .login(authResult.user, remoteAddress(ctx), authResponse, authResult.database)
      .map(createErrorPacket(ctx, _, seqId))
      .getOrElse(MySQLOKPacket(seqId))
    ctx.writeAndFlush(responsePacket)

    AuthenticationResult.finished(authResult.user, remoteAddress(ctx), authResult.database)
  }

  private def authPhaseFastPath(ctx: ChannelHandlerContext, buf: ByteBuf): AuthenticationResult = {
    val packet = MySQLHandshakeResponse41Packet.decode(buf)
    authResponse = packet.authResponse
    currentSeqId.set(packet.sequenceId)
    // always switch to mysql_native_password since Kyuubi Server only support this method
    if (isClientPluginAuth(packet)
      && packet.authPluginName != MySQLAuthenticationMethod.NATIVE_PASSWORD.method) {
      connectionPhase = MySQLConnectionPhase.AUTHENTICATION_METHOD_MISMATCH
      ctx.writeAndFlush(MySQLAuthSwitchRequestPacket(
        currentSeqId.incrementAndGet,
        MySQLAuthenticationMethod.NATIVE_PASSWORD.method,
        authenticator.pluginData))
      return AuthenticationResult.continued(packet.username, remoteAddress(ctx), packet.database)
    }
    AuthenticationResult.finished(packet.username, remoteAddress(ctx), packet.database)
  }

  private def isClientPluginAuth(packet: MySQLHandshakeResponse41Packet) =
    (packet.capabilityFlags & MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH.value) != 0

  private def authenticationMethodMismatch(buf: ByteBuf): Unit = {
    val packet = MySQLAuthSwitchResponsePacket.decode(buf)
    currentSeqId.set(packet.sequenceId)
    authResponse = packet.authPluginResponse
  }

  private def createErrorPacket(
      ctx: ChannelHandlerContext,
      errorCode: MySQLErrorCode,
      seqId: Int
  ): MySQLErrPacket = errorCode match {
    case MySQLErrorCode.ER_DBACCESS_DENIED_ERROR => MySQLErrPacket(
        seqId,
        MySQLErrorCode.ER_DBACCESS_DENIED_ERROR,
        authResult.user,
        remoteAddress(ctx),
        authResult.database)
    case _ => MySQLErrPacket(
        seqId,
        MySQLErrorCode.ER_ACCESS_DENIED_ERROR,
        authResult.user,
        remoteAddress(ctx),
        errorMessage)
  }

  private def errorMessage = if (authResponse.nonEmpty) "YES" else "NO"

  private def remoteAddress(ctx: ChannelHandlerContext): String = {
    ctx.channel.remoteAddress match {
      case address: InetSocketAddress => address.getAddress.getHostAddress
      case other => other.toString
    }
  }
}
