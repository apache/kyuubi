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

import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.server.mysql.MySQLErrPacket
import org.apache.kyuubi.server.mysql.constant.MySQLCapabilityFlag
import org.apache.kyuubi.server.mysql.constant.MySQLCtxAttrKey._

class MySQLAuthHandler extends SimpleChannelInboundHandler[ByteBuf] with Logging {

  private val authenticationEngine = new MySQLAuthenticationEngine

  @volatile private var authenticated = false

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    val nextConnectionId = authenticationEngine.handshake(ctx)
    debug(s"Receive new MySQL connection: $nextConnectionId")
    ctx.channel.attr(CONNECTION_ID).set(nextConnectionId)
    ctx.channel.attr(CAPABILITY_FLAG).set(MySQLCapabilityFlag.handshakeValue)
    ctx.fireChannelActive
  }

  override def channelRead0(ctx: ChannelHandlerContext, payload: ByteBuf): Unit = {
    if (authenticated) {
      ctx.fireChannelRead(payload.retainedSlice)
    } else {
      authenticated = authenticate(ctx, payload)
    }
  }

  private def authenticate(ctx: ChannelHandlerContext, payload: ByteBuf): Boolean = {
    try {
      val authResult = authenticationEngine.authenticate(ctx, payload)
      if (authResult.finished) {
        ctx.channel.attr(USER).set(authResult.user)
        ctx.channel.attr(REMOTE_IP).set(authResult.ip)
        ctx.channel.attr(DATABASE).set(authResult.database)
        return true
      }
    } catch {
      case cause: Exception =>
        error("Exception occur: ", cause)
        ctx.writeAndFlush(MySQLErrPacket(cause))
        ctx.close
    }
    false
  }
}
