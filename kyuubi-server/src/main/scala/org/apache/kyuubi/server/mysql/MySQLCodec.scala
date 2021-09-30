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

import java.nio.ByteOrder
import java.util.{List => JList}

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec._

import org.apache.kyuubi.server.mysql.MySQLCodec._
import org.apache.kyuubi.server.mysql.MySQLRichByteBuf.Implicit
import org.apache.kyuubi.server.mysql.constant._
import org.apache.kyuubi.server.mysql.constant.MySQLCommandPacketType._

object MySQLCodec {
  val PAYLOAD_LENGTH = 3
  val SEQUENCE_LENGTH = 1
}

/**
 * {{{
 * lengthFieldOffset   = 0
 * lengthFieldLength   = 3
 * lengthAdjustment    = 1
 * initialBytesToStrip = 3
 *
 * BEFORE DECODE (16 bytes)                  AFTER DECODE (13 bytes)
 * +----------+------+----------------+      +------+----------------+
 * |  Length  | Seq  | Actual Content |----->| Seq  | Actual Content |
 * | 0x00000C | 0x01 | "HELLO, WORLD" |      | 0x01 | "HELLO, WORLD" |
 * +----------+------+----------------+      +------+----------------+
 * }}}
 *
 * If the payload is larger than or equal to 2&#94;24-1 bytes the length is set to 2&#94;24-1
 * (ff ff ff) and a additional packets are sent with the rest of the payload until the payload
 * of a packet is less than 2&#94;24-1 bytes.
 */
class MySQLFrameDelimiter extends LengthFieldBasedFrameDecoder(
  ByteOrder.LITTLE_ENDIAN, 1 << 24, 0, PAYLOAD_LENGTH, SEQUENCE_LENGTH, PAYLOAD_LENGTH, true)

class MySQLPacketDecoder extends ByteToMessageDecoder {
  override def decode(ctx: ChannelHandlerContext, payload: ByteBuf, out: JList[AnyRef]): Unit = {
    require(0 == payload.readInt1, "Sequence ID of MySQL command packet must be 0")
    val cmdPacket: MySQLCommandPacket = payload.readInt1 match {
      case COM_PING.value => MySQLComPingPacket()
      case COM_QUIT.value => MySQLComQuitPacket()
      case COM_INIT_DB.value => MySQLComInitDbPacket.decode(payload)
      case COM_FIELD_LIST.value => MySQLComFieldListPacket.decode(payload)
      case COM_QUERY.value => MySQLComQueryPacket.decode(payload)
      case COM_STMT_PREPARE.value | COM_STMT_EXECUTE.value
           | COM_STMT_RESET.value | COM_STMT_CLOSE.value =>
        throw new UnsupportedOperationException(
          "Currently Kyuubi does not support server side prepared statement")
      case unsupported =>
        MySQLUnsupportedCommandPacket(MySQLCommandPacketType.valueOf(unsupported))
    }
    out.add(cmdPacket)
  }
}

class MySQLPacketEncoder extends MessageToByteEncoder[SupportsEncode] {
  override def encode(ctx: ChannelHandlerContext, msg: SupportsEncode, out: ByteBuf): Unit = {
    try msg.encode(prepareMessageHeader(out).markWriterIndex)
    catch {
      case ex: Exception =>
        out.resetWriterIndex
        MySQLErrPacket(1, MySQLErrorCode.UNKNOWN_EXCEPTION, ex.getMessage).encode(out)
    } finally updateMessageHeader(out, msg.sequenceId)
  }

  private def prepareMessageHeader(out: ByteBuf): ByteBuf =
    out.writeReserved(PAYLOAD_LENGTH + SEQUENCE_LENGTH)

  private def updateMessageHeader(out: ByteBuf, sequenceId: Int): Unit = {
    out.setMediumLE(0, out.readableBytes - PAYLOAD_LENGTH - SEQUENCE_LENGTH)
    out.setByte(PAYLOAD_LENGTH + SEQUENCE_LENGTH - 1, sequenceId)
  }
}
