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

import io.netty.buffer.{ByteBuf, ByteBufUtil, Unpooled}

import org.apache.kyuubi.KyuubiFunSuite

trait MySQLCodecHelper extends KyuubiFunSuite {

  def decodeHex(hexDump: String): ByteBuf = {
    val compact = hexDump.replaceAll("(?s)\\s", "")
    val bytes = ByteBufUtil.decodeHexDump(compact)
    Unpooled.copiedBuffer(bytes)
  }

  def verifyDecode[T <: MySQLPacket](
      decoder: SupportsDecode[T],
      payload: ByteBuf,
      expected: T
  )(assertion: (T, T) => Unit): Unit = {
    val decoded = decoder.decode(payload)
    assertion(decoded, expected)
  }

  def verifyEncode(expected: ByteBuf, packet: SupportsEncode): Unit = {
    val encoded = Unpooled.buffer()
    packet.encode(encoded)
    assert(encoded === expected)
  }
}
