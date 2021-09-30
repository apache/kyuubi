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

import org.apache.kyuubi.server.mysql._
import org.apache.kyuubi.server.mysql.MySQLRichByteBuf.Implicit
import org.apache.kyuubi.server.mysql.constant._

case class MySQLAuthSwitchRequestPacket(
  sequenceId: Int,
  authPluginName: String,
  authPluginData: MySQLNativePassword.PluginData
) extends MySQLPacket with SupportsEncode {

  def header: Int = 0xfe

  override def encode(payload: ByteBuf): Unit = {
    payload.writeInt1(header)
    payload.writeStringNul(authPluginName)
    payload.writeStringNul(new String(authPluginData.full))
  }
}

object MySQLAuthSwitchResponsePacket extends SupportsDecode[MySQLAuthSwitchResponsePacket] {

  override def decode(payload: ByteBuf): MySQLAuthSwitchResponsePacket = {
    val _sequenceId = payload.readInt1
    val _authPluginResponse = payload.readStringEOFByBytes
    MySQLAuthSwitchResponsePacket(_sequenceId, _authPluginResponse)
  }
}

case class MySQLAuthSwitchResponsePacket(
  sequenceId: Int,
  authPluginResponse: Array[Byte]
) extends MySQLPacket

case class MySQLHandshakePacket(
  connectionId: Int,
  authPluginData: MySQLNativePassword.PluginData
) extends MySQLPacket with SupportsEncode {

  def protocolVersion: Int = MySQLServerDefines.PROTOCOL_VERSION

  def serverVersion: String = MySQLServerDefines.MYSQL_KYUUBI_SERVER_VERSION

  def statusFlag: MySQLStatusFlag = MySQLStatusFlag.SERVER_STATUS_AUTOCOMMIT

  def charset: Int = MySQLServerDefines.CHARSET

  def capabilityFlagsLower: Int = MySQLCapabilityFlag.handshakeValueLower

  def capabilityFlagsUpper: Int = MySQLCapabilityFlag.handshakeValueUpper

  def authPluginName: String = MySQLAuthenticationMethod.NATIVE_PASSWORD.method

  override def sequenceId: Int = 0

  override def encode(payload: ByteBuf): Unit = {
    payload.writeInt1(protocolVersion)
    payload.writeStringNul(serverVersion)
    payload.writeInt4(connectionId)
    payload.writeStringNul(new String(authPluginData.part1))
    payload.writeInt2(capabilityFlagsLower)
    payload.writeInt1(charset)
    payload.writeInt2(statusFlag.value)
    payload.writeInt2(capabilityFlagsUpper)
    payload.writeInt1(if (isClientPluginAuth) authPluginData.full.length + 1 else 0)
    payload.writeReserved(10)
    if (isClientSecureConnection) payload.writeStringNul(new String(authPluginData.part2))
    if (isClientPluginAuth) payload.writeStringNul(authPluginName)
  }

  private def isClientSecureConnection =
    (capabilityFlagsLower & MySQLCapabilityFlag.CLIENT_SECURE_CONNECTION.value & 0x00000ffff) != 0

  private def isClientPluginAuth =
    (capabilityFlagsUpper & MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH.value >> 16) != 0
}

object MySQLHandshakeResponse41Packet extends SupportsDecode[MySQLHandshakeResponse41Packet] {
  override def decode(payload: ByteBuf): MySQLHandshakeResponse41Packet = {
    val sequenceId = payload.readInt1
    val capabilityFlags = payload.readInt4
    val maxPacketSize = payload.readInt4
    val characterSet = payload.readInt1
    payload.skipReserved(23)
    val username = payload.readStringNul
    val authResponse = readAuthResponse(payload, capabilityFlags)
    val database = readDatabase(payload, capabilityFlags)
    val authPluginName = readAuthPluginName(payload, capabilityFlags)
    MySQLHandshakeResponse41Packet(
      sequenceId, capabilityFlags, maxPacketSize, characterSet,
      username, authResponse, database, authPluginName
    )
  }

  private def readAuthResponse(payload: ByteBuf, capabilityFlags: Int): Array[Byte] = {
    if (0 != (capabilityFlags & MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA.value)) {
      return payload.readStringLenencByBytes
    }
    if (0 != (capabilityFlags & MySQLCapabilityFlag.CLIENT_SECURE_CONNECTION.value)) {
      val length = payload.readInt1
      return payload.readStringFixByBytes(length)
    }
    payload.readStringNulByBytes
  }

  private def readDatabase(payload: ByteBuf, capabilityFlags: Int): String =
    if (0 != (capabilityFlags & MySQLCapabilityFlag.CLIENT_CONNECT_WITH_DB.value)) {
      payload.readStringNul
    } else null

  private def readAuthPluginName(payload: ByteBuf, capabilityFlags: Int): String =
    if (0 != (capabilityFlags & MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH.value)) payload.readStringNul
    else null
}

case class MySQLHandshakeResponse41Packet(
  sequenceId: Int,
  capabilityFlags: Int,
  maxPacketSize: Int,
  characterSet: Int,
  username: String,
  authResponse: Array[Byte],
  database: String,
  authPluginName: String
) extends MySQLPacket with SupportsEncode { // seems SupportsEncode is not required

  override def encode(payload: ByteBuf): Unit = {
    payload.writeInt4(capabilityFlags)
    payload.writeInt4(maxPacketSize)
    payload.writeInt1(characterSet)
    payload.writeReserved(23)
    payload.writeStringNul(username)
    writeAuthResponse(payload)
    writeDatabase(payload)
    writeAuthPluginName(payload)
  }

  private def writeAuthResponse(payload: ByteBuf): Unit =
    if ((capabilityFlags & MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA.value) != 0) {
      payload.writeStringLenenc(new String(authResponse))
    } else if ((capabilityFlags & MySQLCapabilityFlag.CLIENT_SECURE_CONNECTION.value) != 0) {
      payload.writeInt1(authResponse.length)
      payload.writeBytes(authResponse)
    } else {
      payload.writeStringNul(new String(authResponse))
    }

  private def writeDatabase(payload: ByteBuf): Unit =
    if ((capabilityFlags & MySQLCapabilityFlag.CLIENT_CONNECT_WITH_DB.value) != 0) {
      payload.writeStringNul(database)
    }

  private def writeAuthPluginName(payload: ByteBuf): Unit =
    if ((capabilityFlags & MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH.value) != 0) {
      payload.writeStringNul(authPluginName)
    }
}
