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

import io.netty.buffer.ByteBuf

import org.apache.kyuubi.server.mysql.MySQLRichByteBuf.Implicit
import org.apache.kyuubi.server.mysql.constant.MySQLCommandPacketType

sealed abstract class MySQLCommandPacket(
    cmdType: MySQLCommandPacketType
) extends MySQLPacket {
  override def sequenceId: Int = 0
}

case class MySQLComPingPacket()
  extends MySQLCommandPacket(MySQLCommandPacketType.COM_PING)

case class MySQLComQuitPacket()
  extends MySQLCommandPacket(MySQLCommandPacketType.COM_QUIT)

object MySQLComInitDbPacket extends SupportsDecode[MySQLComInitDbPacket] {
  override def decode(payload: ByteBuf): MySQLComInitDbPacket = {
    val schema = payload.readStringEOF
    MySQLComInitDbPacket(schema)
  }
}
case class MySQLComInitDbPacket(
    database: String
) extends MySQLCommandPacket(MySQLCommandPacketType.COM_INIT_DB)

object MySQLComFieldListPacket extends SupportsDecode[MySQLComFieldListPacket] {
  override def decode(payload: ByteBuf): MySQLComFieldListPacket = {
    val table = payload.readStringNul
    val fieldWildcard = payload.readStringEOF
    MySQLComFieldListPacket(table, fieldWildcard)
  }
}

case class MySQLComFieldListPacket(
    table: String,
    fieldWildcard: String
) extends MySQLCommandPacket(MySQLCommandPacketType.COM_FIELD_LIST)

object MySQLComQueryPacket extends SupportsDecode[MySQLComQueryPacket] {
  override def decode(payload: ByteBuf): MySQLComQueryPacket = {
    val sql = payload.readStringEOF
    MySQLComQueryPacket(sql)
  }
}

case class MySQLComQueryPacket(
    sql: String
) extends MySQLCommandPacket(MySQLCommandPacketType.COM_QUERY)

case class MySQLUnsupportedCommandPacket(
    cmdType: MySQLCommandPacketType
) extends MySQLCommandPacket(cmdType)
