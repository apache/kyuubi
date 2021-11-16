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

import java.lang.{Boolean => JBoolean}
import java.math.BigDecimal
import java.sql.Timestamp
import java.time.LocalDateTime

import io.netty.buffer.ByteBuf

import org.apache.kyuubi.server.mysql.MySQLDateTimeUtils._
import org.apache.kyuubi.server.mysql.MySQLRichByteBuf.Implicit
import org.apache.kyuubi.server.mysql.constant.{MySQLDataType, MySQLServerDefines}

case class MySQLFieldCountPacket(
    sequenceId: Int,
    columnCount: Int
) extends MySQLPacket with SupportsEncode {

  override def encode(payload: ByteBuf): Unit = {
    payload.writeIntLenenc(columnCount)
  }
}

case class MySQLColumnDefinition41Packet(
    sequenceId: Int,
    flags: Int,
    name: String,
    columnLength: Int,
    columnType: MySQLDataType,
    decimals: Int
) extends MySQLPacket with SupportsEncode {

  def nextLength: Int = 0x0c

  def characterSet: Int = MySQLServerDefines.CHARSET

  def catalog: String = ""

  def database: String = ""

  def table: String = ""

  def originalTable: String = ""

  def originalName: String = ""

  def containDefaultValues: Boolean = false

  override def encode(payload: ByteBuf): Unit = {
    payload.writeStringLenenc(catalog)
    payload.writeStringLenenc(database)
    payload.writeStringLenenc(table)
    payload.writeStringLenenc(originalTable)
    payload.writeStringLenenc(name)
    payload.writeStringLenenc(originalName)
    payload.writeIntLenenc(nextLength)
    payload.writeInt2(characterSet)
    payload.writeInt4(columnLength)
    payload.writeInt1(columnType.value)
    payload.writeInt2(flags)
    payload.writeInt1(decimals)
    payload.writeReserved(2)
    if (containDefaultValues) {
      payload.writeIntLenenc(0)
      payload.writeStringLenenc("")
    }
  }
}

case class MySQLTextResultSetRowPacket(
    sequenceId: Int,
    row: Seq[Any]
) extends MySQLPacket with SupportsEncode {

  private def nullVal = 0xfb

  override def encode(payload: ByteBuf): Unit = {
    row.foreach {
      case null => payload.writeInt1(nullVal)
      // TODO check all possible data types returned from backend service
      case bytes: Array[Byte] => payload.writeBytesLenenc(bytes)
      case ts: Timestamp if ts.getNanos == 0 =>
        payload.writeStringLenenc(ts.toString.split("\\.")(0))
      case decimal: BigDecimal => payload.writeStringLenenc(decimal.toPlainString)
      case JBoolean.TRUE | true => payload.writeBytesLenenc(Array[Byte]('1'))
      case JBoolean.FALSE | false => payload.writeBytesLenenc(Array[Byte]('0'))
      case time: LocalDateTime => payload.writeStringLenenc(dtFmt.format(time))
      case other => payload.writeStringLenenc(other.toString)
    }
  }
}
