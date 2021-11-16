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

import java.sql.SQLException

import io.netty.buffer.ByteBuf

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.server.mysql.MySQLRichByteBuf.Implicit
import org.apache.kyuubi.server.mysql.constant._

case class MySQLOKPacket(
    sequenceId: Int = 0,
    affectedRows: Long = 0L,
    lastInsertId: Long = 0L
) extends MySQLPacket with SupportsEncode {

  def header: Int = 0x00

  def statusFlag: MySQLStatusFlag = MySQLStatusFlag.SERVER_STATUS_AUTOCOMMIT

  def warnings: Int = 0

  def info: String = ""

  override def encode(payload: ByteBuf): Unit = {
    payload.writeInt1(header)
    payload.writeIntLenenc(affectedRows)
    payload.writeIntLenenc(lastInsertId)
    payload.writeInt2(statusFlag.value)
    payload.writeInt2(warnings)
    payload.writeStringEOF(info)
  }
}

object MySQLErrPacket {
  def apply(cause: Throwable): MySQLErrPacket = {
    cause match {
      case kse: KyuubiSQLException if kse.getCause != null =>
        // prefer brief nested error message instead of whole stacktrace
        apply(kse.getCause)
      case e: Exception if e.getMessage contains "NoSuchDatabaseException" =>
        MySQLErrPacket(1, MySQLErrorCode.ER_BAD_DB_ERROR, cause.getMessage)
      case se: SQLException if se.getSQLState == null =>
        MySQLErrPacket(1, MySQLErrorCode.ER_INTERNAL_ERROR, cause.getMessage)
      case se: SQLException =>
        MySQLErrPacket(1, MySQLErrorCode(se.getErrorCode, se.getSQLState, se.getMessage))
      case _ =>
        MySQLErrPacket(1, MySQLErrorCode.UNKNOWN_EXCEPTION, cause.getMessage)
    }
  }
}

case class MySQLErrPacket(
    sequenceId: Int,
    sqlErrorCode: MySQLErrorCode,
    errMsgArgs: String*
) extends MySQLPacket with SupportsEncode {

  def header: Int = 0xff

  def sqlStateMarker: String = "#"

  def errorCode: Int = sqlErrorCode.errorCode

  def sqlState: String = sqlErrorCode.sqlState

  def errorMessage: String = sqlErrorCode.errorMessage format (errMsgArgs: _*)

  override def encode(payload: ByteBuf): Unit = {
    payload.writeInt1(header)
    payload.writeInt2(errorCode)
    payload.writeStringFix(sqlStateMarker)
    payload.writeStringFix(sqlState)
    payload.writeStringEOF(errorMessage)
  }
}

case class MySQLEofPacket(
    sequenceId: Int = 0
) extends MySQLPacket with SupportsEncode {

  def header: Int = 0xfe

  def warnings: Int = 0

  def statusFlags: MySQLStatusFlag = MySQLStatusFlag.SERVER_STATUS_AUTOCOMMIT

  override def encode(payload: ByteBuf): Unit = {
    payload.writeInt1(header)
    payload.writeInt2(warnings)
    payload.writeInt2(statusFlags.value)
  }
}
