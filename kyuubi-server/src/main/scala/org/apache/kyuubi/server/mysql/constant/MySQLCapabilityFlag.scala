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

package org.apache.kyuubi.server.mysql.constant

sealed abstract class MySQLCapabilityFlag(val value: Int)

// https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html
object MySQLCapabilityFlag {

  object CLIENT_LONG_PASSWORD extends MySQLCapabilityFlag(0x00000001)

  object CLIENT_FOUND_ROWS extends MySQLCapabilityFlag(0x00000002)

  object CLIENT_LONG_FLAG extends MySQLCapabilityFlag(0x00000004)

  object CLIENT_CONNECT_WITH_DB extends MySQLCapabilityFlag(0x00000008)

  object CLIENT_NO_SCHEMA extends MySQLCapabilityFlag(0x00000010)

  object CLIENT_COMPRESS extends MySQLCapabilityFlag(0x00000020)

  object CLIENT_ODBC extends MySQLCapabilityFlag(0x00000040)

  object CLIENT_LOCAL_FILES extends MySQLCapabilityFlag(0x00000080)

  object CLIENT_IGNORE_SPACE extends MySQLCapabilityFlag(0x00000100)

  object CLIENT_PROTOCOL_41 extends MySQLCapabilityFlag(0x00000200)

  object CLIENT_INTERACTIVE extends MySQLCapabilityFlag(0x00000400)

  object CLIENT_SSL extends MySQLCapabilityFlag(0x00000800)

  object CLIENT_IGNORE_SIGPIPE extends MySQLCapabilityFlag(0x00001000)

  object CLIENT_TRANSACTIONS extends MySQLCapabilityFlag(0x00002000)

  object CLIENT_RESERVED extends MySQLCapabilityFlag(0x00004000)

  object CLIENT_SECURE_CONNECTION extends MySQLCapabilityFlag(0x00008000)

  object CLIENT_MULTI_STATEMENTS extends MySQLCapabilityFlag(0x00010000)

  object CLIENT_MULTI_RESULTS extends MySQLCapabilityFlag(0x00020000)

  object CLIENT_PS_MULTI_RESULTS extends MySQLCapabilityFlag(0x00040000)

  object CLIENT_PLUGIN_AUTH extends MySQLCapabilityFlag(0x00080000)

  object CLIENT_CONNECT_ATTRS extends MySQLCapabilityFlag(0x00100000)

  object CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA extends MySQLCapabilityFlag(0x00200000)

  object CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS extends MySQLCapabilityFlag(0x00400000)

  object CLIENT_SESSION_TRACK extends MySQLCapabilityFlag(0x00800000)

  // since MySQL 5.7.15, disable it for compatible
  object CLIENT_DEPRECATE_EOF extends MySQLCapabilityFlag(0x01000000)

  val handshakeValue: Int = calculateValues(
    CLIENT_LONG_PASSWORD,
    CLIENT_FOUND_ROWS,
    CLIENT_LONG_FLAG,
    CLIENT_CONNECT_WITH_DB,
    CLIENT_ODBC,
    CLIENT_IGNORE_SPACE,
    CLIENT_PROTOCOL_41,
    CLIENT_INTERACTIVE,
    CLIENT_IGNORE_SIGPIPE,
    CLIENT_TRANSACTIONS,
    CLIENT_SECURE_CONNECTION,
    CLIENT_PLUGIN_AUTH)

  val handshakeValueLower: Int = handshakeValue & 0x0000ffff

  val handshakeValueUpper: Int = handshakeValue >>> 16

  private def calculateValues(capabilities: MySQLCapabilityFlag*): Int =
    capabilities.foldLeft(0) { case (acc, item) => acc | item.value }
}
