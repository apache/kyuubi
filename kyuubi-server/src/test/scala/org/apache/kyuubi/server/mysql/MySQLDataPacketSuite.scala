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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.server.mysql.constant.MySQLDataType

class MySQLDataPacketSuite extends KyuubiFunSuite with MySQLCodecHelper {

  test("encode MySQLFieldCountPacket") {
    val packet = MySQLFieldCountPacket(1, 1)
    val expected = decodeHex("01")
    verifyEncode(expected, packet)
  }

  test("encode MySQLColumnDefinition41Packet") {
    val packet = MySQLColumnDefinition41Packet(1, 0, "UDF()", 100, MySQLDataType.VAR_STRING, 0)
    val expected = decodeHex(
      """00 00 00 00 05 55 44 46 28 29 00 0c 2d 00 64 00
        |00 00 fd 00 00 00 00 00
        |""".stripMargin)
    verifyEncode(expected, packet)
  }

  test("encode MySQLTextResultSetRowPacket") {
    val packet = MySQLTextResultSetRowPacket(2, Seq("1.4.0-SNAPSHOT"))
    val expected = decodeHex("0e 31 2e 34 2e 30 2d 53 4e 41 50 53 48 4f 54")
    verifyEncode(expected, packet)
  }
}
