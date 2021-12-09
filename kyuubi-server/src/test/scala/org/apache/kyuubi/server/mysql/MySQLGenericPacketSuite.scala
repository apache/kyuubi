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
import org.apache.kyuubi.server.mysql.constant.MySQLErrorCode

class MySQLGenericPacketSuite extends KyuubiFunSuite with MySQLCodecHelper {

  test("encode MySQLOKPacket") {
    val packet = MySQLOKPacket(1, 2, 3)
    val expected = decodeHex("00 02 03 02 00 00 00")
    verifyEncode(expected, packet)
  }

  test("encode MySQLErrPacket") {
    val packet = MySQLErrPacket(1, MySQLErrorCode.TOO_MANY_CONNECTIONS_EXCEPTION)
    val expected = decodeHex(
      """ff 10 04 23 30 38 30 30 34 54 6f 6f 20 6d 61 6e
        |79 20 63 6f 6e 6e 65 63 74 69 6f 6e 73
        |""".stripMargin)
    verifyEncode(expected, packet)
  }

  test("encode MySQLEofPacket") {
    val packet = MySQLEofPacket()
    val expected = decodeHex("fe 00 00 02 00")
    verifyEncode(expected, packet)
  }
}
