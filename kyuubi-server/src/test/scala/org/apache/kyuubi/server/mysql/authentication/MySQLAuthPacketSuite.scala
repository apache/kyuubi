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

import java.util

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.server.mysql.MySQLCodecHelper

class MySQLAuthPacketSuite extends KyuubiFunSuite with MySQLCodecHelper {

  private val authPluginData = {
    val part1 = decodeHex("77 37 34 35 45 51 55 65").array
    val part2 = decodeHex("69 44 57 32 44 44 33 4e 6d 36 69 74").array
    MySQLNativePassword.PluginData(part1, part2)
  }

  test("encode MySQLHandshakePacket") {
    val packet = MySQLHandshakePacket(2, authPluginData)
    val expected = decodeHex(
      """0a 35 2e 37 2e 32 32 2d 4b 79 75 75 62 69 2d 53
        |65 72 76 65 72 20 31 2e 34 2e 30 2d 53 4e 41 50
        |53 48 4f 54 00 02 00 00 00 77 37 34 35 45 51 55
        |65 00 4f b7 2d 02 00 08 00 15 00 00 00 00 00 00
        |00 00 00 00 69 44 57 32 44 44 33 4e 6d 36 69 74
        |00 6d 79 73 71 6c 5f 6e 61 74 69 76 65 5f 70 61
        |73 73 77 6f 72 64 00
        |""".stripMargin)
    verifyEncode(expected, packet)
  }

  test("decode MySQLHandshakeResponse41Packet") {
    val payload = decodeHex(
      """01 85 a6 ff 19 00 00 00 01 2d 00 00 00 00 00 00
        |00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00
        |00 63 68 65 6e 67 70 61 6e 00 14 15 c6 82 8e 53
        |67 20 3a 44 f3 d1 3e 62 f8 2d 20 38 3c 75 94 6d
        |79 73 71 6c 5f 6e 61 74 69 76 65 5f 70 61 73 73
        |77 6f 72 64 00
        |""".stripMargin)
    val expected = MySQLHandshakeResponse41Packet(
      1,
      0x19ffa685,
      16777216,
      0x2d,
      "chengpan",
      decodeHex(
        """15 c6 82 8e 53 67 20 3a 44 f3 d1 3e 62 f8 2d 20
          |38 3c 75 94
          |""".stripMargin).array,
      null,
      "mysql_native_password")
    verifyDecode(MySQLHandshakeResponse41Packet, payload, expected) { (decoded, expected) =>
      assert(decoded.sequenceId === expected.sequenceId)
      assert(decoded.capabilityFlags === expected.capabilityFlags)
      assert(decoded.maxPacketSize === expected.maxPacketSize)
      assert(decoded.characterSet === expected.characterSet)
      assert(decoded.username === expected.username)
      assert(util.Arrays.equals(decoded.authResponse, expected.authResponse))
      assert(decoded.database === expected.database)
      assert(decoded.authPluginName === expected.authPluginName)
    }
  }

  test("encode MySQLAuthSwitchRequestPacket") {
    val packet = MySQLAuthSwitchRequestPacket(
      1,
      MySQLAuthenticationMethod.NATIVE_PASSWORD.method,
      authPluginData)
    val expected = decodeHex(
      """fe 6d 79 73 71 6c 5f 6e 61 74 69 76 65 5f 70 61
        |73 73 77 6f 72 64 00 77 37 34 35 45 51 55 65 69
        |44 57 32 44 44 33 4e 6d 36 69 74 00
        |""".stripMargin)
    verifyEncode(expected, packet)
  }

  test("decode MySQLAuthSwitchResponsePacket") {
    val payloadHex = decodeHex(
      """14 00 00 03 f4 17 96 1f 79 f3 ac 10 0b da a6 b3
        |b5 c2 0e ab 59 85 ff b8
        |""".stripMargin)
    val expectedAuthPluginResponse = decodeHex(
      """00 00 03 f4 17 96 1f 79 f3 ac 10 0b da a6 b3 b5
        |c2 0e ab 59 85 ff b8
        |""".stripMargin).array

    verifyDecode(
      MySQLAuthSwitchResponsePacket,
      payloadHex,
      MySQLAuthSwitchResponsePacket(20, expectedAuthPluginResponse)) { (decoded, expected) =>
      assert(decoded.sequenceId === expected.sequenceId)
      assert(util.Arrays.equals(decoded.authPluginResponse, expected.authPluginResponse))
    }
  }
}
