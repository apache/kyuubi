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

class MySQLCommandPacketSuite extends KyuubiFunSuite with MySQLCodecHelper {

  test("decode MySQLComInitDbPacket") {
    val payload = decodeHex("6b 79 75 75 62 69")
    val expected = MySQLComInitDbPacket("kyuubi")
    verifyDecode(MySQLComInitDbPacket, payload, expected) { (decoded, expected) =>
      assert(decoded === expected)
    }
  }

  test("decode MySQLComFieldListPacket") {
    val payload = decodeHex("6b 79 75 75 62 69 00 2a")
    val expected = MySQLComFieldListPacket("kyuubi", "*")
    verifyDecode(MySQLComFieldListPacket, payload, expected) { (decoded, expected) =>
      assert(decoded === expected)
    }
  }

  test("decode MySQLComQueryPacket") {
    val payload = decodeHex(
      """73 65 6c 65 63 74 20 6b 79 75 75 62 69 5f 76 65
        |72 73 69 6f 6e 28 29
        |""".stripMargin)
    val expected = MySQLComQueryPacket("select kyuubi_version()")
    verifyDecode(MySQLComQueryPacket, payload, expected) { (decoded, expected) =>
      assert(decoded === expected)
    }
  }
}
