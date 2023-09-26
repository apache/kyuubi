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

package org.apache.kyuubi.engine.chat.operation

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.chat.WithChatEngine
import org.apache.kyuubi.operation.HiveJDBCTestHelper

class ChatOperationSuite extends HiveJDBCTestHelper with WithChatEngine {

  override def withKyuubiConf: Map[String, String] = Map(
    ENGINE_CHAT_PROVIDER.key -> "echo")

  override protected def jdbcUrl: String = jdbcConnectionUrl

  test("test echo chat provider") {
    withJdbcStatement() { stmt =>
      val result = stmt.executeQuery("Hello, Kyuubi")
      assert(result.next())
      val expected = "This is ChatKyuubi, nice to meet you!"
      assert(result.getString("reply") === expected)
      assert(!result.next())
    }
  }
}
