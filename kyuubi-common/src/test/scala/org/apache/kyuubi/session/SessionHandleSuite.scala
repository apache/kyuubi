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

package org.apache.kyuubi.session

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.shade.hive.service.rpc.thrift.TProtocolVersion

class SessionHandleSuite extends KyuubiFunSuite {
  test("SessionHandle") {
    val h1 = SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10)
    val t1 = h1.toTSessionHandle
    TProtocolVersion.values().foreach { proto =>
      assert(h1 === SessionHandle(t1, proto))
    }
    TProtocolVersion.values().foreach { proto =>
      assert(h1 === SessionHandle(h1.identifier, proto))
    }
    assert(h1 === SessionHandle(t1))
  }
}
