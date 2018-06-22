/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.session

import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.SparkFunSuite

class SessionHandleSuite extends SparkFunSuite {

  test("session handle basic tests") {
    val proto = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8
    val handle1 = new SessionHandle(proto)
    assert(handle1.getHandleIdentifier === handle1.handleId)
    assert(handle1.getProtocolVersion === proto)
    assert(handle1.toTSessionHandle.isSetSessionId)
    assert(handle1.toTSessionHandle.getSessionId ===
      handle1.getHandleIdentifier.toTHandleIdentifier)
    assert(handle1.toString.contains(handle1.getHandleIdentifier.toString))

    val handle2 = new SessionHandle(handle1.toTSessionHandle)
    assert(handle2.getHandleIdentifier === handle2.handleId)
//    assert(handle2.getProtocolVersion === proto)
    assert(handle2.toTSessionHandle.isSetSessionId)
    assert(handle2.toTSessionHandle.getSessionId ===
      handle2.getHandleIdentifier.toTHandleIdentifier)
    assert(handle1 === handle2)
  }
}
