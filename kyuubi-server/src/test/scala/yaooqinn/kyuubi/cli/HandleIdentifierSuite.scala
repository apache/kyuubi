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

package yaooqinn.kyuubi.cli

import java.nio.ByteBuffer
import java.util.UUID

import org.apache.hive.service.cli.thrift.THandleIdentifier
import org.apache.spark.SparkFunSuite

class HandleIdentifierSuite extends SparkFunSuite {

  test("handle identifier instantiating and equality") {
    // default constructor
    val pid = UUID.randomUUID()
    val sid = UUID.randomUUID()
    val handleId1 = new HandleIdentifier(pid, sid)
    assert(pid === handleId1.getPublicId)
    assert(sid === handleId1.getSecretId)
    assert(handleId1.toTHandleIdentifier.bufferForGuid() !== null)
    assert(handleId1.toTHandleIdentifier.bufferForSecret() !== null)
    assert(handleId1.toString === pid.toString)

    // constructor without any parameter
    val handleId2 = new HandleIdentifier()
    assert(pid !== handleId2.getPublicId)
    assert(sid !== handleId2.getSecretId)
    assert(handleId1 !== handleId2)
    assert(handleId1.toTHandleIdentifier !== handleId2.toTHandleIdentifier)

    // constructor with THandleIdentifier
    val handleId3 = new HandleIdentifier(handleId1.toTHandleIdentifier)
    assert(handleId1 === handleId3)

    val tHandleId = new THandleIdentifier(ByteBuffer.allocate(16), ByteBuffer.allocate(16))
    val handleId4 = new HandleIdentifier(tHandleId)
    assert(handleId1 !== handleId4)

  }
}
