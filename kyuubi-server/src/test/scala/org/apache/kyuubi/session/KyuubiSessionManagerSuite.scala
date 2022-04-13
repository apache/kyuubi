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

import java.util.UUID

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

class KyuubiSessionManagerSuite extends KyuubiFunSuite {
  test("static batch session secret id") {
    val staticSecretId = UUID.randomUUID()
    val conf = KyuubiConf().set(KyuubiConf.SESSION_BATCH_STATIC_SECRET_ID, staticSecretId.toString)
    val protocolVersion = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1
    val sessionManager = new KyuubiSessionManager()
    sessionManager.initialize(conf)

    val batchSessionHandle = sessionManager.genBatchSessionHandle(protocolVersion)
    assert(batchSessionHandle.identifier.secretId === staticSecretId)
  }
}
