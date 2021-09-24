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

package org.apache.kyuubi.engine.spark

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{FRONTEND_THRIFT_BINARY_BIND_HOST, FRONTEND_THRIFT_BINARY_BIND_PORT}
import org.apache.kyuubi.service.NoopServer

class SparkThriftBinaryFrontendServiceSuite extends KyuubiFunSuite {
  test("engine connect url use hostname") {
    val conf = new KyuubiConf()
      .set(FRONTEND_THRIFT_BINARY_BIND_HOST.key, "localhost")
      .set(FRONTEND_THRIFT_BINARY_BIND_PORT, 0)
    val service = new SparkThriftBinaryFrontendService(new NoopServer)
    intercept[IllegalStateException](service.connectionUrl)

    conf.set(KyuubiConf.ENGINE_CONNECTION_URL_USE_HOSTNAME, true)
    service.initialize(conf)
    // default use hostname
    assert(service.connectionUrl.startsWith("localhost"))

    // use ip address
    conf.set(KyuubiConf.ENGINE_CONNECTION_URL_USE_HOSTNAME, false)
    assert(service.connectionUrl.startsWith("127.0.0.1"))
  }
}
