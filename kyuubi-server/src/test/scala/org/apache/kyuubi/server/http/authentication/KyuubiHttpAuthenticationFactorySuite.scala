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

package org.apache.kyuubi.server.http.authentication

import org.eclipse.jetty.server.handler.HandlerWrapper

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.server.ui.JettyServer

class KyuubiHttpAuthenticationFactorySuite extends KyuubiFunSuite {

  test("wrapped handler follows Jetty server lifecycle") {
    val server = JettyServer("KyuubiHttpAuthenticationFactorySuite", "localhost", 0, 8, 5000, false)
    val handler = new HandlerWrapper
    val factory = new KyuubiHttpAuthenticationFactory(KyuubiConf())
    val wrapper = factory.httpHandlerWrapperFactory.wrapHandler(handler)

    try {
      server.start()
      server.addHandler(wrapper)
      assert(wrapper.isStarted)
      assert(handler.isStarted)
    } finally {
      server.stop()
    }

    assert(wrapper.isStopped)
    assert(handler.isStopped)
  }
}
