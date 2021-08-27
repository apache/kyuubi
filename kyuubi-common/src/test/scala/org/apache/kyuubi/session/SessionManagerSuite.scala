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

import java.time.Duration

import org.apache.hive.service.rpc.thrift._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.ThriftFrontendServiceSuite

class SessionManagerSuite extends ThriftFrontendServiceSuite with Eventually {

  override val conf = KyuubiConf()
    .set(KyuubiConf.FRONTEND_BIND_PORT, 0)
    .set("kyuubi.test.server.should.fail", "false")
    .set(KyuubiConf.SESSION_CHECK_INTERVAL, Duration.ofSeconds(5).toMillis)
    .set(KyuubiConf.SESSION_IDLE_TIMEOUT, Duration.ofSeconds(5).toMillis)
    .set(KyuubiConf.OPERATION_IDLE_TIMEOUT, Duration.ofSeconds(20).toMillis)

  test("close expired operations") {
    withSessionHandle{ (client, handle) =>
      val req = new TCancelOperationReq()
      val req1 = new TGetSchemasReq(handle)
      val resp1 = client.GetSchemas(req1)

      val sessionManager = server.backendService.sessionManager
      val session = sessionManager
        .getSession(SessionHandle(handle))
        .asInstanceOf[AbstractSession]
      var lastAccessTime = session.lastAccessTime
      assert(sessionManager.getOpenSessionCount == 1)
      assert(session.lastIdleTime > 0)

      resp1.getOperationHandle
      req.setOperationHandle(resp1.getOperationHandle)
      val resp2 = client.CancelOperation(req)
      assert(resp2.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
      assert(sessionManager.getOpenSessionCount == 1)
      assert(session.lastIdleTime == 0)
      assert(lastAccessTime < session.lastAccessTime)
      lastAccessTime = session.lastAccessTime

      eventually (timeout(Span(60, Seconds)), interval(Span(1, Seconds))) {
        assert(session.lastIdleTime > lastAccessTime)
      }

      info("operation is terminated")
      assert(lastAccessTime == session.lastAccessTime)
      assert(sessionManager.getOpenSessionCount == 1)

      eventually (timeout(Span(60, Seconds)), interval(Span(1, Seconds))) {
        assert(session.lastAccessTime > lastAccessTime)
      }
      assert(sessionManager.getOpenSessionCount == 0)
    }
  }
}
