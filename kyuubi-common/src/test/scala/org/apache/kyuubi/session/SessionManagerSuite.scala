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

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.FrontendServiceSuite

class SessionManagerSuite extends FrontendServiceSuite {

  override val conf = KyuubiConf()
    .set(KyuubiConf.FRONTEND_BIND_PORT, 0)
    .set("kyuubi.test.server.should.fail", "false")
    .set(KyuubiConf.SESSION_CHECK_INTERVAL, Duration.ofSeconds(5).toMillis)
    .set(KyuubiConf.SESSION_TIMEOUT, Duration.ofSeconds(5).toMillis)

  def waitFor(waitForTime: Long)(f: () => Boolean): Unit = {
    val startTime = System.currentTimeMillis()
    var spendTime = System.currentTimeMillis() - startTime
    while (f()) {
      spendTime = System.currentTimeMillis() - startTime
      if (spendTime > waitForTime) {
        throw new RuntimeException()
      }
    }
    info(s"Spend time $spendTime ms")
  }

  test("close expired operations") {
    sessionConf.put(
      KyuubiConf.OPERATION_IDLE_TIMEOUT.key,
      String.valueOf(Duration.ofSeconds(20).toMillis)
    )

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

      waitFor(Duration.ofSeconds(60).toMillis) { () =>
        {
          session.lastIdleTime == 0
        }
      }

      info("operation is terminated")
      assert(lastAccessTime == session.lastAccessTime)
      assert(session.lastIdleTime > lastAccessTime)
      assert(sessionManager.getOpenSessionCount == 1)

      waitFor(Duration.ofSeconds(60).toMillis) { () =>
        lastAccessTime == session.lastAccessTime
      }
      assert(sessionManager.getOpenSessionCount == 0)
    }
  }
}
