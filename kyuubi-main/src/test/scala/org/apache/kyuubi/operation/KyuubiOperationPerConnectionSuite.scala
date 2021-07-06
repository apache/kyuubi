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

package org.apache.kyuubi.operation

import java.util.concurrent.CountDownLatch

import org.apache.hive.service.rpc.thrift.{TExecuteStatementReq, TGetOperationStatusReq, TOperationState, TStatusCode}
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{KyuubiSQLException, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf

/**
 * UT with Connection level engine shared cost much time, only run basic jdbc tests.
 */
class KyuubiOperationPerConnectionSuite extends WithKyuubiServer with JDBCTestUtils {

  override protected def jdbcUrl: String = getJdbcUrl

  override protected val conf: KyuubiConf = {
    KyuubiConf().set(KyuubiConf.ENGINE_SHARE_LEVEL, "connection")
  }

  test("KYUUBI #647 - engine crash") {
    withSessionHandle { (client, handle) =>
      val executeStmtReq = new TExecuteStatementReq()
      executeStmtReq.setStatement("select java_method('java.lang.System', 'exit', 1)")
      executeStmtReq.setSessionHandle(handle)
      executeStmtReq.setRunAsync(true)
      val executeStmtResp = client.ExecuteStatement(executeStmtReq)

      eventually(timeout(10.seconds), interval(500.milliseconds)) {
        val getOpStatusReq = new TGetOperationStatusReq(executeStmtResp.getOperationHandle)
        val getOpStatusResp = client.GetOperationStatus(getOpStatusReq)
        assert(getOpStatusResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
        assert(getOpStatusResp.getOperationState === TOperationState.ERROR_STATE)
      }
    }
  }

  test("submit spark app timeout with last log output") {
    @volatile var appIsRunning = false
    val lock = new CountDownLatch(1)
    new Thread(() => {
      while (!appIsRunning) { Thread.sleep(100) }
      try {
        withSessionConf()(Map(KyuubiConf.ENGINE_INIT_TIMEOUT.key -> "3000"))(Map.empty) {
          withJdbcStatement() { statement =>
            val exception = intercept[KyuubiSQLException] {
              statement.execute("select 1")
            }
            assert(exception.getMessage.contains("Failed to detect the root cause"))
            assert(exception.getMessage.contains("The last line log"))
          }
        }
      } finally {
        lock.countDown()
      }
    }).start()

    withJdbcStatement() { statement =>
      appIsRunning = true
      statement.execute("select 1")
      // hold resource so that the queue has no resource for other app
      lock.await()
    }
  }
}
