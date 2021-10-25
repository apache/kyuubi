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

import java.sql.SQLException

import org.apache.hive.service.rpc.thrift.{TExecuteStatementReq, TGetOperationStatusReq, TOperationState, TStatusCode}
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.Utils
import org.apache.kyuubi.WithKyuubiServer
import org.apache.kyuubi.config.KyuubiConf

/**
 * UT with Connection level engine shared cost much time, only run basic jdbc tests.
 */
class KyuubiOperationPerConnectionSuite extends WithKyuubiServer with JDBCTestHelper {

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

      // TODO KYUUBI #745
      eventually(timeout(60.seconds), interval(500.milliseconds)) {
        val getOpStatusReq = new TGetOperationStatusReq(executeStmtResp.getOperationHandle)
        val getOpStatusResp = client.GetOperationStatus(getOpStatusReq)
        assert(getOpStatusResp.getStatus.getStatusCode === TStatusCode.SUCCESS_STATUS)
        assert(getOpStatusResp.getOperationState === TOperationState.ERROR_STATE)
      }
    }
  }

  test("submit spark app timeout with last log output") {
    withSessionConf()(Map(KyuubiConf.ENGINE_INIT_TIMEOUT.key -> "2000"))(Map.empty) {
      val exception = intercept[SQLException] {
        withJdbcStatement() { statement => // no-op
        }
      }
      val verboseMessage = Utils.stringifyException(exception)
      assert(verboseMessage.contains("Failed to detect the root cause"))
    }
  }
}
