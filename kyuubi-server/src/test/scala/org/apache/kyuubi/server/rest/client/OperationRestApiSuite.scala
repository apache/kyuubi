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

package org.apache.kyuubi.server.rest.client

import scala.collection.JavaConverters._

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.RestClientTestHelper
import org.apache.kyuubi.client.{KyuubiRestClient, OperationRestApi}
import org.apache.kyuubi.client.api.v1.dto.OpActionRequest
import org.apache.kyuubi.client.exception.KyuubiRestException
import org.apache.kyuubi.operation.OperationState
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2

class OperationRestApiSuite extends RestClientTestHelper {

  test("get an operation event") {
    val statementHandleStr = getOpHandleStr()

    withOperationRestApi { operationRestApi =>
      val kyuubiEvent = operationRestApi.getOperationEvent(statementHandleStr)
      assert("show tables".equals(kyuubiEvent.getStatement))
      assert(kyuubiEvent.isShouldRunAsync == true)
    }
  }

  test("apply operation action") {
    val statementHandleStr = getOpHandleStr(
      "SELECT java_method('java.lang.Thread', 'sleep', 10000l)")

    withOperationRestApi { operationRestApi =>
      // successful request
      operationRestApi.applyOperationAction(new OpActionRequest("cancel"), statementHandleStr)
      eventually(Timeout(5.seconds)) {
        val kyuubiEvent = operationRestApi.getOperationEvent(statementHandleStr)
        assert(kyuubiEvent.getState === OperationState.CANCELED.name)
      }

      operationRestApi.applyOperationAction(new OpActionRequest("close"), statementHandleStr)
      // failed request
      assertThrows[KyuubiRestException] {
        operationRestApi.applyOperationAction(new OpActionRequest("close"), statementHandleStr)
      }

      // invalid operation
      assertThrows[KyuubiRestException] {
        operationRestApi.applyOperationAction(new OpActionRequest("fake"), statementHandleStr)
      }
    }
  }

  test("get result set metadata/get operation log/get result row set") {
    val statementHandleStr = getOpHandleStr("select \"test_value\", 1, 0.32d, true")

    withOperationRestApi { operationRestApi =>
      // wait for complete
      eventually(Timeout(5.seconds)) {
        val kyuubiEvent = operationRestApi.getOperationEvent(statementHandleStr)
        assert(kyuubiEvent.getState === OperationState.FINISHED.name)
      }

      val resultSetMetadata = operationRestApi.getResultSetMetadata(statementHandleStr)
      assert(resultSetMetadata.getColumns.size == 4)
      assert(resultSetMetadata.getColumns.get(0).getColumnName.equals("test_value"))

      val logRowSet = operationRestApi.getOperationLog(statementHandleStr, 10)
      assert(logRowSet.getLogRowSet.asScala.exists(
        _.contains("select \"test_value\", 1, 0.32d, true")))
      assert(logRowSet.getRowCount === 10)

      val resultRowSet = operationRestApi.getNextRowSet(statementHandleStr)
      assert("test_value".equals(resultRowSet.getRows.asScala.head.getFields.asScala.head.getValue))
      assert(resultRowSet.getRowCount == 1)
    }
  }

  def withOperationRestApi[T](f: OperationRestApi => T): T = {
    val basicKyuubiRestClient: KyuubiRestClient =
      KyuubiRestClient.builder(baseUri.toString)
        .authHeaderMethod(KyuubiRestClient.AuthHeaderMethod.BASIC)
        .username(ldapUser)
        .password(ldapUserPasswd)
        .socketTimeout(30000)
        .build()
    val operationRestApi = new OperationRestApi(basicKyuubiRestClient)
    f(operationRestApi)
  }

  def getOpHandleStr(statement: String = "show tables"): String = {
    val sessionHandle = fe.be.openSession(
      HIVE_CLI_SERVICE_PROTOCOL_V2,
      "admin",
      "123456",
      "localhost",
      Map("testConfig" -> "testValue"))

    val op =
      if (statement.nonEmpty) {
        fe.be.executeStatement(sessionHandle, statement, Map.empty, runAsync = true, 3000)
      } else {
        fe.be.getCatalogs(sessionHandle)
      }

    op.identifier.toString
  }
}
