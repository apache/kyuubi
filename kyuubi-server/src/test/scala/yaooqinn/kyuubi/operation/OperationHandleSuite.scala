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

package yaooqinn.kyuubi.operation

import org.apache.hive.service.cli.thrift.{TOperationType, TProtocolVersion}
import org.apache.spark.SparkFunSuite

class OperationHandleSuite extends SparkFunSuite {

  test("operation handle basic tests") {
    val handle1 = new OperationHandle(
      EXECUTE_STATEMENT, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)
    assert(!handle1.isHasResultSet)
    handle1.setHasResultSet(true)
    assert(handle1.isHasResultSet)
    assert(handle1.toTOperationHandle.isHasResultSet)
    assert(handle1.opType  === EXECUTE_STATEMENT)
    assert(handle1.getOperationType === EXECUTE_STATEMENT)
    assert(handle1.getProtocolVersion === TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)

    val handle2 = new OperationHandle(
      handle1.toTOperationHandle, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)
    assert(handle2.isHasResultSet)
    assert(handle2.toTOperationHandle.isHasResultSet)
    assert(handle2.opType  === EXECUTE_STATEMENT)
    assert(handle2.getOperationType === EXECUTE_STATEMENT)
    assert(handle2.getProtocolVersion === TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)
    assert(handle1 === handle2)

    val handle3 = new OperationHandle(handle2.toTOperationHandle)
    assert(handle3.isHasResultSet)
    assert(handle3.toTOperationHandle.isHasResultSet)
    assert(handle3.opType  === EXECUTE_STATEMENT)
    assert(handle3.getOperationType === EXECUTE_STATEMENT)
    assert(handle3.getProtocolVersion === TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
    assert(handle3 === handle2)

    assert(handle1.toTOperationHandle.getOperationType === TOperationType.EXECUTE_STATEMENT)
    assert(handle1.toString === handle3.toString)
    assert(handle1.toTOperationHandle.getOperationId ===
      handle1.getHandleIdentifier.toTHandleIdentifier)
  }
}
