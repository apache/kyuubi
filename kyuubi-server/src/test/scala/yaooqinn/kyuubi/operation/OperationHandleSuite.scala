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

import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.SparkFunSuite

import yaooqinn.kyuubi.cli.HandleIdentifier

class OperationHandleSuite extends SparkFunSuite {

import TProtocolVersion._

  test("operation handle basic tests") {
    val handle1 = new OperationHandle(EXECUTE_STATEMENT, HIVE_CLI_SERVICE_PROTOCOL_V8)
    assert(!handle1.isHasResultSet)
    handle1.setHasResultSet(true)
    assert(handle1.isHasResultSet)
    assert(handle1.toTOperationHandle.isHasResultSet)
    assert(handle1.getOperationType === EXECUTE_STATEMENT)
    assert(handle1.getProtocolVersion === HIVE_CLI_SERVICE_PROTOCOL_V8)

    val handle2 = new OperationHandle(
      handle1.toTOperationHandle, HIVE_CLI_SERVICE_PROTOCOL_V8)
    assert(handle2.isHasResultSet)
    assert(handle2.toTOperationHandle.isHasResultSet)
    assert(handle2.getOperationType === EXECUTE_STATEMENT)
    assert(handle2.getProtocolVersion === HIVE_CLI_SERVICE_PROTOCOL_V8)
    assert(handle1 equals handle2)

    val handle3 = new OperationHandle(handle2.toTOperationHandle)
    assert(handle3.isHasResultSet)
    assert(handle3.toTOperationHandle.isHasResultSet)
    assert(handle3.getOperationType === EXECUTE_STATEMENT)
    assert(handle3.getProtocolVersion === HIVE_CLI_SERVICE_PROTOCOL_V1)
    assert(handle3 === handle2)

    assert(handle1.toTOperationHandle.getOperationType === EXECUTE_STATEMENT)
    assert(handle1.toString === handle3.toString)
    assert(handle1.toTOperationHandle.getOperationId ===
      handle1.getHandleIdentifier.toTHandleIdentifier)
  }

  test("operation handle to string") {
    val opType = EXECUTE_STATEMENT
    val protocol = HIVE_CLI_SERVICE_PROTOCOL_V8
    val handle = new OperationHandle(opType, protocol)
    val hStr = handle.toString
    assert(hStr.startsWith(classOf[OperationHandle].getSimpleName))
    assert(hStr.contains(opType.toString))
    assert(hStr.contains(handle.getOperationType.toString))
    assert(hStr.contains(handle.getHandleIdentifier.toString))
  }

  test("operation handle equals") {
    val opType = EXECUTE_STATEMENT
    val protocol = HIVE_CLI_SERVICE_PROTOCOL_V8
    val handle = new OperationHandle(opType, protocol)
    assert(!handle.equals(new Object()))
    assert(handle.equals(handle))
    val handle2 = new OperationHandle(opType, protocol)
    assert(!handle.equals(handle2))
    val handle3 = new OperationHandle(handle.toTOperationHandle)
    assert(handle.equals(handle3))
    val handle4 = new OperationHandle(handle.toTOperationHandle,
      HIVE_CLI_SERVICE_PROTOCOL_V7)
    assert(handle.equals(handle4))
    val ctor = classOf[OperationHandle].getDeclaredConstructor(
      classOf[OperationType], classOf[TProtocolVersion], classOf[HandleIdentifier])
    ctor.setAccessible(true)
    val handle5 = ctor.newInstance(GET_TYPE_INFO, protocol, handle.getHandleIdentifier)
    assert(handle5.isInstanceOf[OperationHandle])
    assert(handle5.getOperationType !== handle.getOperationType)
    assert(handle5.getHandleIdentifier === handle.getHandleIdentifier)
    assert(handle5.getProtocolVersion === handle.getProtocolVersion)
    assert(!handle.equals(handle5))
  }

  test("operation handle hash code") {
    val opType = EXECUTE_STATEMENT
    val protocol = HIVE_CLI_SERVICE_PROTOCOL_V8
    val handle = new OperationHandle(opType, protocol)
    val prime = 31
    assert(handle.hashCode ===
      prime * (prime * 1 + handle.getHandleIdentifier.hashCode) + opType.hashCode())
  }

  test("operation handle get protocol version") {
    val opType = EXECUTE_STATEMENT
    val protocol = HIVE_CLI_SERVICE_PROTOCOL_V8
    val handle = new OperationHandle(opType, protocol)
    assert(handle.getProtocolVersion === protocol)
    assert(new OperationHandle(handle.toTOperationHandle).getProtocolVersion === protocol)
    assert(new OperationHandle(handle.toTOperationHandle,
      HIVE_CLI_SERVICE_PROTOCOL_V1).getProtocolVersion === HIVE_CLI_SERVICE_PROTOCOL_V1)
  }

  test("operation handle has result set") {
    val opType = EXECUTE_STATEMENT
    val protocol = HIVE_CLI_SERVICE_PROTOCOL_V8
    val handle = new OperationHandle(opType, protocol)
    assert(!handle.isHasResultSet)
    handle.setHasResultSet(false)
    assert(!handle.isHasResultSet)
    handle.setHasResultSet(true)
    assert(handle.isHasResultSet)
  }

  test("operation handle to tOperationType") {
    val opType = EXECUTE_STATEMENT
    val protocol = HIVE_CLI_SERVICE_PROTOCOL_V8
    val handle = new OperationHandle(opType, protocol)
    assert(!handle.toTOperationHandle.isHasResultSet)
    handle.setHasResultSet(true)
    assert(handle.toTOperationHandle.isHasResultSet)
  }
}
