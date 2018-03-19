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

import org.apache.hive.service.cli.HiveSQLException
import org.apache.hive.service.cli.thrift.TOperationState
import org.apache.spark.SparkFunSuite

class OperationStateSuite extends SparkFunSuite {

  test("OperationState INITIALIZED") {
    val tOpState = TOperationState.INITIALIZED_STATE
    assert(INITIALIZED.toTOperationState() === tOpState)
    assert(!INITIALIZED.isTerminal())
    intercept[HiveSQLException](INITIALIZED.validateTransition(INITIALIZED))
    intercept[HiveSQLException](INITIALIZED.validateTransition(FINISHED))
    intercept[HiveSQLException](INITIALIZED.validateTransition(ERROR))
    intercept[HiveSQLException](INITIALIZED.validateTransition(UNKNOWN))
  }

  test("OperationState RUNNING") {
    val tOpState = TOperationState.RUNNING_STATE
    assert(RUNNING.toTOperationState() === tOpState)
    assert(!RUNNING.isTerminal())
    intercept[HiveSQLException](RUNNING.validateTransition(INITIALIZED))
    intercept[HiveSQLException](RUNNING.validateTransition(PENDING))
    intercept[HiveSQLException](RUNNING.validateTransition(RUNNING))
    intercept[HiveSQLException](RUNNING.validateTransition(UNKNOWN))
  }

  test("OperationState FINISHED") {
    val tOpState = TOperationState.FINISHED_STATE
    assert(FINISHED.toTOperationState() === tOpState)
    assert(FINISHED.isTerminal())
    intercept[HiveSQLException](FINISHED.validateTransition(INITIALIZED))
    intercept[HiveSQLException](FINISHED.validateTransition(PENDING))
    intercept[HiveSQLException](FINISHED.validateTransition(RUNNING))
    intercept[HiveSQLException](FINISHED.validateTransition(FINISHED))
    intercept[HiveSQLException](FINISHED.validateTransition(ERROR))
    intercept[HiveSQLException](FINISHED.validateTransition(CANCELED))
    intercept[HiveSQLException](FINISHED.validateTransition(UNKNOWN))
  }

  test("OperationState CANCELED") {
    val tOpState = TOperationState.CANCELED_STATE
    assert(CANCELED.toTOperationState() === tOpState)
    assert(CANCELED.isTerminal())
    intercept[HiveSQLException](CANCELED.validateTransition(INITIALIZED))
    intercept[HiveSQLException](CANCELED.validateTransition(PENDING))
    intercept[HiveSQLException](CANCELED.validateTransition(RUNNING))
    intercept[HiveSQLException](CANCELED.validateTransition(FINISHED))
    intercept[HiveSQLException](CANCELED.validateTransition(ERROR))
    intercept[HiveSQLException](CANCELED.validateTransition(CANCELED))
    intercept[HiveSQLException](CANCELED.validateTransition(UNKNOWN))
  }

  test("OperationState CLOSED") {
    val tOpState = TOperationState.CLOSED_STATE
    assert(CLOSED.toTOperationState() === tOpState)
    assert(CLOSED.isTerminal())
    intercept[HiveSQLException](CLOSED.validateTransition(INITIALIZED))
    intercept[HiveSQLException](CLOSED.validateTransition(PENDING))
    intercept[HiveSQLException](CLOSED.validateTransition(RUNNING))
    intercept[HiveSQLException](CLOSED.validateTransition(FINISHED))
    intercept[HiveSQLException](CLOSED.validateTransition(ERROR))
    intercept[HiveSQLException](CLOSED.validateTransition(CANCELED))
    intercept[HiveSQLException](CLOSED.validateTransition(CLOSED))
    intercept[HiveSQLException](CLOSED.validateTransition(UNKNOWN))
  }

  test("OperationState ERROR") {
    val tOpState = TOperationState.ERROR_STATE
    assert(ERROR.toTOperationState() === tOpState)
    assert(ERROR.isTerminal())
    intercept[HiveSQLException](ERROR.validateTransition(INITIALIZED))
    intercept[HiveSQLException](ERROR.validateTransition(PENDING))
    intercept[HiveSQLException](ERROR.validateTransition(RUNNING))
    intercept[HiveSQLException](ERROR.validateTransition(FINISHED))
    intercept[HiveSQLException](ERROR.validateTransition(ERROR))
    intercept[HiveSQLException](ERROR.validateTransition(CANCELED))
    intercept[HiveSQLException](ERROR.validateTransition(UNKNOWN))
  }

  test("OperationState UNKNOWN") {
    val tOpState = TOperationState.UKNOWN_STATE
    assert(UNKNOWN.toTOperationState() === tOpState)
    assert(!UNKNOWN.isTerminal())
    intercept[HiveSQLException](UNKNOWN.validateTransition(INITIALIZED))
    intercept[HiveSQLException](UNKNOWN.validateTransition(PENDING))
    intercept[HiveSQLException](UNKNOWN.validateTransition(RUNNING))
    intercept[HiveSQLException](UNKNOWN.validateTransition(FINISHED))
    intercept[HiveSQLException](UNKNOWN.validateTransition(ERROR))
    intercept[HiveSQLException](UNKNOWN.validateTransition(CANCELED))
    intercept[HiveSQLException](UNKNOWN.validateTransition(CLOSED))
    intercept[HiveSQLException](UNKNOWN.validateTransition(UNKNOWN))
  }

  test("OperationState PENDING") {
    val tOpState = TOperationState.PENDING_STATE
    assert(PENDING.toTOperationState() === tOpState)
    assert(!PENDING.isTerminal())
    intercept[HiveSQLException](PENDING.validateTransition(INITIALIZED))
    intercept[HiveSQLException](PENDING.validateTransition(PENDING))
    intercept[HiveSQLException](PENDING.validateTransition(UNKNOWN))
  }
}
