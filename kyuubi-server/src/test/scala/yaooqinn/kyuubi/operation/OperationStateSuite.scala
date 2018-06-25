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

import org.apache.hive.service.cli.thrift.TOperationState
import org.apache.spark.SparkFunSuite

import yaooqinn.kyuubi.KyuubiSQLException

class OperationStateSuite extends SparkFunSuite {

  test("test Operation State") {
    val state = new OperationState {
      override def toTOperationState(): TOperationState = TOperationState.UKNOWN_STATE
    }
    assert(!state.isTerminal())
    intercept[KyuubiSQLException](state.validateTransition(INITIALIZED))
  }

  test("OperationState INITIALIZED") {
    val tOpState = TOperationState.INITIALIZED_STATE
    assert(INITIALIZED.toTOperationState() === tOpState)
    assert(!INITIALIZED.isTerminal())
    INITIALIZED.validateTransition(PENDING)
    INITIALIZED.validateTransition(RUNNING)
    INITIALIZED.validateTransition(CANCELED)
    INITIALIZED.validateTransition(CLOSED)
    intercept[KyuubiSQLException](INITIALIZED.validateTransition(INITIALIZED))
    intercept[KyuubiSQLException](INITIALIZED.validateTransition(FINISHED))
    intercept[KyuubiSQLException](INITIALIZED.validateTransition(ERROR))
    intercept[KyuubiSQLException](INITIALIZED.validateTransition(UNKNOWN))
  }

  test("OperationState RUNNING") {
    val tOpState = TOperationState.RUNNING_STATE
    assert(RUNNING.toTOperationState() === tOpState)
    assert(!RUNNING.isTerminal())
    RUNNING.validateTransition(FINISHED)
    RUNNING.validateTransition(ERROR)
    RUNNING.validateTransition(CANCELED)
    RUNNING.validateTransition(CLOSED)
    intercept[KyuubiSQLException](RUNNING.validateTransition(INITIALIZED))
    intercept[KyuubiSQLException](RUNNING.validateTransition(PENDING))
    intercept[KyuubiSQLException](RUNNING.validateTransition(RUNNING))
    intercept[KyuubiSQLException](RUNNING.validateTransition(UNKNOWN))
  }

  test("OperationState FINISHED") {
    val tOpState = TOperationState.FINISHED_STATE
    assert(FINISHED.toTOperationState() === tOpState)
    assert(FINISHED.isTerminal())
    FINISHED.validateTransition(CLOSED)
    intercept[KyuubiSQLException](FINISHED.validateTransition(INITIALIZED))
    intercept[KyuubiSQLException](FINISHED.validateTransition(PENDING))
    intercept[KyuubiSQLException](FINISHED.validateTransition(RUNNING))
    intercept[KyuubiSQLException](FINISHED.validateTransition(FINISHED))
    intercept[KyuubiSQLException](FINISHED.validateTransition(ERROR))
    intercept[KyuubiSQLException](FINISHED.validateTransition(CANCELED))
    intercept[KyuubiSQLException](FINISHED.validateTransition(UNKNOWN))
  }

  test("OperationState CANCELED") {
    val tOpState = TOperationState.CANCELED_STATE
    assert(CANCELED.toTOperationState() === tOpState)
    assert(CANCELED.isTerminal())
    CANCELED.validateTransition(CLOSED)
    intercept[KyuubiSQLException](CANCELED.validateTransition(INITIALIZED))
    intercept[KyuubiSQLException](CANCELED.validateTransition(PENDING))
    intercept[KyuubiSQLException](CANCELED.validateTransition(RUNNING))
    intercept[KyuubiSQLException](CANCELED.validateTransition(FINISHED))
    intercept[KyuubiSQLException](CANCELED.validateTransition(ERROR))
    intercept[KyuubiSQLException](CANCELED.validateTransition(CANCELED))
    intercept[KyuubiSQLException](CANCELED.validateTransition(UNKNOWN))
  }

  test("OperationState CLOSED") {
    val tOpState = TOperationState.CLOSED_STATE
    assert(CLOSED.toTOperationState() === tOpState)
    assert(CLOSED.isTerminal())
    intercept[KyuubiSQLException](CLOSED.validateTransition(INITIALIZED))
    intercept[KyuubiSQLException](CLOSED.validateTransition(PENDING))
    intercept[KyuubiSQLException](CLOSED.validateTransition(RUNNING))
    intercept[KyuubiSQLException](CLOSED.validateTransition(FINISHED))
    intercept[KyuubiSQLException](CLOSED.validateTransition(ERROR))
    intercept[KyuubiSQLException](CLOSED.validateTransition(CANCELED))
    intercept[KyuubiSQLException](CLOSED.validateTransition(CLOSED))
    intercept[KyuubiSQLException](CLOSED.validateTransition(UNKNOWN))
  }

  test("OperationState ERROR") {
    val tOpState = TOperationState.ERROR_STATE
    assert(ERROR.toTOperationState() === tOpState)
    assert(ERROR.isTerminal())
    ERROR.validateTransition(CLOSED)
    intercept[KyuubiSQLException](ERROR.validateTransition(INITIALIZED))
    intercept[KyuubiSQLException](ERROR.validateTransition(PENDING))
    intercept[KyuubiSQLException](ERROR.validateTransition(RUNNING))
    intercept[KyuubiSQLException](ERROR.validateTransition(FINISHED))
    intercept[KyuubiSQLException](ERROR.validateTransition(ERROR))
    intercept[KyuubiSQLException](ERROR.validateTransition(CANCELED))
    intercept[KyuubiSQLException](ERROR.validateTransition(UNKNOWN))
  }

  test("OperationState UNKNOWN") {
    val tOpState = TOperationState.UKNOWN_STATE
    assert(UNKNOWN.toTOperationState() === tOpState)
    assert(!UNKNOWN.isTerminal())
    intercept[KyuubiSQLException](UNKNOWN.validateTransition(INITIALIZED))
    intercept[KyuubiSQLException](UNKNOWN.validateTransition(PENDING))
    intercept[KyuubiSQLException](UNKNOWN.validateTransition(RUNNING))
    intercept[KyuubiSQLException](UNKNOWN.validateTransition(FINISHED))
    intercept[KyuubiSQLException](UNKNOWN.validateTransition(ERROR))
    intercept[KyuubiSQLException](UNKNOWN.validateTransition(CANCELED))
    intercept[KyuubiSQLException](UNKNOWN.validateTransition(CLOSED))
    intercept[KyuubiSQLException](UNKNOWN.validateTransition(UNKNOWN))
  }

  test("OperationState PENDING") {
    val tOpState = TOperationState.PENDING_STATE
    assert(PENDING.toTOperationState() === tOpState)
    assert(!PENDING.isTerminal())
    PENDING.validateTransition(RUNNING)
    PENDING.validateTransition(FINISHED)
    PENDING.validateTransition(CANCELED)
    PENDING.validateTransition(ERROR)
    PENDING.validateTransition(CLOSED)
    intercept[KyuubiSQLException](PENDING.validateTransition(INITIALIZED))
    intercept[KyuubiSQLException](PENDING.validateTransition(PENDING))
    intercept[KyuubiSQLException](PENDING.validateTransition(UNKNOWN))
  }
}
