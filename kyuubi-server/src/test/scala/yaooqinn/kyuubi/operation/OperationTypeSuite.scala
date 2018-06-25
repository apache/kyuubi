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

import org.apache.hive.service.cli.thrift.TOperationType
import org.apache.spark.SparkFunSuite

class OperationTypeSuite extends SparkFunSuite {

  test("EXECUTE_STATEMENT") {
    val tOpType = TOperationType.EXECUTE_STATEMENT
    assert(OperationType.getOperationType(tOpType) === EXECUTE_STATEMENT)
    assert(OperationType.getOperationType(tOpType).toTOperationType === tOpType)
    assert(EXECUTE_STATEMENT.toTOperationType === tOpType)
  }

  test("GET_CATALOGS") {
    val tOpType = TOperationType.GET_CATALOGS
    assert(OperationType.getOperationType(tOpType) === GET_CATALOGS)
    assert(OperationType.getOperationType(tOpType).toTOperationType === tOpType)
    assert(GET_CATALOGS.toTOperationType === tOpType)
  }

  test("GET_TYPE_INFO") {
    val tOpType = TOperationType.GET_TYPE_INFO
    assert(OperationType.getOperationType(tOpType) === GET_TYPE_INFO)
    assert(OperationType.getOperationType(tOpType).toTOperationType === tOpType)
    assert(GET_TYPE_INFO.toTOperationType === tOpType)
  }

  test("GET_SCHEMAS") {
    val tOpType = TOperationType.GET_SCHEMAS
    assert(OperationType.getOperationType(tOpType) === GET_SCHEMAS)
    assert(OperationType.getOperationType(tOpType).toTOperationType === tOpType)
    assert(GET_SCHEMAS.toTOperationType === tOpType)
  }

  test("GET_TABLES") {
    val tOpType = TOperationType.GET_TABLES
    assert(OperationType.getOperationType(tOpType) === GET_TABLES)
    assert(OperationType.getOperationType(tOpType).toTOperationType === tOpType)
    assert(GET_TABLES.toTOperationType === tOpType)
  }

  test("GET_TABLE_TYPES") {
    val tOpType = TOperationType.GET_TABLE_TYPES
    assert(OperationType.getOperationType(tOpType) === GET_TABLE_TYPES)
    assert(OperationType.getOperationType(tOpType).toTOperationType === tOpType)
    assert(GET_TABLE_TYPES.toTOperationType === tOpType)
  }

  test("GET_COLUMNS") {
    val tOpType = TOperationType.GET_COLUMNS
    assert(OperationType.getOperationType(tOpType) === GET_COLUMNS)
    assert(OperationType.getOperationType(tOpType).toTOperationType === tOpType)
    assert(GET_COLUMNS.toTOperationType === tOpType)
  }

  test("GET_FUNCTIONS") {
    val tOpType = TOperationType.GET_FUNCTIONS
    assert(OperationType.getOperationType(tOpType) === GET_FUNCTIONS)
    assert(OperationType.getOperationType(tOpType).toTOperationType === tOpType)
    assert(GET_FUNCTIONS.toTOperationType === tOpType)
  }

  test("UNKNOWN_OPERATION") {
    val tOpType = TOperationType.UNKNOWN
    assert(OperationType.getOperationType(tOpType) === UNKNOWN_OPERATION)
    assert(OperationType.getOperationType(tOpType).toTOperationType === tOpType)
    assert(UNKNOWN_OPERATION.toTOperationType === tOpType)
    val unknownOperation = new OperationType {
      override def toTOperationType: TOperationType = TOperationType.findByValue(9)
    }
    assert(unknownOperation.toTOperationType === null)
    assert(OperationType.getOperationType(TOperationType.findByValue(9)) === UNKNOWN_OPERATION)
  }
}
