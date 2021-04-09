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

import org.apache.hive.service.rpc.thrift.TOperationType

import org.apache.kyuubi.KyuubiFunSuite

class OperationTypeSuite extends KyuubiFunSuite {

  import OperationType._

  test("getOperationType") {
    val get = OperationType.getOperationType _
    assert(get(TOperationType.EXECUTE_STATEMENT) === EXECUTE_STATEMENT)
    assert(get(TOperationType.GET_TYPE_INFO) === GET_TYPE_INFO)
    assert(get(TOperationType.GET_CATALOGS) === GET_CATALOGS)
    assert(get(TOperationType.GET_SCHEMAS) === GET_SCHEMAS)
    assert(get(TOperationType.GET_TABLES) === GET_TABLES)
    assert(get(TOperationType.GET_TABLE_TYPES) === GET_TABLE_TYPES)
    assert(get(TOperationType.GET_COLUMNS) === GET_COLUMNS)
    assert(get(TOperationType.GET_FUNCTIONS) === GET_FUNCTIONS)
    val e = intercept[UnsupportedOperationException](get(TOperationType.UNKNOWN))
    assert(e.getMessage === "Unsupported Operation type: UNKNOWN")
  }

  test("toTOperationType") {
    val to = OperationType.toTOperationType _
    assert(to(EXECUTE_STATEMENT) === TOperationType.EXECUTE_STATEMENT)
    assert(to(GET_TYPE_INFO) === TOperationType.GET_TYPE_INFO)
    assert(to(GET_CATALOGS) === TOperationType.GET_CATALOGS)
    assert(to(GET_SCHEMAS) === TOperationType.GET_SCHEMAS)
    assert(to(GET_TABLES) === TOperationType.GET_TABLES)
    assert(to(GET_TABLE_TYPES) === TOperationType.GET_TABLE_TYPES)
    assert(to(GET_COLUMNS) === TOperationType.GET_COLUMNS)
    assert(to(GET_FUNCTIONS) === TOperationType.GET_FUNCTIONS)
    val e = intercept[UnsupportedOperationException](to(UNKNOWN_OPERATION))
    assert(e.getMessage === "Unsupported Operation type: UNKNOWN_OPERATION")
  }
}
