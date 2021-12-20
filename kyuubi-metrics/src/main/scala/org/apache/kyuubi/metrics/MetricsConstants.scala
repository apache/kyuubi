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

package org.apache.kyuubi.metrics

object MetricsConstants {

  final private val KYUUBI = "kyuubi."

  final val EXEC_POOL_ALIVE: String = KYUUBI + "exec.pool.threads.alive"
  final val EXEC_POOL_ACTIVE: String = KYUUBI + "exec.pool.threads.active"

  final private val CONN = KYUUBI + "connection."

  final val CONN_OPEN: String = CONN + "opened"
  final val CONN_FAIL: String = CONN + "failed"
  final val CONN_TOTAL: String = CONN + "total"

  final private val ENGINE = KYUUBI + "engine."
  final val ENGINE_FAIL: String = ENGINE + "failed"
  final val ENGINE_TIMEOUT: String = ENGINE + "timeout"
  final val ENGINE_TOTAL: String = ENGINE + "total"

  final private val OPERATION = KYUUBI + "operation."
  final val OPERATION_OPEN: String = OPERATION + "opened"
  final val OPERATION_FAIL: String = OPERATION + "failed"
  final val OPERATION_TOTAL: String = OPERATION + "total"

  final private val BACKEND_SERVICE = KYUUBI + "backend_service."
  final val OPEN_SESSION_MS = BACKEND_SERVICE + "open_session_ms"
  final val CLOSE_SESSION_MS = BACKEND_SERVICE + "close_session_ms"
  final val GET_INFO_MS = BACKEND_SERVICE + "get_info_ms"
  final val EXECUTE_STATEMENT_MS = BACKEND_SERVICE + "execute_statement_ms"
  final val GET_TYPE_INFO_MS = BACKEND_SERVICE + "get_type_info_ms"
  final val GET_CATALOGS_MS = BACKEND_SERVICE + "get_catalogs_ms"
  final val GET_SCHEMAS_MS = BACKEND_SERVICE + "get_schemas_ms"
  final val GET_TABLES_MS = BACKEND_SERVICE + "get_tables_ms"
  final val GET_TABLE_TYPES_MS = BACKEND_SERVICE + "get_table_types_ms"
  final val GET_COLUMNS_MS = BACKEND_SERVICE + "get_columns_ms"
  final val GET_FUNCTIONS_MS = BACKEND_SERVICE + "get_functions_ms"
  final val GET_OPERATION_STATUS_MS = BACKEND_SERVICE + "get_operation_status_ms"
  final val CANCEL_OPERATION_MS = BACKEND_SERVICE + "cancel_operation_ms"
  final val CLOSE_OPERATION_MS = BACKEND_SERVICE + "close_operation_ms"
  final val GET_RESULT_SET_METADATA_MS = BACKEND_SERVICE + "get_result_set_metadata_ms"
  final val FETCH_RESULTS_MS = BACKEND_SERVICE + "fetch_results_ms"

}
