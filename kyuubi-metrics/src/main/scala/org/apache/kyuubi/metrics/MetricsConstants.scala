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

  final val GC_METRIC: String = KYUUBI + "gc"
  final val MEMORY_USAGE: String = KYUUBI + "memory_usage"
  final val BUFFER_POOL: String = KYUUBI + "buffer_pool"
  final val THREAD_STATE: String = KYUUBI + "thread_state"
  final val CLASS_LOADING: String = KYUUBI + "class_loading"
  final val JVM: String = KYUUBI + "jvm"

  final val EXEC_POOL_ALIVE: String = KYUUBI + "exec.pool.threads.alive"
  final val EXEC_POOL_ACTIVE: String = KYUUBI + "exec.pool.threads.active"
  final val EXEC_POOL_WORK_QUEUE_SIZE: String = KYUUBI + "exec.pool.work_queue.size"

  final private val CONN = KYUUBI + "connection."
  final private val THRIFT_HTTP_CONN = KYUUBI + "thrift.http.connection."
  final private val THRIFT_BINARY_CONN = KYUUBI + "thrift.binary.connection."
  final private val REST_CONN = KYUUBI + "rest.connection."

  final val THRIFT_SSL_CERT_EXPIRATION = KYUUBI + "thrift.ssl.cert.expiration"

  final val CONN_OPEN: String = CONN + "opened"
  final val CONN_FAIL: String = CONN + "failed"
  final val CONN_TOTAL: String = CONN + "total"

  final val THRIFT_HTTP_CONN_OPEN: String = THRIFT_HTTP_CONN + "opened"
  final val THRIFT_HTTP_CONN_FAIL: String = THRIFT_HTTP_CONN + "failed"
  final val THRIFT_HTTP_CONN_TOTAL: String = THRIFT_HTTP_CONN + "total"

  final val THRIFT_BINARY_CONN_OPEN: String = THRIFT_BINARY_CONN + "opened"
  final val THRIFT_BINARY_CONN_FAIL: String = THRIFT_BINARY_CONN + "failed"
  final val THRIFT_BINARY_CONN_TOTAL: String = THRIFT_BINARY_CONN + "total"

  final val REST_CONN_OPEN: String = REST_CONN + "opened"
  final val REST_CONN_FAIL: String = REST_CONN + "failed"
  final val REST_CONN_TOTAL: String = REST_CONN + "total"

  final private val ENGINE = KYUUBI + "engine."
  final val ENGINE_FAIL: String = ENGINE + "failed"
  final val ENGINE_TIMEOUT: String = ENGINE + "timeout"
  final val ENGINE_TOTAL: String = ENGINE + "total"

  final private val ENGINE_STARTUP_PERMIT: String = ENGINE + "startup.permit."
  final val ENGINE_STARTUP_PERMIT_LIMIT: String = ENGINE_STARTUP_PERMIT + "limit"
  final val ENGINE_STARTUP_PERMIT_AVAILABLE: String = ENGINE_STARTUP_PERMIT + "available"
  final val ENGINE_STARTUP_PERMIT_WAITING: String = ENGINE_STARTUP_PERMIT + "waiting"

  final private val OPERATION = KYUUBI + "operation."
  final val OPERATION_OPEN: String = OPERATION + "opened"
  final val OPERATION_FAIL: String = OPERATION + "failed"
  final val OPERATION_TOTAL: String = OPERATION + "total"
  final val OPERATION_STATE: String = OPERATION + "state"
  final val OPERATION_EXEC_TIME: String = OPERATION + "exec_time"
  final val OPERATION_BATCH_PENDING_MAX_ELAPSE: String = OPERATION + "batch_pending_max_elapse"

  final private val BACKEND_SERVICE = KYUUBI + "backend_service."
  final val BS_FETCH_LOG_ROWS_RATE = BACKEND_SERVICE + "fetch_log_rows_rate"
  final val BS_FETCH_RESULT_ROWS_RATE = BACKEND_SERVICE + "fetch_result_rows_rate"
  final val BS_OPEN_SESSION = BACKEND_SERVICE + "open_session"
  final val BS_CLOSE_SESSION = BACKEND_SERVICE + "close_session"
  final val BS_GET_INFO = BACKEND_SERVICE + "get_info"
  final val BS_EXECUTE_STATEMENT = BACKEND_SERVICE + "execute_statement"
  final val BS_GET_TYPE_INFO = BACKEND_SERVICE + "get_type_info"
  final val BS_GET_CATALOGS = BACKEND_SERVICE + "get_catalogs"
  final val BS_GET_SCHEMAS = BACKEND_SERVICE + "get_schemas"
  final val BS_GET_TABLES = BACKEND_SERVICE + "get_tables"
  final val BS_GET_TABLE_TYPES = BACKEND_SERVICE + "get_table_types"
  final val BS_GET_COLUMNS = BACKEND_SERVICE + "get_columns"
  final val BS_GET_FUNCTIONS = BACKEND_SERVICE + "get_functions"
  final val BS_GET_PRIMARY_KEY = BACKEND_SERVICE + "get_primary_keys"
  final val BS_GET_CROSS_REFERENCE = BACKEND_SERVICE + "get_cross_reference"
  final val BS_GET_OPERATION_STATUS = BACKEND_SERVICE + "get_operation_status"
  final val BS_CANCEL_OPERATION = BACKEND_SERVICE + "cancel_operation"
  final val BS_CLOSE_OPERATION = BACKEND_SERVICE + "close_operation"
  final val BS_GET_RESULT_SET_METADATA = BACKEND_SERVICE + "get_result_set_metadata"
  final val BS_FETCH_RESULTS = BACKEND_SERVICE + "fetch_results"

  final private val METADATA_REQUEST = KYUUBI + "metadata.request."
  final val METADATA_REQUEST_OPENED = METADATA_REQUEST + "opened"
  final val METADATA_REQUEST_TOTAL = METADATA_REQUEST + "total"
  final val METADATA_REQUEST_FAIL = METADATA_REQUEST + "failed"
  final val METADATA_REQUEST_RETRYING = METADATA_REQUEST + "retrying"

  final private val JETTY = KYUUBI + "jetty."
  final val JETTY_API_V1 = JETTY + "api.v1"
}
