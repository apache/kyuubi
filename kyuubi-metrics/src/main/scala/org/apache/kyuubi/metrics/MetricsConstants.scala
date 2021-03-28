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

  final val KYUUBI = "kyuubi"

  final val SERVICE = KYUUBI + ".service"
  final val ENGINE = KYUUBI + ".engine"
  final val SESSION = KYUUBI + ".session"
  final val OPERATION = KYUUBI + ".operation"

  final val SERVICE_EXEC_POOL = SERVICE + ".exec.pool.threads"

  final val T_SHR_LV = "share_level"

  final val T_USER = "user"

  final val T_ERR = "err"

  final val T_STAT = "status"
  final val STAT_ALIVE = "alive"
  final val STAT_ACTIVE = "active"
  final val STAT_OPENED = "opened"
  final val STAT_FAILED = "failed"

  final val T_SCH = "schedule"
  final val SCH_SYNC = "sync"
  final val SCH_ASYNC = "async"

  final val T_OP = "op_type"
  final val OP_EXEC_STMT = "execute_statement"
  final val OP_G_CATALOG = "get_catalog"
  final val OP_G_COL = "get_column"
  final val OP_G_FUNC = "get_function"
  final val OP_G_SCHEMA = "get_schema"
  final val OP_G_TBL = "get_table"
  final val OP_G_TBL_TYP = "get_table_type"
  final val OP_G_TYP_INF = "get_type_info"

  final val T_EVT = "event"
  final val EVT_OPEN = "open"
  final val EVT_CLOSE = "close"
  final val EVT_FAIL = "fail"
  final val EVT_TIMEOUT = "timeout"
}
