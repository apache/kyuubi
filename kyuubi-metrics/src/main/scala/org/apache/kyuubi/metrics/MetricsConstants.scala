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

  final private val STATEMENT = KYUUBI + "statement."
  final val STATEMENT_OPEN: String = STATEMENT + "opened"
  final val STATEMENT_FAIL: String = STATEMENT + "failed"
  final val STATEMENT_TOTAL: String = STATEMENT + "total"

  final private val ENGINE = KYUUBI + "engine."
  final val ENGINE_FAIL: String = ENGINE + "failed"
  final val ENGINE_TIMEOUT: String = ENGINE + "timeout"
  final val ENGINE_TOTAL: String = ENGINE + "total"
}
