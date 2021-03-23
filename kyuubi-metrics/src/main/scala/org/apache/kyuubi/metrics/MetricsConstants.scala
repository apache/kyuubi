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

  val EXEC_POOL_ALIVE: String = "exec.pool.threads.alive"
  val EXEC_POOL_ACTIVE: String = "exec.pool.threads.active"

  val CONN_OPEN: String = "connection.open"
  val CONN_FAIL: String = "connection.fail"
  val CONN_TOTAL: String = "connection.total"

  val SESSION_OPEN_KEY: String = "session.open"
  val SESSION_TOTAL_KEY: String = "session.total"

  val STATEMENT_OPEN: String = "statement.open"
  val STATEMENT_FAIL: String = "statement.fail"
  val STATEMENT_TOTAL: String = "statement.total"

  val ENGINE_FAIL: String = "engine.fail"
  val ENGINE_TIMEOUT: String = "engine.timeout"
  val ENGINE_TOTAL: String = "engine.total"
}
