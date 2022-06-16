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

package org.apache.kyuubi.ctl.util

import java.util.Locale

object BatchUtil {
  private val PENDING_STATE = "PENDING"
  private val RUNNING_STATE = "RUNNING"
  private val FINISHED_STATE = "FINISHED"
  private val ERROR_STATE = "ERROR"
  private val CANCELED_STATE = "CANCELED"
  private val terminalBatchStates = Seq(FINISHED_STATE, ERROR_STATE, CANCELED_STATE)

  def isPendingState(state: String): Boolean = {
    PENDING_STATE.equalsIgnoreCase(state)
  }

  def isRunningState(state: String): Boolean = {
    RUNNING_STATE.equalsIgnoreCase(state)
  }

  def isFinishedState(state: String): Boolean = {
    FINISHED_STATE.equalsIgnoreCase(state)
  }

  def isTerminalState(state: String): Boolean = {
    state != null && terminalBatchStates.contains(state.toUpperCase(Locale.ROOT))
  }
}
