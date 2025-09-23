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

package org.apache.kyuubi.session

import scala.language.implicitConversions

import org.apache.kyuubi.KyuubiSQLException

object SessionState extends Enumeration {

  type SessionState = Value

  val INITIALIZED, PENDING, RUNNING, TIMEOUT, CLOSED, ERROR =
    Value

  val terminalStates: Seq[SessionState] = Seq(TIMEOUT, CLOSED, ERROR)

  def validateTransition(oldState: SessionState, newState: SessionState): Unit = {
    oldState match {
      case INITIALIZED if Set(PENDING, RUNNING, TIMEOUT, CLOSED).contains(newState) =>
      case PENDING
          if Set(RUNNING, TIMEOUT, CLOSED, ERROR).contains(
            newState) =>
      case RUNNING
          if Set(TIMEOUT, CLOSED, ERROR).contains(newState) =>
      case TIMEOUT | ERROR if CLOSED.equals(newState) =>
      case _ => throw KyuubiSQLException(
          s"Illegal Session state transition from $oldState to $newState")
    }
  }

  def isTerminal(state: SessionState): Boolean = {
    terminalStates.contains(state)
  }
}
