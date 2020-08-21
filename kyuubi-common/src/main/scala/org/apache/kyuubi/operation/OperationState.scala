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

import scala.language.implicitConversions

import org.apache.hive.service.rpc.thrift.TOperationState

import org.apache.kyuubi.KyuubiSQLException

object OperationState extends Enumeration {
  import TOperationState._

  type OperationState = Value

  val INITIALIZED, PENDING, RUNNING, FINISHED, TIMEOUT, CANCELED, CLOSED, ERROR, UNKNOWN = Value

  implicit def toTOperationState(from: OperationState): TOperationState = from match {
    case INITIALIZED => INITIALIZED_STATE
    case PENDING => PENDING_STATE
    case RUNNING => RUNNING_STATE
    case FINISHED => FINISHED_STATE
    case TIMEOUT => TIMEDOUT_STATE
    case CANCELED => CANCELED_STATE
    case CLOSED => CLOSED_STATE
    case ERROR => ERROR_STATE
    case _ => UKNOWN_STATE
  }

  def validateTransition(oldState: OperationState, newState: OperationState): Unit = {
    oldState match {
      case INITIALIZED if Set(PENDING, RUNNING, TIMEOUT, CANCELED, CLOSED).contains(newState) =>
      case PENDING
        if Set(RUNNING, FINISHED, TIMEOUT, CANCELED, CLOSED, ERROR).contains(newState) =>
      case RUNNING if Set(FINISHED, TIMEOUT, CANCELED, CLOSED, ERROR).contains(newState) =>
      case FINISHED | CANCELED | TIMEOUT | ERROR if CLOSED.equals(newState) =>
      case _ => throw KyuubiSQLException(
        s"Illegal Operation state transition from $oldState to $newState")
    }
  }

  def isTerminal(state: OperationState): Boolean = state match {
    case FINISHED | TIMEOUT | CANCELED | CLOSED | ERROR => true
    case _ => false
  }
}
