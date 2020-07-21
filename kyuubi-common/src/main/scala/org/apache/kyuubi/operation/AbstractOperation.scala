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

import org.apache.hive.service.rpc.thrift.{TProtocolVersion, TRowSet, TTableSchema}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.OperationType.OperationType
import org.apache.kyuubi.session.Session

abstract class AbstractOperation(opType: OperationType, session: Session) extends Operation {
  import OperationState._
  private final val handle = OperationHandle(opType, session.protocol)
  private final val operationTimeout: Long = session.conf.get(KyuubiConf.OPERATION_IDLE_TIMEOUT)

  @volatile private var state: OperationState = INITIALIZED
  @volatile protected var startTime: Long = _
  @volatile protected var completedTime: Long = _
  @volatile protected var lastAccessTime: Long = System.currentTimeMillis()

  @volatile protected var operationException: KyuubiSQLException = _
  @volatile protected var hasResultSet: Boolean = false

  protected def setHasResultSet(hasResultSet: Boolean): Unit = {
    this.hasResultSet = hasResultSet
    handle.setHasResultSet(hasResultSet)
  }

  protected def setOperationException(opEx: KyuubiSQLException): Unit = {
    this.operationException = opEx
  }

  protected def setState(newState: OperationState): Unit = {
    OperationState.validateTransition(state, newState)
    state = newState

    state match {
      case RUNNING => startTime = System.currentTimeMillis()
      case ERROR | FINISHED | CANCELED => completedTime = System.currentTimeMillis()
      case _ =>
    }

    lastAccessTime = System.currentTimeMillis()
  }

  protected def isClosedOrCanceled: Boolean = {
    state == OperationState.CLOSED || state == OperationState.CANCELED
  }

  protected def assertState(state: OperationState): Unit = {
    if (this.state ne state) {
      throw new IllegalStateException(s"Expected state $state, but found ${this.state}")
    }
    lastAccessTime = System.currentTimeMillis()
  }

  /**
   * Verify if the given fetch orientation is part of the default orientation types.
   */
  protected def validateDefaultFetchOrientation(orientation: FetchOrientation): Unit = {
    validateFetchOrientation(orientation, Operation.DEFAULT_FETCH_ORIENTATION_SET)
  }

  /**
   * Verify if the given fetch orientation is part of the supported orientation types.
   */
  private def validateFetchOrientation(
      orientation: FetchOrientation,
      supportedOrientations: Set[FetchOrientation]): Unit = {
    if (!supportedOrientations.contains(orientation)) {
      throw KyuubiSQLException(s"The fetch type $orientation is not supported for this resultset")
    }
  }

  protected def runInternal(): Unit

  protected def beforeRun(): Unit

  protected def afterRun(): Unit

  override def run(): Unit = {
    beforeRun()
    try {
      runInternal()
    } finally {
      afterRun()
    }
  }

  override def cancel(): Unit

  override def close(): Unit

  override def getProtocolVersion: TProtocolVersion = handle.protocol

  override def getResultSetSchema: TTableSchema

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet

  override def getSession: Session = session

  override def getHandle: OperationHandle

  override def getStatus: OperationStatus = {
    OperationStatus(state, startTime, completedTime, hasResultSet, Option(operationException))
  }

  override def shouldRunAsync: Boolean

  override def isTimedOut: Boolean = {
    if (operationTimeout <= 0) {
      false
    } else {
      OperationState.isTerminal(state) &&
        lastAccessTime + operationTimeout <= System.currentTimeMillis()
    }
  }
}
