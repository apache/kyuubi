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

package org.apache.kyuubi.engine.hive.operation

import java.util.concurrent.Future

import org.apache.hive.service.cli.{OperationState => HiveOperationState}
import org.apache.hive.service.cli.operation.Operation
import org.apache.hive.service.cli.operation.OperationManager
import org.apache.hive.service.cli.session.{HiveSession, SessionManager => HiveSessionManager}
import org.apache.hive.service.rpc.thrift.{TRowSet, TTableSchema}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.hive.session.HiveSessionImpl
import org.apache.kyuubi.operation.{AbstractOperation, FetchOrientation, OperationState, OperationStatus}
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.OperationType.OperationType
import org.apache.kyuubi.session.Session

abstract class HiveOperation(opType: OperationType, session: Session)
  extends AbstractOperation(opType, session) {

  protected val hive: HiveSession = session.asInstanceOf[HiveSessionImpl].hive

  protected def delegatedSessionManager: HiveSessionManager = hive.getSessionManager

  protected def delegatedOperationManager: OperationManager = {
    delegatedSessionManager.getOperationManager
  }

  val internalHiveOperation: Operation

  override def beforeRun(): Unit = {
    setState(OperationState.RUNNING)
  }

  override def afterRun(): Unit = {
    state.synchronized {
      if (!isTerminalState(state)) {
        setState(OperationState.FINISHED)
      }
    }
  }
  override def runInternal(): Unit = {
    internalHiveOperation.run()
  }

  override def getBackgroundHandle: Future[_] = {
    internalHiveOperation.getBackgroundHandle
  }

  override def cancel(): Unit = {
    internalHiveOperation.cancel(HiveOperationState.CANCELED)
  }

  override def close(): Unit = {
    internalHiveOperation.close()
  }

  override def getStatus: OperationStatus = {
    super.getStatus
    val status = internalHiveOperation.getStatus
    val state = OperationState.withName(status.getState.name().stripSuffix("_STATE"))

    OperationStatus(
      state,
      createTime,
      status.getOperationStarted,
      lastAccessTime,
      status.getOperationCompleted,
      status.getHasResultSet,
      Option(status.getOperationException).map(KyuubiSQLException(_)))
  }

  override def getResultSetSchema: TTableSchema = {
    internalHiveOperation.getResultSetSchema.toTTableSchema
  }

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet = {
    val tOrder = FetchOrientation.toTFetchOrientation(order)
    val hiveOrder = org.apache.hive.service.cli.FetchOrientation.getFetchOrientation(tOrder)
    val rowSet = internalHiveOperation.getNextRowSet(hiveOrder, rowSetSize)
    rowSet.toTRowSet
  }

  override def isTimedOut: Boolean = internalHiveOperation.isTimedOut(System.currentTimeMillis)

  override def shouldRunAsync: Boolean = false
}
