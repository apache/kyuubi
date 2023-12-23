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

import org.apache.hive.service.cli.{FetchOrientation => HiveFetchOrientation}
import org.apache.hive.service.cli.operation.{Operation, OperationManager}
import org.apache.hive.service.cli.session.{HiveSession, SessionManager => HiveSessionManager}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY
import org.apache.kyuubi.engine.hive.session.HiveSessionImpl
import org.apache.kyuubi.engine.hive.util.HiveRpcUtils
import org.apache.kyuubi.operation.{AbstractOperation, FetchOrientation, OperationState, OperationStatus}
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TFetchResultsResp, TGetResultSetMetadataResp}

abstract class HiveOperation(session: Session) extends AbstractOperation(session) {

  protected val hive: HiveSession = session.asInstanceOf[HiveSessionImpl].hive

  protected def delegatedSessionManager: HiveSessionManager = hive.getSessionManager

  protected def delegatedOperationManager: OperationManager = {
    delegatedSessionManager.getOperationManager
  }

  val internalHiveOperation: Operation

  override def beforeRun(): Unit = {
    setState(OperationState.RUNNING)
    hive.getHiveConf.set(KYUUBI_SESSION_USER_KEY, session.user)
  }

  override def afterRun(): Unit = {
    withLockRequired {
      if (!isTerminalState(state)) {
        setState(OperationState.FINISHED)
        hive.getHiveConf.unset(KYUUBI_SESSION_USER_KEY)
      }
    }
  }
  override def runInternal(): Unit = {
    internalHiveOperation.run()
    val hasResultSet = internalHiveOperation.getStatus.getHasResultSet
    setHasResultSet(hasResultSet)
  }

  override def getBackgroundHandle: Future[_] = {
    internalHiveOperation.getBackgroundHandle
  }

  override def cancel(): Unit = {
    delegatedOperationManager.cancelOperation(internalHiveOperation.getHandle)
  }

  override def close(): Unit = {
    delegatedOperationManager.closeOperation(internalHiveOperation.getHandle)
  }

  override def getStatus: OperationStatus = {
    val status = internalHiveOperation.getStatus
    val state = OperationState.withName(status.getState.name().stripSuffix("_STATE"))

    OperationStatus(
      state,
      createTime,
      status.getOperationStarted,
      lastAccessTime,
      status.getOperationCompleted,
      hasResultSet,
      Option(status.getOperationException).map(KyuubiSQLException(_)))
  }

  override def getResultSetMetadata: TGetResultSetMetadataResp = {
    val schema = internalHiveOperation.getResultSetSchema.toTTableSchema
    val resp = new TGetResultSetMetadataResp
    resp.setSchema(HiveRpcUtils.asKyuubi(schema))
    resp.setStatus(OK_STATUS)
    resp
  }

  override def getNextRowSetInternal(
      order: FetchOrientation,
      rowSetSize: Int): TFetchResultsResp = {
    val hiveTOrder = HiveRpcUtils.asHive(FetchOrientation.toTFetchOrientation(order))
    val hiveOrder = HiveFetchOrientation.getFetchOrientation(hiveTOrder)
    val rowSet = internalHiveOperation.getNextRowSet(hiveOrder, rowSetSize)
    val resp = new TFetchResultsResp(OK_STATUS)
    resp.setResults(HiveRpcUtils.asKyuubi(rowSet.toTRowSet))
    resp.setHasMoreRows(false)
    resp
  }

  def getOperationLogRowSet(order: FetchOrientation, rowSetSize: Int): TFetchResultsResp = {
    val hiveTOrder = HiveRpcUtils.asHive(FetchOrientation.toTFetchOrientation(order))
    val hiveOrder = HiveFetchOrientation.getFetchOrientation(hiveTOrder)
    val handle = internalHiveOperation.getHandle
    val rowSet = delegatedOperationManager.getOperationLogRowSet(
      handle,
      hiveOrder,
      rowSetSize,
      hive.getHiveConf).toTRowSet
    val resp = new TFetchResultsResp(OK_STATUS)
    resp.setResults(HiveRpcUtils.asKyuubi(rowSet))
    resp.setHasMoreRows(false)
    resp
  }

  override def isTimedOut: Boolean = internalHiveOperation.isTimedOut(System.currentTimeMillis)

  override def shouldRunAsync: Boolean = false
}
