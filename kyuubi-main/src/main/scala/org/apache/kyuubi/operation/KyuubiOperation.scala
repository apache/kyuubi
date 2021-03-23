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

import com.codahale.metrics.MetricRegistry
import org.apache.commons.lang3.StringUtils
import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.transport.TTransportException

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.metrics.MetricsConstants.STATEMENT_FAIL
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.OperationType.OperationType
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.util.ThriftUtils

abstract class KyuubiOperation(
    opType: OperationType,
    session: Session,
    client: TCLIService.Iface,
    remoteSessionHandle: TSessionHandle) extends AbstractOperation(opType, session) {

  @volatile protected var _remoteOpHandle: TOperationHandle = _

  def remoteOpHandle(): TOperationHandle = _remoteOpHandle

  protected def verifyTStatus(tStatus: TStatus): Unit = {
    ThriftUtils.verifyTStatus(tStatus)
  }

  protected def onError(action: String = "operating"): PartialFunction[Throwable, Unit] = {
    case e: Throwable =>
      state.synchronized {
        if (isTerminalState(state)) {
          warn(s"Ignore exception in terminal state with $statementId: $e")
        } else {
          val errorType = e.getClass.getSimpleName
          MetricsSystem.tracing {
            _.decAndGetCount(MetricRegistry.name(STATEMENT_FAIL, errorType))
          }
          setState(OperationState.ERROR)
          val ke = e match {
            case kse: KyuubiSQLException => kse
            case te: TTransportException if te.getType == TTransportException.END_OF_FILE &&
                StringUtils.isEmpty(te.getMessage) =>
              // https://issues.apache.org/jira/browse/THRIFT-4858
              KyuubiSQLException(
                s"Error $action $opType: Socket for $remoteSessionHandle is closed", e)
            case _ =>
              KyuubiSQLException(s"Error $action $opType: ${e.getMessage}", e)
          }
          setOperationException(ke)
          throw ke
        }
      }
  }

  override protected def beforeRun(): Unit = {
    setHasResultSet(true)
    setState(OperationState.RUNNING)
  }

  override protected def afterRun(): Unit = {
    state.synchronized {
      if (!isTerminalState(state)) {
        setState(OperationState.FINISHED)
      }
    }
  }

  override def cancel(): Unit = {
    if (_remoteOpHandle != null && !isClosedOrCanceled) {
      try {
        val req = new TCancelOperationReq(_remoteOpHandle)
        val resp = client.CancelOperation(req)
        verifyTStatus(resp.getStatus)
        setState(OperationState.CANCELED)
      } catch onError("cancelling")
    }
  }

  override def close(): Unit = {
    if (_remoteOpHandle != null && !isClosedOrCanceled) {
      try {
        getOperationLog.foreach(_.close())
        val req = new TCloseOperationReq(_remoteOpHandle)
        val resp = client.CloseOperation(req)
        verifyTStatus(resp.getStatus)
        setState(OperationState.CLOSED)
      } catch onError("closing")
    }
  }

  override def getResultSetSchema: TTableSchema = {
    if (_remoteOpHandle == null) {
      val tColumnDesc = new TColumnDesc()
      tColumnDesc.setColumnName("Result")
      val desc = new TTypeDesc
      desc.addToTypes(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.STRING_TYPE)))
      tColumnDesc.setTypeDesc(desc)
      tColumnDesc.setPosition(0)
      val schema = new TTableSchema()
      schema.addToColumns(tColumnDesc)
      schema
    } else {
      val req = new TGetResultSetMetadataReq(_remoteOpHandle)
      val resp = client.GetResultSetMetadata(req)
      verifyTStatus(resp.getStatus)
      resp.getSchema
    }

  }

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    val req = new TFetchResultsReq(
      _remoteOpHandle, FetchOrientation.toTFetchOrientation(order), rowSetSize)
    val resp = client.FetchResults(req)
    verifyTStatus(resp.getStatus)
    resp.getResults
  }

  override def shouldRunAsync: Boolean = false
}
