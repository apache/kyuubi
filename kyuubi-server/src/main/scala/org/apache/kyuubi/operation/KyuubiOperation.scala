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

import java.io.IOException

import com.codahale.metrics.MetricRegistry
import org.apache.commons.lang3.StringUtils
import org.apache.hive.service.rpc.thrift._
import org.apache.thrift.TException
import org.apache.thrift.transport.TTransportException

import org.apache.kyuubi.{KyuubiSQLException, Utils}
import org.apache.kyuubi.metrics.MetricsConstants.{OPERATION_FAIL, OPERATION_OPEN, OPERATION_STATE, OPERATION_TOTAL}
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.operation.OperationState.OperationState
import org.apache.kyuubi.session.{KyuubiSessionImpl, KyuubiSessionManager, Session}
import org.apache.kyuubi.util.ThriftUtils

abstract class KyuubiOperation(session: Session) extends AbstractOperation(session) {

  MetricsSystem.tracing { ms =>
    ms.incCount(MetricRegistry.name(OPERATION_OPEN, opType))
    ms.incCount(MetricRegistry.name(OPERATION_TOTAL, opType))
    ms.markMeter(MetricRegistry.name(OPERATION_STATE, opType, state.toString.toLowerCase))
    ms.incCount(MetricRegistry.name(OPERATION_TOTAL))
    ms.markMeter(MetricRegistry.name(OPERATION_STATE, state.toString.toLowerCase))
  }

  protected[operation] lazy val client = session.asInstanceOf[KyuubiSessionImpl].client

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
          MetricsSystem.tracing(_.incCount(
            MetricRegistry.name(OPERATION_FAIL, opType, errorType)))
          val ke = e match {
            case kse: KyuubiSQLException => kse
            case te: TTransportException
                if te.getType == TTransportException.END_OF_FILE &&
                  StringUtils.isEmpty(te.getMessage) =>
              // https://issues.apache.org/jira/browse/THRIFT-4858
              KyuubiSQLException(
                s"Error $action $opType: Socket for ${session.handle} is closed",
                e)
            case e =>
              KyuubiSQLException(s"Error $action $opType: ${Utils.stringifyException(e)}", e)
          }
          setOperationException(ke)
          setState(OperationState.ERROR)
          throw ke
        }
      }
  }

  override protected def beforeRun(): Unit = {
    setHasResultSet(true)
    setState(OperationState.RUNNING)
    sendCredentialsIfNeeded()
  }

  protected def sendCredentialsIfNeeded(): Unit = {
    val appUser = session.asInstanceOf[KyuubiSessionImpl].engine.appUser
    val sessionManager = session.sessionManager.asInstanceOf[KyuubiSessionManager]
    sessionManager.credentialsManager.sendCredentialsIfNeeded(
      session.handle.identifier.toString,
      appUser,
      client.sendCredentials)
  }

  override protected def afterRun(): Unit = {
    state.synchronized {
      if (!isTerminalState(state)) {
        setState(OperationState.FINISHED)
      }
    }
  }

  override def cancel(): Unit = state.synchronized {
    if (!isClosedOrCanceled) {
      setState(OperationState.CANCELED)
      MetricsSystem.tracing(_.decCount(MetricRegistry.name(OPERATION_OPEN, opType)))
      if (_remoteOpHandle != null) {
        try {
          client.cancelOperation(_remoteOpHandle)
        } catch {
          case e @ (_: TException | _: KyuubiSQLException) =>
            warn(s"Error cancelling ${_remoteOpHandle.getOperationId}: ${e.getMessage}", e)
        }
      }
    }
  }

  override def close(): Unit = state.synchronized {
    if (!isClosedOrCanceled) {
      setState(OperationState.CLOSED)
      MetricsSystem.tracing(_.decCount(MetricRegistry.name(OPERATION_OPEN, opType)))
      try {
        // For launch engine operation, we use OperationLog to pass engine submit log but
        // at that time we do not have remoteOpHandle
        getOperationLog.foreach(_.close())
      } catch {
        case e: IOException => error(e.getMessage, e)
      }
      if (_remoteOpHandle != null) {
        try {
          client.closeOperation(_remoteOpHandle)
        } catch {
          case e @ (_: TException | _: KyuubiSQLException) =>
            warn(s"Error closing ${_remoteOpHandle.getOperationId}: ${e.getMessage}", e)
        }
      }
    }
  }

  override def getResultSetMetadata: TGetResultSetMetadataResp = {
    if (_remoteOpHandle == null) {
      val tColumnDesc = new TColumnDesc()
      tColumnDesc.setColumnName("Result")
      val desc = new TTypeDesc
      desc.addToTypes(TTypeEntry.primitiveEntry(new TPrimitiveTypeEntry(TTypeId.STRING_TYPE)))
      tColumnDesc.setTypeDesc(desc)
      tColumnDesc.setPosition(0)
      val schema = new TTableSchema()
      schema.addToColumns(tColumnDesc)
      val resp = new TGetResultSetMetadataResp
      resp.setSchema(schema)
      resp.setStatus(OK_STATUS)
      resp
    } else {
      client.getResultSetMetadata(_remoteOpHandle)
    }
  }

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Int): TRowSet = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    client.fetchResults(_remoteOpHandle, order, rowSetSize, fetchLog = false)
  }

  override def shouldRunAsync: Boolean = false

  override def setState(newState: OperationState): Unit = {
    MetricsSystem.tracing { ms =>
      ms.markMeter(MetricRegistry.name(OPERATION_STATE, opType, state.toString.toLowerCase), -1)
      ms.markMeter(MetricRegistry.name(OPERATION_STATE, opType, newState.toString.toLowerCase))
      ms.markMeter(MetricRegistry.name(OPERATION_STATE, newState.toString.toLowerCase))
    }
    super.setState(newState)
  }
}
