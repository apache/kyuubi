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
package org.apache.kyuubi.grpc.session

import java.util

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.grpc.operation.{GrpcOperation, OperationKey}
import org.apache.kyuubi.grpc.utils.ProtoUtils

abstract class AbstractGrpcSession(
    val userId: String) extends GrpcSession with Logging {
  override val sessionKey: SessionKey = SessionKey(userId)

  final private val _createTime: Long = System.currentTimeMillis()
  override def createTime: Long = _createTime

  @volatile private var _lastAccessTime: Long = _createTime
  override def lastAccessTime: Long = _lastAccessTime

  @volatile private var closedTimeMs: Option[Long] = None

  @volatile private var _lastIdleTime: Long = _createTime
  override def lastIdleTime: Long = _lastIdleTime

  final private val opKeySet = new util.HashSet[OperationKey]

  protected def runGrpcOperation(operation: GrpcOperation): OperationKey = {
    if (closedTimeMs.isDefined) {
      throw KyuubiSQLException("Cannot build operation because the session is closed")
    }
    try {
      val opKey = operation.operationKey
      opKeySet.add(opKey)
      operation.run()
      opKey
    } catch {
      case e: KyuubiSQLException =>
        opKeySet.remove(operation.operationKey)
        sessionManager.grpcOperationManager.close(operation.operationKey)
        throw e
    }
  }

  override def removeOperation(operationKey: OperationKey): Unit = {
    sessionManager.grpcOperationManager.removeOperation(operationKey)
  }

  override def closeOperation(operationKey: OperationKey): Unit = {
    sessionManager.grpcOperationManager.closeOperation(operationKey)
  }

  override def interruptOperation(operationKey: OperationKey): Unit = {
    sessionManager.grpcOperationManager.interruptOperation(operationKey)
  }

  override def open(): Unit = {
    sessionEventsManager.postStarted()
  }

  override def close(): Unit = {
    if (closedTimeMs.isDefined) {
      throw KyuubiSQLException(s"Session ${sessionKey.sessionId} is already closed.")
    }
    closedTimeMs = Some(System.currentTimeMillis())
    sessionEventsManager.postClosed()
  }

}

object SessionTag {
  def apply(sessionKey: SessionKey, tag: String, prefix: String): String = {
    ProtoUtils.throwIfInvalidTag(tag)
    s"${prefix}_" +
      s"User_${sessionKey.userId}_" +
      s"Session_${sessionKey.sessionId}_" +
      s"Tag_${tag}"
  }

  def unapply(sessionTag: String, prefix: String): Option[String] = {
    if (sessionTag.startsWith(prefix)) Some(sessionTag) else None
  }
}
