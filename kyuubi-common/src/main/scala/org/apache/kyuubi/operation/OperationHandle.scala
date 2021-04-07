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

import java.util.Objects

import scala.language.implicitConversions

import org.apache.kyuubi.cli.{Handle, HandleIdentifier}
import org.apache.kyuubi.operation.OperationType.OperationType
import org.apache.kyuubi.shade.hive.service.rpc.thrift.{TOperationHandle, TProtocolVersion}

case class OperationHandle(
    identifier: HandleIdentifier,
    typ: OperationType,
    protocol: TProtocolVersion) extends Handle {

  private var _hasResultSet: Boolean = _

  def setHasResultSet(hasResultSet: Boolean): Unit = _hasResultSet = hasResultSet

  implicit def toTOperationHandle: TOperationHandle = {
    val tOperationHandle = new TOperationHandle
    tOperationHandle.setOperationId(identifier.toTHandleIdentifier)
    tOperationHandle.setOperationType(OperationType.toTOperationType(typ))
    tOperationHandle.setHasResultSet(_hasResultSet)
    tOperationHandle
  }

  override def hashCode(): Int = {
    (Objects.hashCode(identifier) + 31) * 31 + Objects.hashCode(typ)
  }

  override def equals(obj: Any): Boolean = obj match {
    case OperationHandle(id, typ, _) =>
      Objects.equals(this.typ, typ) && Objects.equals(this.identifier, id)
    case _ => false
  }

  override def toString: String = {
    s"OperationHandle [type=$typ, identifier: $identifier]"
  }
}

object OperationHandle {
  def apply(operationType: OperationType, protocol: TProtocolVersion): OperationHandle = {
    apply(HandleIdentifier(), operationType, protocol)
  }

  def apply(tOperationHandle: TOperationHandle, protocol: TProtocolVersion): OperationHandle = {
    apply(HandleIdentifier(tOperationHandle.getOperationId),
      OperationType.getOperationType(tOperationHandle.getOperationType),
      protocol)
  }

  def apply(tOperationHandle: TOperationHandle): OperationHandle = {
    apply(tOperationHandle, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)
  }

  implicit def toTOperationHandle(handle: OperationHandle): TOperationHandle = {
    val tOperationHandle = new TOperationHandle
    tOperationHandle.setOperationId(handle.identifier.toTHandleIdentifier)
    tOperationHandle.setOperationType(OperationType.toTOperationType(handle.typ))
    tOperationHandle.setHasResultSet(handle._hasResultSet)
    tOperationHandle
  }
}
