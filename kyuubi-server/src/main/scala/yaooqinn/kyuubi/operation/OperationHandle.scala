/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.operation

import java.util.Objects

import org.apache.hive.service.cli.thrift.{TOperationHandle, TProtocolVersion}

import yaooqinn.kyuubi.cli.{Handle, HandleIdentifier}

class OperationHandle private(
    opType: OperationType,
    protocol: TProtocolVersion,
    handleId: HandleIdentifier) extends Handle(handleId) {

  private var hasResultSet: Boolean = false

  def this(opType: OperationType, protocol: TProtocolVersion) =
    this(opType, protocol, new HandleIdentifier)

  def this(tOperationHandle: TOperationHandle, protocol: TProtocolVersion) = {
    this(
      OperationType.getOperationType(tOperationHandle.getOperationType),
      protocol,
      new HandleIdentifier(tOperationHandle.getOperationId))
    setHasResultSet(tOperationHandle.isHasResultSet)
  }

  def this(tOperationHandle: TOperationHandle) =
    this(tOperationHandle, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1)

  def getOperationType: OperationType = opType

  def toTOperationHandle: TOperationHandle = {
    val tOperationHandle = new TOperationHandle
    tOperationHandle.setOperationId(getHandleIdentifier.toTHandleIdentifier)
    tOperationHandle.setOperationType(opType.toTOperationType)
    tOperationHandle.setHasResultSet(this.hasResultSet)
    tOperationHandle
  }

  def setHasResultSet(hasResultSet: Boolean): Unit = {
    this.hasResultSet = hasResultSet
  }

  def isHasResultSet: Boolean = this.hasResultSet

  def getProtocolVersion: TProtocolVersion = protocol

  override def hashCode: Int = 31 * super.hashCode + Objects.hashCode(opType)

  override def equals(obj: Any): Boolean = {
    obj match {
      case o: OperationHandle if opType == o.getOperationType && super.equals(o) => true
      case _ => false
    }
  }

  override def toString: String =
    "OperationHandle [opType=" + opType + ", getHandleIdentifier()=" + getHandleIdentifier + "]"

}
