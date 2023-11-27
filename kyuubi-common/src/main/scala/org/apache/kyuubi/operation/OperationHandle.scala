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

import java.util.UUID

import org.apache.kyuubi.cli.Handle
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TOperationHandle

case class OperationHandle(identifier: UUID) {

  private var _hasResultSet: Boolean = _

  def setHasResultSet(hasResultSet: Boolean): Unit = _hasResultSet = hasResultSet

  def toTOperationHandle: TOperationHandle = {
    val tOperationHandle = new TOperationHandle
    tOperationHandle.setOperationId(Handle.toTHandleIdentifier(identifier))
    tOperationHandle.setHasResultSet(_hasResultSet)
    tOperationHandle
  }

  override def toString: String = s"OperationHandle [$identifier]"

}

object OperationHandle {
  def apply(): OperationHandle = new OperationHandle(UUID.randomUUID())

  def apply(tOperationHandle: TOperationHandle): OperationHandle = {
    new OperationHandle(
      Handle.fromTHandleIdentifier(tOperationHandle.getOperationId))
  }

  def apply(operationHandleStr: String): OperationHandle = {
    OperationHandle(UUID.fromString(operationHandleStr))
  }
}
