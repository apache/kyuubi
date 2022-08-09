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

package org.apache.kyuubi.restore.operation

import org.apache.kyuubi.operation.{KyuubiOperation, OperationHandle}
import org.apache.kyuubi.session.Session

class RestoredKyuubiOperation(
    session: Session,
    operationHandle: String,
    remoteOperationHandle: String) extends KyuubiOperation(session) {

  val handle: OperationHandle = OperationHandle(operationHandle)

  override def getHandle: OperationHandle = handle

  override protected def runInternal(): Unit = {
    throw new UnsupportedOperationException("Can not run restored operation.")
  }

  def reopen(): Unit = {
    setRemoteOpHandle(OperationHandle(remoteOperationHandle))
    beforeRun()
    afterRun()
  }
}
