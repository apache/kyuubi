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
package org.apache.kyuubi.grpc.operation

import org.apache.kyuubi.{KyuubiSQLException, Logging}
import org.apache.kyuubi.grpc.event.SimpleOperationEventsManager
import org.apache.kyuubi.grpc.events.OperationEventsManager
import org.apache.kyuubi.grpc.session.SimpleGrpcSessionImpl
import org.apache.kyuubi.grpc.utils.SystemClock
import org.apache.kyuubi.operation.log.OperationLog

class SimpleGrpcOperationImpl(
    grpcSession: SimpleGrpcSessionImpl,
    shouldFail: Boolean = false)
  extends AbstractGrpcOperation[SimpleGrpcSessionImpl](grpcSession) with Logging {

  override def operationEventsManager: OperationEventsManager =
    new SimpleOperationEventsManager(this, new SystemClock())

  override def runInternal(): Unit = {
    if (shouldFail) {
      val exception = KyuubiSQLException("noop operation err")
      setOperationException(exception)
      operationEventsManager.postFailed(exception.getMessage)
    }
  }

  override def beforeRun(): Unit = {
    operationEventsManager.postStarted()
  }

  override def close(): Unit = {
    operationEventsManager.postClosed()
  }

  override def afterRun(): Unit = {
    info("afterRun")
  }

  override def interrupt(): Unit = {
    info("interrupt")
  }

  override def isTimedOut: Boolean = false
  override def getOperationLog: Option[OperationLog] = None

  override protected def key: OperationKey = OperationKey(grpcSession.sessionKey)
}
