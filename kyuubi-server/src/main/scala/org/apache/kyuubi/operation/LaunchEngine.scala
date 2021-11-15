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

import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.KyuubiSessionImpl

class LaunchEngine(session: KyuubiSessionImpl, override val shouldRunAsync: Boolean) extends
  KyuubiOperation(OperationType.LAUNCH_ENGINE, session) {
  import LaunchEngine._

  override protected val operationTimeout: Long = 0

  private lazy val _operationLog: OperationLog = if (shouldRunAsync) {
    OperationLog.createOperationLog(session, getHandle)
  } else {
    // when launch engine synchronously, operation log is not needed
    null
  }
  override def getOperationLog: Option[OperationLog] = Option(_operationLog)

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(_operationLog)
    setHasResultSet(false)
    setState(OperationState.PENDING)
  }

  override protected def afterRun(): Unit = { }

  override protected def runInternal(): Unit = {
    val asyncOperation: Runnable = () => {
      setState(OperationState.RUNNING)
      try {
        session.openEngineSession(getOperationLog)
        setState(OperationState.FINISHED)
      } catch {
        onError()
      } finally {
        new Thread(s"close-launch-engine-op-${getHandle}") {
          override def run(): Unit = {
            if (shouldRunAsync) {
              // delay to close it for async mode to enable client to get more launch engine log
              Thread.sleep(DEFAULT_LAUNCH_ENGINE_OPERATION_CLOSE_DELAY)
            }
            if (!isClosedOrCanceled) {
              session.closeOperation(getHandle)
            }
          }
        }.start()
      }
    }
    try {
      val opHandle = session.sessionManager.submitBackgroundOperation(asyncOperation)
      setBackgroundHandle(opHandle)
    } catch onError("submitting open engine operation in background, request rejected")

    if (!shouldRunAsync) getBackgroundHandle.get()
  }

  override def close(): Unit = {
    OperationLog.removeCurrentOperationLog()
    super.close()
  }
}

object LaunchEngine {
  val DEFAULT_LAUNCH_ENGINE_OPERATION_CLOSE_DELAY = 10 * 1000
}
