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

import org.apache.kyuubi.engine.{ApplicationInfo, ApplicationState}
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.KyuubiSessionImpl

class LaunchEngine(session: KyuubiSessionImpl, override val shouldRunAsync: Boolean)
  extends KyuubiApplicationOperation(session) {

  private lazy val _operationLog: OperationLog =
    if (shouldRunAsync) {
      OperationLog.createOperationLog(session, getHandle)
    } else {
      // when launch engine synchronously, operation log is not needed
      null
    }
  override def getOperationLog: Option[OperationLog] = Option(_operationLog)

  override protected def currentApplicationInfo(): Option[ApplicationInfo] = {
    Option(client).map { cli =>
      ApplicationInfo(
        cli.engineId.orNull,
        cli.engineName.orNull,
        ApplicationState.RUNNING,
        cli.engineUrl)
    }
  }

  override protected def beforeRun(): Unit = {
    OperationLog.setCurrentOperationLog(_operationLog)
    setHasResultSet(false)
    setState(OperationState.PENDING)
  }

  override protected def afterRun(): Unit = {
    OperationLog.removeCurrentOperationLog()
  }

  override protected def runInternal(): Unit = {
    val asyncOperation: Runnable = () => {
      setState(OperationState.RUNNING)
      try {
        session.openEngineSession(getOperationLog)
        setState(OperationState.FINISHED)
      } catch onError()
    }
    try {
      val opHandle = session.sessionManager.submitBackgroundOperation(asyncOperation)
      setBackgroundHandle(opHandle)
    } catch onError("submitting open engine operation in background, request rejected")

    if (!shouldRunAsync) getBackgroundHandle.get()
  }

  override protected def applicationInfoMap: Option[Map[String, String]] = {
    super.applicationInfoMap.map { _ + ("refId" -> session.engine.getEngineRefId) }
  }

}
