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

package org.apache.kyuubi.engine.trino.operation

import io.trino.client.ClientSession

import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

class SetCurrentCatalog(session: Session, catalog: String)
  extends TrinoOperation(session) {

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)

  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  override protected def runInternal(): Unit = {
    try {
      val session = trinoContext.clientSession.get
      var builder = ClientSession.builder(session)
      builder = builder.catalog(catalog)
      trinoContext.clientSession.set(builder.build())
      setHasResultSet(false)
    } catch onError()
  }
}
