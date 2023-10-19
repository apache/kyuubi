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

import com.google.common.collect.ImmutableList
import io.trino.client.{ClientTypeSignature, ClientTypeSignatureParameter, Column}
import io.trino.client.ClientStandardTypes.VARCHAR
import io.trino.client.ClientTypeSignature.VARCHAR_UNBOUNDED_LENGTH

import org.apache.kyuubi.operation.IterableFetchIterator
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.session.Session

class GetCurrentDatabase(session: Session)
  extends TrinoOperation(session) {

  private val operationLog: OperationLog = OperationLog.createOperationLog(session, getHandle)

  override def getOperationLog: Option[OperationLog] = Option(operationLog)

  override protected def runInternal(): Unit = {
    try {
      val session = trinoContext.clientSession.get
      val catalog = session.getSchema

      val clientTypeSignature = new ClientTypeSignature(
        VARCHAR,
        ImmutableList.of(ClientTypeSignatureParameter.ofLong(VARCHAR_UNBOUNDED_LENGTH)))
      val column = new Column("TABLE_SCHEM", VARCHAR, clientTypeSignature)
      schema = List[Column](column)
      iter = new IterableFetchIterator(List(List(catalog)))
    } catch onError()
  }
}
