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

import org.apache.hive.service.rpc.thrift.TRowSet

import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.session.Session

class NoopOperationManager extends OperationManager("noop") {
  override def newExecuteStatementOperation(
      session: Session,
      statement: String,
      runAsync: Boolean,
      queryTimeout: Long): Operation = new NoopOperation(session)

  override def newGetTypeInfoOperation(session: Session): Operation = new NoopOperation(session)

  override def newGetCatalogsOperation(session: Session): Operation = new NoopOperation(session)

  override def newGetSchemasOperation(
      session: Session,
      catalog: String,
      schema: String): Operation = {
    new NoopOperation(session)
  }

  override def newGetTablesOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: java.util.List[String]): Operation = {
    new NoopOperation(session)
  }

  override def newGetTableTypesOperation(session: Session): Operation = {
    new NoopOperation(session)
  }

  override def newGetColumnsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation = new NoopOperation(session)

  override def newGetFunctionsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      functionName: String): Operation = new NoopOperation(session)

  override def getOperationLogRowSet(
      opHandle: OperationHandle,
      order: FetchOrientation,
      maxRows: Int): TRowSet = new TRowSet()
}
