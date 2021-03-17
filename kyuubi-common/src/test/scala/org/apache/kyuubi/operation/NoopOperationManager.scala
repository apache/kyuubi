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

import org.apache.hive.service.rpc.thrift.{TRow, TRowSet}

import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.util.ThriftUtils

class NoopOperationManager extends OperationManager("noop") {
  private val invalid = "invalid"

  override def newExecuteStatementOperation(
      session: Session,
      statement: String,
      runAsync: Boolean,
      queryTimeout: Long): Operation = {
    val operation =
      new NoopOperation(OperationType.EXECUTE_STATEMENT, session, statement == invalid)
    addOperation(operation)
  }

  override def newGetTypeInfoOperation(session: Session): Operation = {
    val operation = new NoopOperation(OperationType.GET_TYPE_INFO, session)
    addOperation(operation)
  }

  override def newGetCatalogsOperation(session: Session): Operation = {
    val operation = new NoopOperation(OperationType.GET_CATALOGS, session)
    addOperation(operation)
  }

  override def newGetSchemasOperation(
      session: Session,
      catalog: String,
      schema: String): Operation = {
    val operation = new NoopOperation(OperationType.GET_SCHEMAS, session)
    addOperation(operation)
  }

  override def newGetTablesOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: java.util.List[String]): Operation = {
    val operation = new NoopOperation(OperationType.GET_TABLES, session, schemaName == invalid)
    addOperation(operation)
  }

  override def newGetTableTypesOperation(session: Session): Operation = {
    val operation = new NoopOperation(OperationType.GET_TABLE_TYPES, session)
    addOperation(operation)
  }

  override def newGetColumnsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation = {
    val operation = new NoopOperation(OperationType.GET_COLUMNS, session)
    addOperation(operation)
  }

  override def newGetFunctionsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      functionName: String): Operation = {
    val operation = new NoopOperation(OperationType.GET_FUNCTIONS, session)
    addOperation(operation)
  }

  override def getOperationLogRowSet(
      opHandle: OperationHandle,
      order: FetchOrientation,
      maxRows: Int): TRowSet = ThriftUtils.EMPTY_ROW_SET
}
