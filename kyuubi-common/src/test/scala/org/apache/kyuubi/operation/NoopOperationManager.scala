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

import java.nio.ByteBuffer
import java.util

import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.{TColumn, TFetchResultsResp, TRow, TRowSet, TStatus, TStatusCode, TStringColumn}

class NoopOperationManager extends OperationManager("noop") {
  private val invalid = "invalid"

  override def newExecuteStatementOperation(
      session: Session,
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): Operation = {
    val operation = new NoopOperation(session, statement == invalid)
    addOperation(operation)
  }

  override def newSetCurrentCatalogOperation(session: Session, catalog: String): Operation = {
    val operation = new NoopOperation(session)
    addOperation(operation)
  }

  override def newGetCurrentCatalogOperation(session: Session): Operation = {
    val operation = new NoopOperation(session)
    addOperation(operation)
  }

  override def newSetCurrentDatabaseOperation(session: Session, database: String): Operation = {
    val operation = new NoopOperation(session)
    addOperation(operation)
  }

  override def newGetCurrentDatabaseOperation(session: Session): Operation = {
    val operation = new NoopOperation(session)
    addOperation(operation)
  }

  override def newGetTypeInfoOperation(session: Session): Operation = {
    val operation = new NoopOperation(session)
    addOperation(operation)
  }

  override def newGetCatalogsOperation(session: Session): Operation = {
    val operation = new NoopOperation(session)
    addOperation(operation)
  }

  override def newGetSchemasOperation(
      session: Session,
      catalog: String,
      schema: String): Operation = {
    val operation = new NoopOperation(session)
    addOperation(operation)
  }

  override def newGetTablesOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: java.util.List[String]): Operation = {
    val operation = new NoopOperation(session, schemaName == invalid)
    addOperation(operation)
  }

  override def newGetTableTypesOperation(session: Session): Operation = {
    val operation = new NoopOperation(session)
    addOperation(operation)
  }

  override def newGetColumnsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation = {
    val operation = new NoopOperation(session)
    addOperation(operation)
  }

  override def newGetFunctionsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      functionName: String): Operation = {
    val operation = new NoopOperation(session)
    addOperation(operation)
  }

  override def newGetPrimaryKeysOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String): Operation = {
    val operation =
      new NoopOperation(session, schemaName == invalid)
    addOperation(operation)
  }

  override def newGetCrossReferenceOperation(
      session: Session,
      primaryCatalog: String,
      primarySchema: String,
      primaryTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): Operation = {
    val operation =
      new NoopOperation(session, primarySchema == invalid)
    addOperation(operation)
  }

  override def getOperationLogRowSet(
      opHandle: OperationHandle,
      order: FetchOrientation,
      maxRows: Int): TFetchResultsResp = {
    val logs = new util.ArrayList[String]()
    logs.add("test")
    val tColumn = TColumn.stringVal(new TStringColumn(logs, ByteBuffer.allocate(0)))
    val tRow = new TRowSet(0, new util.ArrayList[TRow](logs.size()))
    tRow.addToColumns(tColumn)
    val resp = new TFetchResultsResp(new TStatus(TStatusCode.SUCCESS_STATUS))
    resp.setResults(tRow)
    resp.setHasMoreRows(false)
    resp
  }

  override def getQueryId(operation: Operation): String = {
    val queryId = "noop_query_id"
    queryId
  }
}
