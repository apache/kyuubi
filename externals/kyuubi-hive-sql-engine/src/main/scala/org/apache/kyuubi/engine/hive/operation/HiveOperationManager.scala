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

package org.apache.kyuubi.engine.hive.operation

import java.util.List

import org.apache.hadoop.hive.conf.HiveConf

import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.hive.session.HiveSessionImpl
import org.apache.kyuubi.operation.{Operation, OperationHandle, OperationManager}
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.session.Session
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TFetchResultsResp

class HiveOperationManager() extends OperationManager("HiveOperationManager") {
  // we use hive's operation log
  override protected def skipOperationLog: Boolean = true

  override def newExecuteStatementOperation(
      session: Session,
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): Operation = {
    val normalizedConf = session.asInstanceOf[HiveSessionImpl].normalizedConf
    if (normalizedConf.get(ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED.key).map(
        _.toBoolean).getOrElse(
        session.sessionManager.getConf.get(ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED))) {
      val catalogDatabaseOperation = processCatalogDatabase(session, statement, confOverlay)
      if (catalogDatabaseOperation != null) {
        return catalogDatabaseOperation
      }
    }
    val operation = new ExecuteStatement(session, statement, confOverlay, runAsync, queryTimeout)
    addOperation(operation)
  }

  override def newSetCurrentCatalogOperation(session: Session, catalog: String): Operation = {
    val op = new SetCurrentCatalog(session, catalog)
    addOperation(op)
  }

  override def newGetCurrentCatalogOperation(session: Session): Operation = {
    val op = new GetCurrentCatalog(session)
    addOperation(op)
  }

  override def newSetCurrentDatabaseOperation(session: Session, database: String): Operation = {
    val op = new SetCurrentDatabase(session, database)
    addOperation(op)
  }

  override def newGetCurrentDatabaseOperation(session: Session): Operation = {
    val op = new GetCurrentDatabase(session)
    addOperation(op)
  }

  override def newGetTypeInfoOperation(session: Session): Operation = {
    val operation = new GetTypeInfo(session)
    addOperation(operation)
  }

  override def newGetCatalogsOperation(session: Session): Operation = {
    val operation = new GetCatalogs(session)
    addOperation(operation)
  }

  override def newGetSchemasOperation(
      session: Session,
      catalog: String,
      schema: String): Operation = {
    val operation = new GetSchemas(session, catalog, schema)
    addOperation(operation)
  }

  override def newGetTablesOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: List[String]): Operation = {
    val operation = new GetTables(session, catalogName, schemaName, tableName, tableTypes)
    addOperation(operation)
  }

  override def newGetTableTypesOperation(session: Session): Operation = {
    val operation = new GetTableTypes(session)
    addOperation(operation)
  }

  override def newGetColumnsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation = {
    val operation = new GetColumns(session, catalogName, schemaName, tableName, columnName)
    addOperation(operation)
  }

  override def newGetFunctionsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      functionName: String): Operation = {
    val operation = new GetFunctions(session, catalogName, schemaName, functionName)
    addOperation(operation)
  }

  override def newGetPrimaryKeysOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String): Operation = {
    val operation = new GetPrimaryKeys(session, catalogName, schemaName, tableName)
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
    val operation = new GetCrossReference(
      session,
      primaryCatalog,
      primarySchema,
      primaryTable,
      foreignCatalog,
      foreignSchema,
      foreignTable)
    addOperation(operation)
  }

  override def getOperationLogRowSet(
      opHandle: OperationHandle,
      order: FetchOrientation,
      maxRows: Int): TFetchResultsResp = {
    val operation = getOperation(opHandle).asInstanceOf[HiveOperation]
    operation.getOperationLogRowSet(order, maxRows)
  }

  override def getQueryId(operation: Operation): String = {
    val hiveOperation = operation.asInstanceOf[HiveOperation]
    val internalHiveOperation = hiveOperation.internalHiveOperation
    internalHiveOperation.getParentSession.getHiveConf.getVar(
      HiveConf.getConfVars("hive.query.id"))
  }
}
