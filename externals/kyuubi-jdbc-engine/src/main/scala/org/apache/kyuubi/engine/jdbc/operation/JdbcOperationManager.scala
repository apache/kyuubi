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
package org.apache.kyuubi.engine.jdbc.operation

import java.util

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_JDBC_FETCH_SIZE, ENGINE_JDBC_OPERATION_INCREMENTAL_COLLECT, ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED}
import org.apache.kyuubi.engine.jdbc.session.JdbcSessionImpl
import org.apache.kyuubi.engine.jdbc.util.SupportServiceLoader
import org.apache.kyuubi.operation.{Operation, OperationManager}
import org.apache.kyuubi.session.Session

class JdbcOperationManager(conf: KyuubiConf) extends OperationManager("JdbcOperationManager")
  with SupportServiceLoader {

  override def name(): String = "jdbc"

  override def newExecuteStatementOperation(
      session: Session,
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): Operation = {
    val normalizedConf = session.asInstanceOf[JdbcSessionImpl].normalizedConf
    val engineConf = session.sessionManager.getConf
    val convertCatalogDatabaseEnabled = normalizedConf
      .get(ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED.key)
      .map(_.toBoolean)
      .getOrElse(engineConf.get(ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED))
    // Intercept Kyuubi's special `_GET_CATALOG` / `_SET_CATALOG` / `use <db>` /
    // `select current_database()` statements (sent by KyuubiConnection.getCatalog/setCatalog/
    // setSchema/getSchema) and route them to the corresponding lifecycle operations instead
    // of forwarding to the backend JDBC driver as raw SQL.
    if (convertCatalogDatabaseEnabled) {
      val catalogDatabaseOperation = processCatalogDatabase(session, statement, confOverlay)
      if (catalogDatabaseOperation != null) {
        return catalogDatabaseOperation
      }
    }
    val incrementalCollect = normalizedConf
      .get(ENGINE_JDBC_OPERATION_INCREMENTAL_COLLECT.key)
      .map(_.toBoolean)
      .getOrElse(engineConf.get(ENGINE_JDBC_OPERATION_INCREMENTAL_COLLECT))
    val fetchSize = normalizedConf
      .get(ENGINE_JDBC_FETCH_SIZE.key)
      .map(_.toInt)
      .getOrElse(engineConf.get(ENGINE_JDBC_FETCH_SIZE))
    val executeStatement =
      new ExecuteStatement(
        session,
        statement,
        runAsync,
        queryTimeout,
        incrementalCollect,
        fetchSize)
    addOperation(executeStatement)
  }

  override def newGetTypeInfoOperation(session: Session): Operation =
    addOperation(new GetTypeInfo(session))

  override def newGetCatalogsOperation(session: Session): Operation =
    addOperation(new GetCatalogs(session))

  override def newGetSchemasOperation(
      session: Session,
      catalog: String,
      schema: String): Operation =
    addOperation(new GetSchemas(session, catalog, schema))

  override def newGetTablesOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: util.List[String]): Operation = {
    val typesArray =
      if (tableTypes == null) null
      else tableTypes.toArray(Array.empty[String])
    addOperation(new GetTables(session, catalogName, schemaName, tableName, typesArray))
  }

  override def newGetTableTypesOperation(session: Session): Operation =
    addOperation(new GetTableTypes(session))

  override def newGetColumnsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation =
    addOperation(new GetColumns(session, catalogName, schemaName, tableName, columnName))

  override def newGetFunctionsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      functionName: String): Operation =
    addOperation(new GetFunctions(session, catalogName, schemaName, functionName))

  override def newGetPrimaryKeysOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String): Operation =
    addOperation(new GetPrimaryKeys(session, catalogName, schemaName, tableName))

  override def newGetCrossReferenceOperation(
      session: Session,
      primaryCatalog: String,
      primarySchema: String,
      primaryTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): Operation =
    addOperation(new GetCrossReference(
      session,
      primaryCatalog,
      primarySchema,
      primaryTable,
      foreignCatalog,
      foreignSchema,
      foreignTable))

  override def getQueryId(operation: Operation): String = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def newSetCurrentCatalogOperation(session: Session, catalog: String): Operation =
    addOperation(new SetCurrentCatalog(session, catalog))

  override def newGetCurrentCatalogOperation(session: Session): Operation =
    addOperation(new GetCurrentCatalog(session))

  override def newSetCurrentDatabaseOperation(session: Session, database: String): Operation =
    addOperation(new SetCurrentDatabase(session, database))

  override def newGetCurrentDatabaseOperation(session: Session): Operation =
    addOperation(new GetCurrentDatabase(session))
}
