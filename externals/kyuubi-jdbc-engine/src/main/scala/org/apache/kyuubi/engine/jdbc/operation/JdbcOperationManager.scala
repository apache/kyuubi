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

import java.sql.Connection
import java.util

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_JDBC_FETCH_SIZE, ENGINE_JDBC_OPERATION_INCREMENTAL_COLLECT, ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED}
import org.apache.kyuubi.engine.jdbc.dialect.JdbcDialect
import org.apache.kyuubi.engine.jdbc.operation.ExecuteMetaDataOperation.MetaDataCall
import org.apache.kyuubi.engine.jdbc.session.JdbcSessionImpl
import org.apache.kyuubi.engine.jdbc.util.SupportServiceLoader
import org.apache.kyuubi.operation.{Operation, OperationManager}
import org.apache.kyuubi.session.Session

class JdbcOperationManager(conf: KyuubiConf) extends OperationManager("JdbcOperationManager")
  with SupportServiceLoader {

  // No engine-level dialect cached: each operation resolves dialect from its sessionConf and
  // injects it into the closures below so the metadata call and downstream normalisation
  // never disagree.

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

  override def newGetTypeInfoOperation(session: Session): Operation = {
    val call: MetaDataCall = (dialect, conn) => dialect.getTypeInfo(conn)
    addOperation(new ExecuteMetaDataOperation(session, call))
  }

  override def newGetCatalogsOperation(session: Session): Operation = {
    val call: MetaDataCall = (dialect, conn) => dialect.getCatalogs(conn)
    addOperation(new ExecuteMetaDataOperation(session, call))
  }

  override def newGetSchemasOperation(
      session: Session,
      catalog: String,
      schema: String): Operation = {
    val call: MetaDataCall = (dialect, conn) => dialect.getSchemas(conn, catalog, schema)
    addOperation(new ExecuteMetaDataOperation(session, call))
  }

  override def newGetTablesOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: util.List[String]): Operation = {
    val typesArray =
      if (tableTypes == null) null
      else tableTypes.toArray(Array.empty[String])
    val call: MetaDataCall =
      (dialect, conn) => dialect.getTables(conn, catalogName, schemaName, tableName, typesArray)
    addOperation(new ExecuteMetaDataOperation(session, call))
  }

  override def newGetTableTypesOperation(session: Session): Operation = {
    val call: MetaDataCall = (dialect, conn) => dialect.getTableTypes(conn)
    addOperation(new ExecuteMetaDataOperation(session, call))
  }

  override def newGetColumnsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation = {
    val call: MetaDataCall =
      (dialect, conn) => dialect.getColumns(conn, catalogName, schemaName, tableName, columnName)
    addOperation(new ExecuteMetaDataOperation(session, call))
  }

  override def newGetFunctionsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      functionName: String): Operation = {
    val call: MetaDataCall =
      (dialect, conn) => dialect.getFunctions(conn, catalogName, schemaName, functionName)
    addOperation(new ExecuteMetaDataOperation(session, call))
  }

  override def newGetPrimaryKeysOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String): Operation = {
    val call: MetaDataCall =
      (dialect, conn) => dialect.getPrimaryKeys(conn, catalogName, schemaName, tableName)
    addOperation(new ExecuteMetaDataOperation(session, call))
  }

  override def newGetCrossReferenceOperation(
      session: Session,
      primaryCatalog: String,
      primarySchema: String,
      primaryTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): Operation = {
    val call: MetaDataCall = (dialect, conn) =>
      dialect.getCrossReference(
        conn,
        primaryCatalog,
        primarySchema,
        primaryTable,
        foreignCatalog,
        foreignSchema,
        foreignTable)
    addOperation(new ExecuteMetaDataOperation(session, call))
  }

  override def getQueryId(operation: Operation): String = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def newSetCurrentCatalogOperation(session: Session, catalog: String): Operation = {
    val action: (JdbcDialect, Connection) => Unit =
      (dialect, conn) => dialect.setCatalog(conn, catalog)
    addOperation(new SetCurrentCatalog(session, action))
  }

  override def newGetCurrentCatalogOperation(session: Session): Operation = {
    val fetch: (JdbcDialect, Connection) => String =
      (dialect, conn) => dialect.getCatalog(conn)
    addOperation(new GetCurrentCatalog(session, fetch))
  }

  override def newSetCurrentDatabaseOperation(session: Session, database: String): Operation = {
    val action: (JdbcDialect, Connection) => Unit =
      (dialect, conn) => dialect.setSchema(conn, database)
    addOperation(new SetCurrentDatabase(session, action))
  }

  override def newGetCurrentDatabaseOperation(session: Session): Operation = {
    val fetch: (JdbcDialect, Connection) => String =
      (dialect, conn) => dialect.getCurrentSchema(conn)
    addOperation(new GetCurrentDatabase(session, fetch))
  }
}
