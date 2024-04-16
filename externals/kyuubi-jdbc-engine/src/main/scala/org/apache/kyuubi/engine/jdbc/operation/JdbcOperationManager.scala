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
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_JDBC_FETCH_SIZE, ENGINE_JDBC_OPERATION_INCREMENTAL_COLLECT}
import org.apache.kyuubi.engine.jdbc.dialect.{JdbcDialect, JdbcDialects}
import org.apache.kyuubi.engine.jdbc.session.JdbcSessionImpl
import org.apache.kyuubi.engine.jdbc.util.SupportServiceLoader
import org.apache.kyuubi.operation.{Operation, OperationManager}
import org.apache.kyuubi.session.Session

class JdbcOperationManager(conf: KyuubiConf) extends OperationManager("JdbcOperationManager")
  with SupportServiceLoader {

  private lazy val dialect: JdbcDialect = JdbcDialects.get(conf)

  override def name(): String = "jdbc"

  override def newExecuteStatementOperation(
      session: Session,
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): Operation = {
    val normalizedConf = session.asInstanceOf[JdbcSessionImpl].normalizedConf
    val incrementalCollect = normalizedConf.get(ENGINE_JDBC_OPERATION_INCREMENTAL_COLLECT.key).map(
      _.toBoolean).getOrElse(
      session.sessionManager.getConf.get(ENGINE_JDBC_OPERATION_INCREMENTAL_COLLECT))
    val fetchSize = normalizedConf.get(ENGINE_JDBC_FETCH_SIZE.key).map(_.toInt)
      .getOrElse(session.sessionManager.getConf.get(ENGINE_JDBC_FETCH_SIZE))
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
    val operation = dialect.getTypeInfoOperation(session)
    addOperation(operation)
  }

  override def newGetCatalogsOperation(session: Session): Operation = {
    val query = dialect.getCatalogsOperation()
    val normalizedConf = session.asInstanceOf[JdbcSessionImpl].normalizedConf
    val fetchSize = normalizedConf.get(ENGINE_JDBC_FETCH_SIZE.key).map(_.toInt)
      .getOrElse(session.sessionManager.getConf.get(ENGINE_JDBC_FETCH_SIZE))
    val executeStatement =
      new ExecuteStatement(session, query, false, 0L, true, fetchSize)
    addOperation(executeStatement)
  }

  override def newGetSchemasOperation(
      session: Session,
      catalog: String,
      schema: String): Operation = {
    val query = dialect.getSchemasOperation(catalog, schema)
    val normalizedConf = session.asInstanceOf[JdbcSessionImpl].normalizedConf
    val fetchSize = normalizedConf.get(ENGINE_JDBC_FETCH_SIZE.key).map(_.toInt)
      .getOrElse(session.sessionManager.getConf.get(ENGINE_JDBC_FETCH_SIZE))
    val executeStatement =
      new ExecuteStatement(session, query, false, 0L, true, fetchSize)
    addOperation(executeStatement)
  }

  override def newGetTablesOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: util.List[String]): Operation = {
    val query = dialect.getTablesQuery(catalogName, schemaName, tableName, tableTypes)
    val normalizedConf = session.asInstanceOf[JdbcSessionImpl].normalizedConf
    val fetchSize = normalizedConf.get(ENGINE_JDBC_FETCH_SIZE.key).map(_.toInt)
      .getOrElse(session.sessionManager.getConf.get(ENGINE_JDBC_FETCH_SIZE))
    val executeStatement =
      new ExecuteStatement(session, query, false, 0L, true, fetchSize)
    addOperation(executeStatement)
  }

  override def newGetTableTypesOperation(session: Session): Operation = {
    val operation = dialect.getTableTypesOperation(session)
    addOperation(operation)
  }

  override def newGetColumnsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation = {
    val query = dialect.getColumnsQuery(session, catalogName, schemaName, tableName, columnName)
    val normalizedConf = session.asInstanceOf[JdbcSessionImpl].normalizedConf
    val fetchSize = normalizedConf.get(ENGINE_JDBC_FETCH_SIZE.key).map(
      _.toInt).getOrElse(
      session.sessionManager.getConf.get(ENGINE_JDBC_FETCH_SIZE))
    val executeStatement =
      new ExecuteStatement(session, query, false, 0L, true, fetchSize)
    addOperation(executeStatement)
  }

  override def newGetFunctionsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      functionName: String): Operation = {
    val operation = dialect.getFunctionsOperation(session)
    addOperation(operation)
  }

  override def newGetPrimaryKeysOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String): Operation = {
    val operation = dialect.getPrimaryKeysOperation(session)
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
    val operation = dialect.getCrossReferenceOperation(session)
    addOperation(operation)
  }

  override def getQueryId(operation: Operation): String = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def newSetCurrentCatalogOperation(session: Session, catalog: String): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def newGetCurrentCatalogOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def newSetCurrentDatabaseOperation(session: Session, database: String): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def newGetCurrentDatabaseOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }
}
