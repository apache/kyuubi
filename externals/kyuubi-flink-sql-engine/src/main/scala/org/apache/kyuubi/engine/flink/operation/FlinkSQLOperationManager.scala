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

package org.apache.kyuubi.engine.flink.operation

import java.util
import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiConf.OperationModes._
import org.apache.kyuubi.engine.flink.result.Constants
import org.apache.kyuubi.engine.flink.session.FlinkSessionImpl
import org.apache.kyuubi.operation.{Operation, OperationManager}
import org.apache.kyuubi.session.Session

class FlinkSQLOperationManager extends OperationManager("FlinkSQLOperationManager") {

  private lazy val operationModeDefault = getConf.get(OPERATION_PLAN_ONLY_MODE)

  private lazy val resultMaxRowsDefault = getConf.get(ENGINE_FLINK_MAX_ROWS)

  private lazy val operationConvertCatalogDatabaseDefault =
    getConf.get(ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED)

  override def newExecuteStatementOperation(
      session: Session,
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): Operation = {
    val flinkSession = session.asInstanceOf[FlinkSessionImpl]
    if (flinkSession.sessionContext.getConfigMap.getOrDefault(
        ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED.key,
        operationConvertCatalogDatabaseDefault.toString).toBoolean) {
      val catalogDatabaseOperation = processCatalogDatabase(session, statement, confOverlay)
      if (catalogDatabaseOperation != null) {
        return catalogDatabaseOperation
      }
    }
    val mode = flinkSession.sessionContext.getConfigMap.getOrDefault(
      OPERATION_PLAN_ONLY_MODE.key,
      operationModeDefault)
    val resultMaxRows =
      flinkSession.normalizedConf.getOrElse(
        ENGINE_FLINK_MAX_ROWS.key,
        resultMaxRowsDefault.toString).toInt
    val op = OperationModes.withName(mode.toUpperCase(Locale.ROOT)) match {
      case NONE =>
        new ExecuteStatement(session, statement, runAsync, queryTimeout, resultMaxRows)
      case mode =>
        new PlanOnlyStatement(session, statement, mode)
    }
    addOperation(op)
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
    val op = new GetTypeInfo(session)
    addOperation(op)
  }

  override def newGetCatalogsOperation(session: Session): Operation = {
    val op = new GetCatalogs(session)
    addOperation(op)
  }

  override def newGetSchemasOperation(
      session: Session,
      catalog: String,
      schema: String): Operation = {
    val op = new GetSchemas(session, catalog, schema)
    addOperation(op)
  }

  override def newGetTablesOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: util.List[String]): Operation = {

    val tTypes =
      if (tableTypes == null || tableTypes.isEmpty) {
        Constants.SUPPORTED_TABLE_TYPES.toSet
      } else {
        tableTypes.asScala.toSet
      }

    val op = new GetTables(
      session = session,
      catalog = catalogName,
      schema = schemaName,
      tableName = tableName,
      tableTypes = tTypes)

    addOperation(op)
  }

  override def newGetTableTypesOperation(session: Session): Operation = {
    val op = new GetTableTypes(session)
    addOperation(op)
  }

  override def newGetColumnsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation = {
    throw new UnsupportedOperationException(
      "Unsupported Operation type GetColumns. You can execute " +
        "DESCRIBE statement instead to get column infos.")
  }

  override def newGetFunctionsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      functionName: String): Operation = {
    val op = new GetFunctions(session, catalogName, schemaName, functionName)
    addOperation(op)
  }

  override def newGetPrimaryKeysOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def newGetCrossReferenceOperation(
      session: Session,
      primaryCatalog: String,
      primarySchema: String,
      primaryTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def getQueryId(operation: Operation): String = {
    throw KyuubiSQLException.featureNotSupported()
  }
}
