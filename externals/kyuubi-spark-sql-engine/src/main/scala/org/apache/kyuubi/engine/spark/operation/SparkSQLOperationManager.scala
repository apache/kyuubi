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

package org.apache.kyuubi.engine.spark.operation

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.spark.repl.KyuubiSparkILoop
import org.apache.kyuubi.engine.spark.session.SparkSessionImpl
import org.apache.kyuubi.engine.spark.shim.SparkCatalogShim
import org.apache.kyuubi.operation.{NoneMode, Operation, OperationManager, PlanOnlyMode}
import org.apache.kyuubi.session.{Session, SessionHandle}

class SparkSQLOperationManager private (name: String) extends OperationManager(name) {

  def this() = this(classOf[SparkSQLOperationManager].getSimpleName)

  private lazy val planOnlyModeDefault = getConf.get(OPERATION_PLAN_ONLY_MODE)
  private lazy val operationIncrementalCollectDefault = getConf.get(OPERATION_INCREMENTAL_COLLECT)
  private lazy val operationLanguageDefault = getConf.get(OPERATION_LANGUAGE)
  private lazy val operationConvertCatalogDatabaseDefault =
    getConf.get(ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED)

  private val sessionToRepl = new ConcurrentHashMap[SessionHandle, KyuubiSparkILoop]().asScala
  private val sessionToPythonProcess =
    new ConcurrentHashMap[SessionHandle, SessionPythonWorker]().asScala

  def closeILoop(session: SessionHandle): Unit = {
    val maybeRepl = sessionToRepl.remove(session)
    maybeRepl.foreach(_.close())
  }

  def closePythonProcess(session: SessionHandle): Unit = {
    val maybeProcess = sessionToPythonProcess.remove(session)
    maybeProcess.foreach(_.close)
  }

  override def newExecuteStatementOperation(
      session: Session,
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): Operation = {
    val spark = session.asInstanceOf[SparkSessionImpl].spark
    if (spark.conf.getOption(ENGINE_OPERATION_CONVERT_CATALOG_DATABASE_ENABLED.key)
        .map(_.toBoolean).getOrElse(operationConvertCatalogDatabaseDefault)) {
      val catalogDatabaseOperation = processCatalogDatabase(session, statement, confOverlay)
      if (catalogDatabaseOperation != null) {
        return catalogDatabaseOperation
      }
    }
    val lang = OperationLanguages(confOverlay.getOrElse(
      OPERATION_LANGUAGE.key,
      spark.conf.get(OPERATION_LANGUAGE.key, operationLanguageDefault)))
    val operation =
      lang match {
        case OperationLanguages.SQL =>
          val mode = PlanOnlyMode.fromString(spark.conf.get(
            OPERATION_PLAN_ONLY_MODE.key,
            planOnlyModeDefault))

          spark.conf.set(OPERATION_PLAN_ONLY_MODE.key, mode.name)
          mode match {
            case NoneMode =>
              val incrementalCollect = spark.conf.getOption(OPERATION_INCREMENTAL_COLLECT.key)
                .map(_.toBoolean).getOrElse(operationIncrementalCollectDefault)
              new ExecuteStatement(session, statement, runAsync, queryTimeout, incrementalCollect)
            case mode =>
              new PlanOnlyStatement(session, statement, mode)
          }
        case OperationLanguages.SCALA =>
          val repl = sessionToRepl.getOrElseUpdate(session.handle, KyuubiSparkILoop(spark))
          new ExecuteScala(session, repl, statement, runAsync, queryTimeout)
        case OperationLanguages.PYTHON =>
          try {
            ExecutePython.init()
            val worker = sessionToPythonProcess.getOrElseUpdate(
              session.handle,
              ExecutePython.createSessionPythonWorker(spark, session))
            new ExecutePython(session, statement, worker)
          } catch {
            case e: Throwable =>
              spark.conf.set(OPERATION_LANGUAGE.key, OperationLanguages.SQL.toString)
              throw KyuubiSQLException(
                s"Failed to init python environment, fall back to SQL mode: ${e.getMessage}",
                e)
          }
        case OperationLanguages.UNKNOWN =>
          spark.conf.unset(OPERATION_LANGUAGE.key)
          throw KyuubiSQLException(s"The operation language $lang" +
            " doesn't support in Spark SQL engine.")
      }
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
      tableTypes: java.util.List[String]): Operation = {
    val tTypes =
      if (tableTypes == null || tableTypes.isEmpty) {
        SparkCatalogShim.sparkTableTypes
      } else {
        tableTypes.asScala.toSet
      }
    val op = new GetTables(session, catalogName, schemaName, tableName, tTypes)
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
    val op = new GetColumns(session, catalogName, schemaName, tableName, columnName)
    addOperation(op)
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
