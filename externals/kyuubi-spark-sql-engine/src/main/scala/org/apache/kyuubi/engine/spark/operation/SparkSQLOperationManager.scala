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

import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.engine.spark.shim.SparkCatalogShim
import org.apache.kyuubi.operation.{Operation, OperationManager}
import org.apache.kyuubi.session.{Session, SessionHandle}

class SparkSQLOperationManager private (name: String) extends OperationManager(name) {

  def this() = this(classOf[SparkSQLOperationManager].getSimpleName)

  private val sessionToSpark = new ConcurrentHashMap[SessionHandle, SparkSession]()

  private def getSparkSession(sessionHandle: SessionHandle): SparkSession = {
    val sparkSession = sessionToSpark.get(sessionHandle)
    if (sparkSession == null) {
      throw KyuubiSQLException(s"$sessionHandle has not been initialized or already been closed")
    }
    sparkSession
  }

  def setSparkSession(sessionHandle: SessionHandle, spark: SparkSession): Unit = {
    sessionToSpark.put(sessionHandle, spark)
  }

  def removeSparkSession(sessionHandle: SessionHandle): SparkSession = {
    sessionToSpark.remove(sessionHandle)
  }

  def getOpenSparkSessionCount: Int = sessionToSpark.size()

  override def newExecuteStatementOperation(
      session: Session,
      statement: String,
      runAsync: Boolean,
      queryTimeout: Long): Operation = {
    val spark = getSparkSession(session.handle)
    val operation = new ExecuteStatement(spark, session, statement, runAsync, queryTimeout)
    addOperation(operation)
  }

  override def newGetTypeInfoOperation(session: Session): Operation = {
    val spark = getSparkSession(session.handle)
    val op = new GetTypeInfo(spark, session)
    addOperation(op)
  }

  override def newGetCatalogsOperation(session: Session): Operation = {
    val spark = getSparkSession(session.handle)
    val op = new GetCatalogs(spark, session)
    addOperation(op)
  }

  override def newGetSchemasOperation(
      session: Session,
      catalog: String,
      schema: String): Operation = {
    val spark = getSparkSession(session.handle)
    val op = new GetSchemas(spark, session, catalog, schema)
    addOperation(op)
  }

  override def newGetTablesOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: java.util.List[String]): Operation = {
    val spark = getSparkSession(session.handle)
    val tTypes = if (tableTypes == null || tableTypes.isEmpty) {
      SparkCatalogShim.sparkTableTypes
    } else {
      tableTypes.asScala.toSet
    }
    val op = new GetTables(spark, session, catalogName, schemaName, tableName, tTypes)
    addOperation(op)
  }

  override def newGetTableTypesOperation(session: Session): Operation = {
    val spark = getSparkSession(session.handle)
    val op = new GetTableTypes(spark, session)
    addOperation(op)
  }

  override def newGetColumnsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation = {
    val spark = getSparkSession(session.handle)
    val op = new GetColumns(spark, session, catalogName, schemaName, tableName, columnName)
    addOperation(op)
  }

  override def newGetFunctionsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      functionName: String): Operation = {
    val spark = getSparkSession(session.handle)
    val op = new GetFunctions(spark, session, catalogName, schemaName, functionName)
    addOperation(op)
  }
}
