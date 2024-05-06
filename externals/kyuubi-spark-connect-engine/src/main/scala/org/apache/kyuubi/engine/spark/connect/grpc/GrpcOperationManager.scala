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
package org.apache.kyuubi.engine.spark.connect.grpc

import java.util

import scala.jdk.CollectionConverters.iterableAsScalaIterableConverter

import io.grpc.stub.StreamObserver

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.spark.connect.grpc.proto.{ConfigRequest, ConfigResponse}
import org.apache.kyuubi.operation.{Operation, OperationHandle, OperationManager}
import org.apache.kyuubi.operation.OperationState.{CANCELED, CLOSED, ERROR, FINISHED, UNKNOWN}
import org.apache.kyuubi.operation.log.LogDivertAppender
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.session.Session

abstract class GrpcOperationManager(name: String) extends OperationManager(name) {
  final private val handleToOperation = new util.HashMap[OperationHandle, Operation]()

  override def initialize(conf: KyuubiConf): Unit = {
    LogDivertAppender.initialize(skipOperationLog)
    super.initialize(conf)
  }

  def newConfigOperation(
      session: Session,
      request: ConfigRequest,
      response: StreamObserver[ConfigResponse]): Operation

  override def newExecuteStatementOperation(
      session: Session,
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): Operation = {
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

  override def newGetTypeInfoOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def newGetCatalogsOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def newGetSchemasOperation(
      session: Session,
      catalog: String,
      schema: String): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def newGetTablesOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: util.List[String]): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def newGetTableTypesOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def newGetColumnsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def newGetFunctionsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      functionName: String): Operation = {
    throw KyuubiSQLException.featureNotSupported()
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
