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

import java.util

import org.apache.kyuubi.session.Session

abstract class GrpcOperationManager(name: String) extends OperationManager(name: String) {

  override def newExecuteStatementOperation(
      session: Session,
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): Operation = throw new UnsupportedOperationException

  override def newSetCurrentCatalogOperation(session: Session, catalog: String): Operation =
    throw new UnsupportedOperationException

  override def newGetCurrentCatalogOperation(session: Session): Operation =
    throw new UnsupportedOperationException

  override def newSetCurrentDatabaseOperation(session: Session, database: String): Operation =
    throw new UnsupportedOperationException

  override def newGetCurrentDatabaseOperation(session: Session): Operation =
    throw new UnsupportedOperationException

  override def newGetTypeInfoOperation(session: Session): Operation =
    throw new UnsupportedOperationException

  override def newGetCatalogsOperation(session: Session): Operation =
    throw new UnsupportedOperationException

  override def newGetSchemasOperation(
      session: Session,
      catalog: String,
      schema: String): Operation = throw new UnsupportedOperationException

  override def newGetTablesOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: util.List[String]): Operation = throw new UnsupportedOperationException

  override def newGetTableTypesOperation(session: Session): Operation =
    throw new UnsupportedOperationException

  override def newGetColumnsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation = throw new UnsupportedOperationException

  override def newGetFunctionsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      functionName: String): Operation = throw new UnsupportedOperationException

  override def newGetPrimaryKeysOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String): Operation = throw new UnsupportedOperationException

  override def newGetCrossReferenceOperation(
      session: Session,
      primaryCatalog: String,
      primarySchema: String,
      primaryTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): Operation = throw new UnsupportedOperationException

  override def getQueryId(operation: Operation): String = throw new UnsupportedOperationException
}
