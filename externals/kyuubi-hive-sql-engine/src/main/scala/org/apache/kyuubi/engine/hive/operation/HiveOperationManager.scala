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

import java.sql.SQLException
import java.util.List

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.metastore.api.{FieldSchema, Schema}
import org.apache.hive.service.cli.{RowSetFactory, TableSchema}
import org.apache.hive.service.rpc.thrift.TRowSet

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.operation.{Operation, OperationHandle, OperationManager}
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.session.Session

class HiveOperationManager() extends OperationManager("HiveOperationManager") {
  override def newExecuteStatementOperation(
      session: Session,
      statement: String,
      confOverlay: Map[String, String],
      runAsync: Boolean,
      queryTimeout: Long): Operation = {
    val operation = new ExecuteStatement(session, statement, confOverlay, runAsync, queryTimeout)
    addOperation(operation)
  }

  override def newGetTypeInfoOperation(session: Session): Operation = {
    throw KyuubiSQLException.featureNotSupported()
  }

  override def newGetCatalogsOperation(session: Session): Operation = {
    val operation = new GetCatalogs(session)
    addOperation(operation)
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
      tableTypes: List[String]): Operation = {
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

  override def getOperationLogRowSet(
      opHandle: OperationHandle,
      order: FetchOrientation,
      maxRows: Int): TRowSet = {
    def getLogSchema: TableSchema = {
      val schema = new Schema
      val fieldSchema = new FieldSchema
      fieldSchema.setName("operation_log")
      fieldSchema.setType("string")
      schema.addToFieldSchemas(fieldSchema)
      new TableSchema(schema)
    }

    val operation = getOperation(opHandle).asInstanceOf[HiveOperation]
    val internalHiveOperation = operation.internalHiveOperation

    val rowSet = RowSetFactory.create(getLogSchema, operation.getProtocolVersion, false)
    val operationLog = internalHiveOperation.getOperationLog
    if (operationLog == null) {
      throw KyuubiSQLException("Couldn't find log associated with operation handle: " + opHandle)
    }

    try {
      val logs = operationLog.readOperationLog(false, maxRows)
      for (log <- logs.asScala) {
        rowSet.addRow(Array(log))
      }
    } catch {
      case e: SQLException =>
        throw new KyuubiSQLException(e.getMessage, e.getCause)
    }

    rowSet.toTRowSet
  }
}
