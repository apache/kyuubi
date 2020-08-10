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

import java.nio.ByteBuffer

import org.apache.hive.service.rpc.thrift.{TColumn, TRow, TRowSet, TStringColumn, TTableSchema}

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.operation.FetchOrientation.FetchOrientation
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.session.Session

/**
 * The [[OperationManager]] manages all the operations during their lifecycle.
 *
 *
 * @param name Service Name
 */
abstract class OperationManager(name: String) extends AbstractService(name) {

  private final val handleToOperation = new java.util.HashMap[OperationHandle, Operation]()

  override def initialize(conf: KyuubiConf): Unit = {
    super.initialize(conf)
  }

  override def start(): Unit = super.start()

  override def stop(): Unit = super.stop()

  def newExecuteStatementOperation(
      session: Session,
      statement: String,
      runAsync: Boolean,
      queryTimeout: Long): Operation
  def newGetTypeInfoOperation(session: Session): Operation
  def newGetCatalogsOperation(session: Session): Operation
  def newGetSchemasOperation(session: Session, catalog: String, schema: String): Operation
  def newGetTablesOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: java.util.List[String]): Operation
  def newGetTableTypesOperation(session: Session): Operation
  def newGetColumnsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): Operation
  def newGetFunctionsOperation(
      session: Session,
      catalogName: String,
      schemaName: String,
      functionName: String): Operation

  final def addOperation(operation: Operation): Operation = synchronized {
    handleToOperation.put(operation.getHandle, operation)
    operation
  }

  @throws[KyuubiSQLException]
  final def getOperation(opHandle: OperationHandle): Operation = {
    val operation = synchronized { handleToOperation.get(opHandle) }
    if (operation == null) throw KyuubiSQLException(s"Invalid $opHandle")
    operation
  }

  @throws[KyuubiSQLException]
  final def removeOperation(opHandle: OperationHandle): Operation = synchronized {
    val operation = handleToOperation.remove(opHandle)
    if (operation == null) throw KyuubiSQLException(s"Invalid $opHandle")
    operation
  }

  @throws[KyuubiSQLException]
  final def cancelOperation(opHandle: OperationHandle): Unit = {
    val operation = getOperation(opHandle)
    import OperationState._
    operation.getStatus.state match {
      case CANCELED | CLOSED | FINISHED | ERROR | UNKNOWN =>
      case _ => operation.cancel()
    }
  }

  @throws[KyuubiSQLException]
  final def closeOperation(opHandle: OperationHandle): Unit = {
    val operation = removeOperation(opHandle)
    operation.close()
  }

  final def getOperationResultSetSchema(opHandle: OperationHandle): TTableSchema = {
    getOperation(opHandle).getResultSetSchema
  }

  final def getOperationNextRowSet(
      opHandle: OperationHandle,
      order: FetchOrientation,
      maxRows: Int): TRowSet = {
    getOperation(opHandle).getNextRowSet(order, maxRows)
  }

  def getOperationLogRowSet(
      opHandle: OperationHandle,
      order: FetchOrientation,
      maxRows: Int): TRowSet = {
    // TODO: Support fetch log result
    val values = new java.util.ArrayList[String]
    val tColumn = TColumn.stringVal(new TStringColumn(values, ByteBuffer.allocate(0)))
    val tRow = new TRowSet(0, new java.util.ArrayList[TRow](1))
    tRow.addToColumns(tColumn)
    tRow
  }

  final def removeExpiredOperations(handles: Seq[OperationHandle]): Seq[Operation] = synchronized {
    handles.map(handleToOperation.get).filter { operation =>
      val isTimeout = operation != null && operation.isTimedOut
      if (isTimeout) {
        handleToOperation.remove(operation.getHandle)
        warn("Operation " + operation.getHandle + " is timed-out and will be closed")
        isTimeout
      } else {
        false
      }
    }
  }
}
