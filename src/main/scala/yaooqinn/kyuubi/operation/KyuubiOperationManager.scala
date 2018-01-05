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

package yaooqinn.kyuubi.operation

import java.sql.SQLException
import java.util.{ArrayList, HashMap => JMap, List => JList}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.metastore.api.{FieldSchema, Schema}
import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.hive.service.cli._
import org.apache.log4j.Logger
import org.apache.spark.{KyuubiConf, SparkConf, SparkUtils}

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.service.AbstractService
import yaooqinn.kyuubi.session.KyuubiSession

private[kyuubi] class KyuubiOperationManager private(name: String)
  extends AbstractService(name) with Logging {

  def this() = this(classOf[KyuubiOperationManager].getSimpleName)

  private[this] val handleToOperation = new JMap[OperationHandle, KyuubiSQLOperation]

  val userToOperationLog = new ConcurrentHashMap[String, OperationLog]()

  override def init(conf: SparkConf): Unit = synchronized {
    if (conf.getBoolean(KyuubiConf.KYUUBI_LOGGING_OPERATION_ENABLED.key, defaultValue = true)) {
      initOperationLogCapture()
    } else {
      debug("Operation level logging is turned off")
    }
    super.init(conf)
  }

  private[this] def initOperationLogCapture(): Unit = {
    // Register another Appender (with the same layout) that talks to us.
    val ap = new LogDivertAppender(this)
    Logger.getRootLogger.addAppender(ap)
  }

  private[this] def getOperationLogByThread: OperationLog = OperationLog.getCurrentOperationLog

  private[this] def getOperationLogByName: OperationLog = {
    if (!userToOperationLog.isEmpty) {
      userToOperationLog.get(SparkUtils.getCurrentUserName())
    } else {
      null
    }
  }

  def getOperationLog: OperationLog = {
    Option(getOperationLogByThread).getOrElse(getOperationLogByName)
  }

  def setOperationLog(user: String, log: OperationLog): Unit = {
    OperationLog.setCurrentOperationLog(log)
    userToOperationLog.put(Option(user).getOrElse(SparkUtils.getCurrentUserName()), log)
  }

  def unregisterOperationLog(user: String): Unit = {
    OperationLog.removeCurrentOperationLog()
    userToOperationLog.remove(user)
  }

  def newExecuteStatementOperation(
      parentSession: KyuubiSession,
      statement: String): KyuubiSQLOperation = synchronized {
    val operation = new KyuubiSQLOperation(parentSession, statement)
    addOperation(operation)
    operation
  }

  def getOperation(operationHandle: OperationHandle): KyuubiSQLOperation = {
    val operation = getOperationInternal(operationHandle)
    if (operation == null) throw new HiveSQLException("Invalid OperationHandle: " + operationHandle)
    operation
  }

  private[this] def getOperationInternal(operationHandle: OperationHandle) =
    handleToOperation.get(operationHandle)

  private[this] def addOperation(operation: KyuubiSQLOperation): Unit = {
    handleToOperation.put(operation.getHandle, operation)
  }

  private[this] def removeOperation(opHandle: OperationHandle) =
    handleToOperation.remove(opHandle)

  @throws[HiveSQLException]
  def cancelOperation(opHandle: OperationHandle): Unit = {
    val operation = getOperation(opHandle)
    val opState = operation.getStatus.getState
    if ((opState eq OperationState.CANCELED)
      || (opState eq OperationState.CLOSED)
      || (opState eq OperationState.FINISHED)
      || (opState eq OperationState.ERROR)
      || (opState eq OperationState.UNKNOWN)) {
      // Cancel should be a no-op in either cases
      debug(opHandle + ": Operation is already aborted in state - " + opState)
    }
    else {
      debug(opHandle + ": Attempting to cancel from state - " + opState)
      operation.cancel()
    }
  }

  @throws[HiveSQLException]
  def closeOperation(opHandle: OperationHandle): Unit = {
    val operation = removeOperation(opHandle)
    if (operation == null) throw new HiveSQLException("Operation does not exist!")
    operation.close()
  }

  @throws[HiveSQLException]
  def getOperationNextRowSet(
      opHandle: OperationHandle,
      orientation: FetchOrientation, maxRows: Long): RowSet =
    getOperation(opHandle).getNextRowSet(orientation, maxRows)

  @throws[HiveSQLException]
  def getOperationLogRowSet(
      opHandle: OperationHandle,
      orientation: FetchOrientation, maxRows: Long): RowSet = {
    // get the OperationLog object from the operation
    val operationLog: OperationLog = getOperation(opHandle).getOperationLog
    if (operationLog == null) {
      throw new HiveSQLException("Couldn't find log associated with operation handle: " + opHandle)
    }
    // read logs
    var logs: JList[String] = new ArrayList[String]()
    try
      logs = operationLog.readOperationLog(isFetchFirst(orientation), maxRows)
    catch {
      case e: SQLException =>
        throw new HiveSQLException(e.getMessage, e.getCause)
    }
    // convert logs to RowSet
    val tableSchema: TableSchema = new TableSchema(getLogSchema)
    val rowSet: RowSet =
      RowSetFactory.create(tableSchema, getOperation(opHandle).getProtocolVersion)
    for (log <- logs.asScala) {
      rowSet.addRow(Array[AnyRef](log))
    }
    rowSet
  }

  private[this] def isFetchFirst(fetchOrientation: FetchOrientation): Boolean = {
    fetchOrientation == FetchOrientation.FETCH_FIRST
  }

  private[this] def getLogSchema: Schema = {
    val schema: Schema = new Schema
    val fieldSchema: FieldSchema = new FieldSchema
    fieldSchema.setName("operation_log")
    fieldSchema.setType("string")
    schema.addToFieldSchemas(fieldSchema)
    schema
  }
}
