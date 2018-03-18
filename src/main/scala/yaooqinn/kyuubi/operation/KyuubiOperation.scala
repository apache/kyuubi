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

import java.io.{File, FileNotFoundException}
import java.security.PrivilegedExceptionAction
import java.sql.{Date, Timestamp}
import java.util.{Arrays, EnumSet, UUID}
import java.util.concurrent.{Future, RejectedExecutionException}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException
import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.KyuubiConf._
import org.apache.spark.SparkUtils
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSQLUtils}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.types._

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.session.KyuubiSession
import yaooqinn.kyuubi.ui.KyuubiServerMonitor

class KyuubiOperation(session: KyuubiSession, statement: String) extends Logging {

  private[this] var state: OperationState = INITIALIZED
  private[this] val opHandle: OperationHandle =
    new OperationHandle(EXECUTE_STATEMENT, session.getProtocolVersion)

  private[this] val operationTimeout =
    session.sparkSession().sparkContext.getConf.getTimeAsMs(OPERATION_IDLE_TIMEOUT.key)
  private[this] var lastAccessTime = System.currentTimeMillis()

  private[this] var hasResultSet: Boolean = false
  private[this] var operationException: HiveSQLException = _
  private[this] var backgroundHandle: Future[_] = _
  private[this] var operationLog: OperationLog = _
  private[this] var isOperationLogEnabled: Boolean = false

  private[this] var result: DataFrame = _
  private[this] var iter: Iterator[Row] = _
  private[this] var dataTypes: Array[DataType] = _
  private[this] var statementId: String = _

  private[this] lazy val resultSchema: TableSchema = {
    if (result == null || result.schema.isEmpty) {
      new TableSchema(Arrays.asList(new FieldSchema("Result", "string", "")))
    } else {
      info(s"Result Schema: ${result.schema}")
      KyuubiOperation.getTableSchema(result.schema)
    }
  }

  private[this] val DEFAULT_FETCH_ORIENTATION_SET: EnumSet[FetchOrientation] =
    EnumSet.of(FetchOrientation.FETCH_NEXT, FetchOrientation.FETCH_FIRST)

  def getBackgroundHandle: Future[_] = backgroundHandle

  def setBackgroundHandle(backgroundHandle: Future[_]): Unit = {
    this.backgroundHandle = backgroundHandle
  }

  def getSession: KyuubiSession = session

  def getHandle: OperationHandle = opHandle

  def getProtocolVersion: TProtocolVersion = opHandle.getProtocolVersion

  def getStatus: OperationStatus = new OperationStatus(state, operationException)

  def getOperationLog: OperationLog = operationLog

  private[this] def setOperationException(opEx: HiveSQLException): Unit = {
    this.operationException = opEx
  }

  @throws[HiveSQLException]
  private[this] def setState(newState: OperationState): Unit = {
    state.validateTransition(newState)
    this.state = newState
    this.lastAccessTime = System.currentTimeMillis()
  }

  private[this] def checkState(state: OperationState): Boolean = {
    this.state == state
  }
  
  private[this] def isClosedOrCanceled: Boolean = {
    checkState(CLOSED) || checkState(CANCELED)
  }

  @throws[HiveSQLException]
  private[this] def assertState(state: OperationState): Unit = {
    if (this.state ne state) {
      throw new HiveSQLException("Expected state " + state + ", but found " + this.state)
    }
    this.lastAccessTime = System.currentTimeMillis()
  }

  private[this] def createOperationLog(): Unit = {
    if (session.isOperationLogEnabled) {
      val logFile =
        new File(session.getSessionLogDir, opHandle.getHandleIdentifier.toString)
      val logFilePath = logFile.getAbsolutePath
      this.isOperationLogEnabled = true
      // create log file
      try {
        if (logFile.exists) {
          warn(
            s"""
               |The operation log file should not exist, but it is already there: $logFilePath"
             """.stripMargin)
          logFile.delete
        }
        if (!logFile.createNewFile) {
          // the log file already exists and cannot be deleted.
          // If it can be read/written, keep its contents and use it.
          if (!logFile.canRead || !logFile.canWrite) {
            warn(
              s"""
                 |The already existed operation log file cannot be recreated,
                 |and it cannot be read or written: $logFilePath"
               """.stripMargin)
            this.isOperationLogEnabled = false
            return
          }
        }
      } catch {
        case e: Exception =>
          warn("Unable to create operation log file: " + logFilePath, e)
          this.isOperationLogEnabled = false
          return
      }
      // create OperationLog object with above log file
      try {
        this.operationLog = new OperationLog(this.opHandle.toString, logFile, new HiveConf())
      } catch {
        case e: FileNotFoundException =>
          warn("Unable to instantiate OperationLog object for operation: " + this.opHandle, e)
          this.isOperationLogEnabled = false
          return
      }
      // register this operationLog
      session.getSessionMgr.getOperationMgr
        .setOperationLog(session.getUserName, this.operationLog)
    }
  }

  private[this] def registerCurrentOperationLog(): Unit = {
    if (isOperationLogEnabled) {
      if (operationLog == null) {
        warn("Failed to get current OperationLog object of Operation: "
          + getHandle.getHandleIdentifier)
        isOperationLogEnabled = false
      } else {
        session.getSessionMgr.getOperationMgr
          .setOperationLog(session.getUserName, operationLog)
      }
    }
  }

  private[this] def unregisterOperationLog(): Unit = {
    if (isOperationLogEnabled) {
      session.getSessionMgr.getOperationMgr
        .unregisterOperationLog(session.getUserName)
    }
  }

  @throws[HiveSQLException]
  def run(): Unit = {
    createOperationLog()
    try {
      runInternal()
    } finally {
      unregisterOperationLog()
    }
  }

  private[this] def cleanupOperationLog(): Unit = {
    if (isOperationLogEnabled) {
      if (operationLog == null) {
        error("Operation [ " + opHandle.getHandleIdentifier + " ] " +
          "logging is enabled, but its OperationLog object cannot be found.")
      } else {
        operationLog.close()
      }
    }
  }

  def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    debug(s"CLOSING $statementId")
    cleanup(CLOSED)
    cleanupOperationLog()
    session.sparkSession().sparkContext.clearJobGroup()
  }

  def cancel(): Unit = {
    info(s"Cancel '$statement' with $statementId")
    cleanup(CANCELED)
  }

  def addNonNullColumnValue(from: Row, to: ArrayBuffer[Any], ordinal: Int) {
    dataTypes(ordinal) match {
      case StringType =>
        to += from.getString(ordinal)
      case IntegerType =>
        to += from.getInt(ordinal)
      case BooleanType =>
        to += from.getBoolean(ordinal)
      case DoubleType =>
        to += from.getDouble(ordinal)
      case FloatType =>
        to += from.getFloat(ordinal)
      case DecimalType() =>
        to += from.getDecimal(ordinal)
      case LongType =>
        to += from.getLong(ordinal)
      case ByteType =>
        to += from.getByte(ordinal)
      case ShortType =>
        to += from.getShort(ordinal)
      case DateType =>
        to += from.getAs[Date](ordinal)
      case TimestampType =>
        to += from.getAs[Timestamp](ordinal)
      case BinaryType =>
        to += from.getAs[Array[Byte]](ordinal)
      case _: ArrayType | _: StructType | _: MapType =>
        val hiveString = SparkSQLUtils.toHiveString((from.get(ordinal), dataTypes(ordinal)))
        to += hiveString
    }
  }

  def getResultSetSchema: TableSchema = resultSchema

  def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = {
    validateDefaultFetchOrientation(order)
    assertState(FINISHED)
    setHasResultSet(true)
    val resultRowSet: RowSet = RowSetFactory.create(getResultSetSchema, getProtocolVersion)

    if (!iter.hasNext) {
      resultRowSet
    } else {
      // maxRowsL here typically maps to java.sql.Statement.getFetchSize, which is an int
      val maxRows = maxRowsL.toInt
      var curRow = 0
      while (curRow < maxRows && iter.hasNext) {
        val Row = iter.next()
        val row = ArrayBuffer[Any]()
        var curCol = 0
        while (curCol < Row.length) {
          if (Row.isNullAt(curCol)) {
            row += null
          } else {
            addNonNullColumnValue(Row, row, curCol)
          }
          curCol += 1
        }
        resultRowSet.addRow(row.toArray.asInstanceOf[Array[Object]])
        curRow += 1
      }
      resultRowSet
    }
  }

  private[this] def setHasResultSet(hasResultSet: Boolean): Unit = {
    this.hasResultSet = hasResultSet
    opHandle.setHasResultSet(hasResultSet)
  }

  /**
   * Verify if the given fetch orientation is part of the default orientation types.
   *
   * @param orientation
   *
   * @throws HiveSQLException
   */
  @throws[HiveSQLException]
  private[this] def validateDefaultFetchOrientation(orientation: FetchOrientation): Unit = {
    validateFetchOrientation(orientation, DEFAULT_FETCH_ORIENTATION_SET)
  }

  /**
   * Verify if the given fetch orientation is part of the supported orientation types.
   *
   * @param orientation
   * @param supportedOrientations
   *
   * @throws HiveSQLException
   */
  @throws[HiveSQLException]
  private[this] def validateFetchOrientation(
      orientation: FetchOrientation,
      supportedOrientations: EnumSet[FetchOrientation]): Unit = {
    if (!supportedOrientations.contains(orientation)) {
      throw new HiveSQLException(
        "The fetch type " + orientation.toString + " is not supported for this resultset", "HY106")
    }
  }

  private[this] def runInternal(): Unit = {
    setState(PENDING)
    setHasResultSet(true)

    // Runnable impl to call runInternal asynchronously, from a different thread
    val backgroundOperation = new Runnable() {
      override def run(): Unit = {
        try {
          session.ugi.doAs(new PrivilegedExceptionAction[Unit]() {
            registerCurrentOperationLog()
            override def run(): Unit = {
              try {
                execute()
              } catch {
                case e: HiveSQLException => setOperationException(e)
              }
            }
          })
        } catch {
          case e: Exception => setOperationException(new HiveSQLException(e))
        }
      }
    }

    try {
      // This submit blocks if no background threads are available to run this operation
      val backgroundHandle =
        session.getSessionMgr.submitBackgroundOperation(backgroundOperation)
      setBackgroundHandle(backgroundHandle)
    } catch {
      case rejected: RejectedExecutionException =>
        setState(ERROR)
        throw new HiveSQLException("The background threadpool cannot accept" +
          " new task for execution, please retry the operation", rejected)
      case NonFatal(e) =>
        error(s"Error executing query in background", e)
        setState(ERROR)
        throw e
    }
  }

  private[this] def execute(): Unit = {
    try {
      statementId = UUID.randomUUID().toString
      info(s"Running query '$statement' with $statementId")
      setState(RUNNING)
      KyuubiServerMonitor.getListener(session.getUserName).foreach {
        _.onStatementStart(
          statementId,
          session.getSessionHandle.getSessionId.toString,
          statement,
          statementId,
          session.getUserName)
      }
      session.sparkSession().sparkContext.setJobGroup(statementId, statement)
      result = session.sparkSession().sql(statement)
      KyuubiServerMonitor.getListener(session.getUserName).foreach {
        _.onStatementParsed(statementId, result.queryExecution.toString())
      }
      debug(result.queryExecution.toString())
      iter = result.collect().iterator
      dataTypes = result.queryExecution.analyzed.output.map(_.dataType).toArray
      setState(FINISHED)
      KyuubiServerMonitor.getListener(session.getUserName).foreach(_.onStatementFinish(statementId))
    } catch {
      case e: HiveSQLException =>
        if (!isClosedOrCanceled) {
          onStatementError(statementId, e.getMessage, SparkUtils.exceptionString(e))
          throw e
        }
      case e: ParseException =>
        if (!isClosedOrCanceled) {
          onStatementError(
            statementId, e.withCommand(statement).getMessage, SparkUtils.exceptionString(e))
          throw new HiveSQLException(
            e.withCommand(statement).getMessage, "ParseException", 2000, e)
        }
      case e: AnalysisException =>
        if (!isClosedOrCanceled) {
          onStatementError(statementId, e.getMessage, SparkUtils.exceptionString(e))
          throw new HiveSQLException(e.getMessage, "AnalysisException", 2001, e)
        }
      case e: HiveAccessControlException =>
        if (!isClosedOrCanceled) {
          onStatementError(statementId, e.getMessage, SparkUtils.exceptionString(e))
          throw new HiveSQLException(e.getMessage, "HiveAccessControlException", 3000, e)
        }
      case e: Throwable =>
        if (!isClosedOrCanceled) {
          onStatementError(statementId, e.getMessage, SparkUtils.exceptionString(e))
          throw new HiveSQLException(e.toString, "<unknown>", 10000, e)
        }
    } finally {
      if (statementId != null) {
        session.sparkSession().sparkContext.cancelJobGroup(statementId)
      }
    }
  }

  private[this] def onStatementError(id: String, message: String, trace: String): Unit = {
    error(
      s"""
         |Error executing query as ${session.getUserName},
         |$statement
         |Current operation state ${this.state},
         |$trace
       """.stripMargin)
    setState(ERROR)
    KyuubiServerMonitor.getListener(session.getUserName)
      .foreach(_.onStatementError(id, message, trace))
  }

  private[this] def cleanup(state: OperationState) {
    if (this.state != CLOSED) {
      setState(state)
    }
    val backgroundHandle = getBackgroundHandle
    if (backgroundHandle != null) {
      backgroundHandle.cancel(true)
    }
    if (statementId != null) {
      session.sparkSession().sparkContext.cancelJobGroup(statementId)
    }
  }

  def isTimedOut: Boolean = {
    if (operationTimeout <= 0) {
      false
    } else {
      // check only when it's in terminal state
      state.isTerminal && lastAccessTime + operationTimeout <= System.currentTimeMillis()
    }
  }
}

object KyuubiOperation {
  def getTableSchema(structType: StructType): TableSchema = {
    val schema = structType.map { field =>
      val attrTypeString = if (field.dataType == NullType) "void" else field.dataType.catalogString
      new FieldSchema(field.name, attrTypeString, field.getComment().getOrElse(""))
    }
    new TableSchema(schema.asJava)
  }

  val DEFAULT_FETCH_ORIENTATION: FetchOrientation = FetchOrientation.FETCH_NEXT
  val DEFAULT_FETCH_MAX_ROWS = 100

}
