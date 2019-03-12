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
import java.util.UUID
import java.util.concurrent.{Future, RejectedExecutionException}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException
import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.KyuubiConf._
import org.apache.spark.KyuubiSparkUtil
import org.apache.spark.scheduler.cluster.KyuubiSparkExecutorUtils
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSQLUtils}
import org.apache.spark.sql.catalyst.catalog.FunctionResource
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.execution.command.AddJarCommand
import org.apache.spark.sql.types._

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.cli.FetchOrientation
import yaooqinn.kyuubi.schema.{RowSet, RowSetBuilder}
import yaooqinn.kyuubi.session.KyuubiSession
import yaooqinn.kyuubi.ui.KyuubiServerMonitor
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiOperation(session: KyuubiSession, statement: String) extends Logging {

  import KyuubiOperation._

  private var state: OperationState = INITIALIZED
  private val opHandle: OperationHandle =
    new OperationHandle(EXECUTE_STATEMENT, session.getProtocolVersion)

  private val sparkSession = session.sparkSession
  private val conf = sparkSession.conf

  private val operationTimeout =
    KyuubiSparkUtil.timeStringAsMs(conf.get(OPERATION_IDLE_TIMEOUT))
  private var lastAccessTime = System.currentTimeMillis()

  private var hasResultSet: Boolean = false
  private var operationException: KyuubiSQLException = _
  private var backgroundHandle: Future[_] = _
  private var operationLog: OperationLog = _
  private var isOperationLogEnabled: Boolean = false

  private var result: DataFrame = _
  private var iter: Iterator[Row] = _
  private var statementId: String = _

  private val DEFAULT_FETCH_ORIENTATION_SET: Set[FetchOrientation] =
    Set(FetchOrientation.FETCH_NEXT, FetchOrientation.FETCH_FIRST)

  private val incrementalCollect: Boolean = conf.get(OPERATION_INCREMENTAL_COLLECT).toBoolean

  def getBackgroundHandle: Future[_] = backgroundHandle

  def setBackgroundHandle(backgroundHandle: Future[_]): Unit = {
    this.backgroundHandle = backgroundHandle
  }

  def getSession: KyuubiSession = session

  def getHandle: OperationHandle = opHandle

  def getProtocolVersion: TProtocolVersion = opHandle.getProtocolVersion

  def getStatus: OperationStatus = new OperationStatus(state, operationException)

  def getOperationLog: OperationLog = operationLog

  private def setOperationException(opEx: KyuubiSQLException): Unit = {
    this.operationException = opEx
  }

  @throws[KyuubiSQLException]
  private def setState(newState: OperationState): Unit = {
    state.validateTransition(newState)
    this.state = newState
    this.lastAccessTime = System.currentTimeMillis()
  }

  private def checkState(state: OperationState): Boolean = {
    this.state == state
  }
  
  def isClosedOrCanceled: Boolean = {
    checkState(CLOSED) || checkState(CANCELED)
  }

  @throws[KyuubiSQLException]
  private def assertState(state: OperationState): Unit = {
    if (this.state ne state) {
      throw new KyuubiSQLException("Expected state " + state + ", but found " + this.state)
    }
    this.lastAccessTime = System.currentTimeMillis()
  }

  private def createOperationLog(): Unit = {
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

  private def registerCurrentOperationLog(): Unit = {
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

  private def unregisterOperationLog(): Unit = {
    if (isOperationLogEnabled) {
      session.getSessionMgr.getOperationMgr
        .unregisterOperationLog(session.getUserName)
    }
  }

  @throws[KyuubiSQLException]
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
    sparkSession.sparkContext.clearJobGroup()
  }

  def cancel(): Unit = {
    info(s"Cancel '$statement' with $statementId")
    cleanup(CANCELED)
  }

  def getResultSetSchema: StructType = if (result == null || result.schema.isEmpty) {
    new StructType().add("Result", "string")
  } else {
    result.schema
  }

  def getNextRowSet(order: FetchOrientation, rowSetSize: Long): RowSet = {
    validateDefaultFetchOrientation(order)
    assertState(FINISHED)
    setHasResultSet(true)
    val taken = if (order == FetchOrientation.FETCH_FIRST) {
      result.toLocalIterator().asScala.take(rowSetSize.toInt)
    } else {
      iter.take(rowSetSize.toInt)
    }
    RowSetBuilder.create(getResultSetSchema, taken.toSeq, session.getProtocolVersion)
  }

  private def setHasResultSet(hasResultSet: Boolean): Unit = {
    this.hasResultSet = hasResultSet
    opHandle.setHasResultSet(hasResultSet)
  }

  /**
   * Verify if the given fetch orientation is part of the default orientation types.
   */
  @throws[KyuubiSQLException]
  private def validateDefaultFetchOrientation(orientation: FetchOrientation): Unit = {
    validateFetchOrientation(orientation, DEFAULT_FETCH_ORIENTATION_SET)
  }

  /**
   * Verify if the given fetch orientation is part of the supported orientation types.
   */
  @throws[KyuubiSQLException]
  private def validateFetchOrientation(
      orientation: FetchOrientation,
      supportedOrientations: Set[FetchOrientation]): Unit = {
    if (!supportedOrientations.contains(orientation)) {
      throw new KyuubiSQLException(
        "The fetch type " + orientation.toString + " is not supported for this resultset", "HY106")
    }
  }

  private def runInternal(): Unit = {
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
                case e: KyuubiSQLException => setOperationException(e)
              }
            }
          })
        } catch {
          case e: Exception => setOperationException(new KyuubiSQLException(e))
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
        throw new KyuubiSQLException("The background threadpool cannot accept" +
          " new task for execution, please retry the operation", rejected)
      case NonFatal(e) =>
        error(s"Error executing query in background", e)
        setState(ERROR)
        throw e
    }
  }

  private def localizeAndAndResource(path: String): Unit = try {
    if (isResourceDownloadable(path)) {
      val src = new Path(path)
      val destFileName = src.getName
      val destFile =
        new File(session.getResourcesSessionDir, destFileName).getCanonicalPath
      val fs = src.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
      fs.copyToLocalFile(src, new Path(destFile))
      FileUtil.chmod(destFile, "ugo+rx", true)
      AddJarCommand(destFile).run(session.sparkSession)
    }
  } catch {
    case e: Exception => throw new KyuubiSQLException(s"Failed to read external resource: $path", e)
  }

  private def execute(): Unit = {
    try {
      statementId = UUID.randomUUID().toString
      info(s"Running query '$statement' with $statementId")
      setState(RUNNING)

      val classLoader = SparkSQLUtils.getUserJarClassLoader(sparkSession)
      Thread.currentThread().setContextClassLoader(classLoader)

      KyuubiServerMonitor.getListener(session.getUserName).foreach {
        _.onStatementStart(
          statementId,
          session.getSessionHandle.getSessionId.toString,
          statement,
          statementId,
          session.getUserName)
      }
      sparkSession.sparkContext.setJobGroup(statementId, statement)
      KyuubiSparkUtil.setActiveSparkContext(sparkSession.sparkContext)

      val parsedPlan = SparkSQLUtils.parsePlan(sparkSession, statement)
      parsedPlan match {
        case c if c.nodeName == "CreateFunctionCommand" =>
          val resources =
            ReflectUtils.getFieldValue(c, "resources").asInstanceOf[Seq[FunctionResource]]
          resources.foreach { case FunctionResource(_, uri) =>
            localizeAndAndResource(uri)
          }
        case a if a.nodeName == "AddJarCommand" =>
          val path = ReflectUtils.getFieldValue(a, "path").asInstanceOf[String]
          localizeAndAndResource(path)
        case _ =>
      }
      result = SparkSQLUtils.toDataFrame(sparkSession, parsedPlan)
      KyuubiServerMonitor.getListener(session.getUserName).foreach {
        _.onStatementParsed(statementId, result.queryExecution.toString())
      }

      if (conf.get(BACKEND_SESSION_LONG_CACHE).toBoolean &&
        KyuubiSparkUtil.classIsLoadable(conf.get(BACKEND_SESSION_TOKEN_UPDATE_CLASS))) {
        KyuubiSparkExecutorUtils.populateTokens(sparkSession.sparkContext, session.ugi)
      }
      debug(result.queryExecution.toString())
      iter = if (incrementalCollect) {
        info("Executing query in incremental collection mode")
        result.toLocalIterator().asScala
      } else {
        val resultLimit = conf.get(OPERATION_RESULT_LIMIT).toInt
        if (resultLimit >= 0) {
          result.take(resultLimit).toList.toIterator
        } else {
          result.collect().toList.iterator
        }
      }
      setState(FINISHED)
      KyuubiServerMonitor.getListener(session.getUserName).foreach(_.onStatementFinish(statementId))
    } catch {
      case e: KyuubiSQLException =>
        if (!isClosedOrCanceled) {
          onStatementError(statementId, e.getMessage, KyuubiSparkUtil.exceptionString(e))
          throw e
        }
      case e: ParseException =>
        if (!isClosedOrCanceled) {
          val err = KyuubiSparkUtil.exceptionString(e)
          onStatementError(statementId, e.withCommand(statement).getMessage, err)
          throw new KyuubiSQLException(
            e.withCommand(statement).getMessage + err, "ParseException", 2000, e)
        }
      case e: AnalysisException =>
        if (!isClosedOrCanceled) {
          val err = KyuubiSparkUtil.exceptionString(e)
          onStatementError(statementId, e.getMessage, err)
          throw new KyuubiSQLException(err, "AnalysisException", 2001, e)
        }
      case e: HiveAccessControlException =>
        if (!isClosedOrCanceled) {
          val err = KyuubiSparkUtil.exceptionString(e)
          onStatementError(statementId, e.getMessage, err)
          throw new KyuubiSQLException(err, "HiveAccessControlException", 3000, e)
        }
      case e: Throwable =>
        if (!isClosedOrCanceled) {
          val err = KyuubiSparkUtil.exceptionString(e)
          onStatementError(statementId, e.getMessage, err)
          throw new KyuubiSQLException(err, "<unknown>", 10000, e)
        }
    } finally {
      if (statementId != null) {
        sparkSession.sparkContext.cancelJobGroup(statementId)
      }
    }
  }

  private def onStatementError(id: String, message: String, trace: String): Unit = {
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

  private def cleanup(state: OperationState) {
    if (this.state != CLOSED) {
      setState(state)
    }
    val backgroundHandle = getBackgroundHandle
    if (backgroundHandle != null) {
      backgroundHandle.cancel(true)
    }
    if (statementId != null) {
      sparkSession.sparkContext.cancelJobGroup(statementId)
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
  val DEFAULT_FETCH_ORIENTATION: FetchOrientation = FetchOrientation.FETCH_NEXT
  val DEFAULT_FETCH_MAX_ROWS = 100

  def isResourceDownloadable(resource: String): Boolean = {
    val scheme = new Path(resource).toUri.getScheme
    StringUtils.equalsIgnoreCase(scheme, "hdfs")
  }
}
