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

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException
import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.spark.KyuubiConf._
import org.apache.spark.KyuubiSparkUtil
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSQLUtils}
import org.apache.spark.sql.catalyst.catalog.{FileResource, FunctionResource, JarResource}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{AddFileCommand, AddJarCommand, CreateFunctionCommand}
import org.apache.spark.sql.types._

import yaooqinn.kyuubi.KyuubiSQLException
import yaooqinn.kyuubi.cli.FetchOrientation
import yaooqinn.kyuubi.schema.{RowSet, RowSetBuilder}
import yaooqinn.kyuubi.session.KyuubiClientSession
import yaooqinn.kyuubi.ui.KyuubiServerMonitor
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiClientOperation(session: KyuubiClientSession, statement: String)
  extends AbstractKyuubiOperation(session, statement) {

  import KyuubiClientOperation._

  private val sparkSession = session.sparkSession
  private val conf = sparkSession.conf

  override protected val operationTimeout =
    KyuubiSparkUtil.timeStringAsMs(conf.get(OPERATION_IDLE_TIMEOUT))

  private var operationLog: OperationLog = _
  private var isOperationLogEnabled: Boolean = false

  private var result: DataFrame = _
  private var iter: Iterator[Row] = _

  private val incrementalCollect: Boolean = conf.get(OPERATION_INCREMENTAL_COLLECT).toBoolean

  override def getOperationLog: OperationLog = operationLog

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
  override def run(): Unit = {
    createOperationLog()
    try {
      runInternal()
    } finally {
      unregisterOperationLog()
    }
  }

  private def cleanupOperationLog(): Unit = {
    if (isOperationLogEnabled) {
      if (operationLog == null) {
        error("Operation [ " + opHandle.getHandleIdentifier + " ] " +
          "logging is enabled, but its OperationLog object cannot be found.")
      } else {
        operationLog.close()
      }
    }
  }

  override def close(): Unit = {
    super.close()
    cleanupOperationLog()
    sparkSession.sparkContext.clearJobGroup()
  }

  override def getResultSetSchema: StructType = if (result == null || result.schema.isEmpty) {
    new StructType().add("Result", "string")
  } else {
    result.schema
  }

  override def getNextRowSet(order: FetchOrientation, rowSetSize: Long): RowSet = {
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

  private def localizeAndAndResource(path: String): Option[String] = try {
    if (isResourceDownloadable(path)) {
      val src = new Path(path)
      val destFileName = src.getName
      val destFile =
        new File(session.getResourcesSessionDir, destFileName).getCanonicalPath
      val fs = src.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
      fs.copyToLocalFile(src, new Path(destFile))
      FileUtil.chmod(destFile, "ugo+rx", true)
      Some(destFile)
    } else {
      None
    }
  } catch {
    case e: Exception => throw new KyuubiSQLException(s"Failed to read external resource: $path", e)
  }

  private[operation] def transform(plan: LogicalPlan): LogicalPlan = plan match {
    case c: CreateFunctionCommand =>
      val resources =
        ReflectUtils.getFieldValue(c, "resources").asInstanceOf[Seq[FunctionResource]]
      resources.foreach {
        case FunctionResource(JarResource, uri) =>
          localizeAndAndResource(uri).map(path => AddJarCommand(path).run(sparkSession))
        case FunctionResource(FileResource, uri) =>
          localizeAndAndResource(uri).map(path => AddFileCommand(path).run(sparkSession))
        case o =>
          throw new KyuubiSQLException(s"Resource Type '${o.resourceType}' is not supported.")
      }
      if (resources.isEmpty) {
        c
      } else {
        ReflectUtils.setFieldValue(c, "resources", Seq.empty[FunctionResource])
        c
      }
    case a: AddJarCommand => localizeAndAndResource(a.path).map(AddJarCommand).getOrElse(a)
    case a: AddFileCommand => localizeAndAndResource(a.path).map(AddFileCommand).getOrElse(a)
    case _ => plan
  }

  override protected def execute(): Unit = {
    try {
      registerCurrentOperationLog()
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
      result = SparkSQLUtils.toDataFrame(sparkSession, transform(parsedPlan))
      KyuubiServerMonitor.getListener(session.getUserName).foreach {
        _.onStatementParsed(statementId, result.queryExecution.toString())
      }

      debug(result.queryExecution.toString())
      iter = if (incrementalCollect) {
        val numParts = result.rdd.getNumPartitions
        info(s"Executing query in incremental mode, running $numParts jobs before optimization")
        val limit = conf.get(OPERATION_INCREMENTAL_RDD_PARTITIONS_LIMIT).toInt
        if (numParts > limit) {
          val partRows = conf.get(OPERATION_INCREMENTAL_PARTITION_ROWS).toInt
          val count = Try { result.persist.count() } match {
            case Success(outputSize) =>
              val num = math.min(math.max(outputSize / partRows, 1), numParts)
              info(s"The total query output is $outputSize and will be coalesced to $num of" +
                s" partitions with $partRows rows on average")
              num
            case _ =>
              warn("Failed to calculate the query output size, do not coalesce")
              numParts
          }
          info(s"Executing query in incremental mode, running $count jobs after optimization")
          result.coalesce(count.toInt).toLocalIterator().asScala
        } else {
          info(s"Executing query in incremental mode, running $numParts jobs without optimization")
          result.toLocalIterator().asScala
        }
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
          val err = KyuubiSparkUtil.exceptionString(e)
          onStatementError(statementId, e.getMessage, err)
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

  override protected def onStatementError(id: String, message: String, trace: String): Unit = {
    super.onStatementError(id, message, trace)
    KyuubiServerMonitor.getListener(session.getUserName)
      .foreach(_.onStatementError(id, message, trace))
  }

  override protected def cleanup(state: OperationState) {
    super.cleanup(state)
    if (statementId != null) {
      sparkSession.sparkContext.cancelJobGroup(statementId)
    }
  }
}

object KyuubiClientOperation {

  def isResourceDownloadable(resource: String): Boolean = {
    val scheme = new Path(resource).toUri.getScheme
    StringUtils.equalsIgnoreCase(scheme, "hdfs")
  }
}