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

package yaooqinn.kyuubi.session

import java.io.{File, IOException}
import java.lang.reflect.UndeclaredThrowableException
import java.security.PrivilegedExceptionAction
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.collection.mutable.{HashSet => MHSet}
import scala.concurrent.{Await, Promise, TimeoutException}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal
import scala.util.matching.Regex

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.{SparkConf, SparkContext, SparkUtils}
import org.apache.spark.KyuubiConf._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.types.StructType
import org.apache.spark.ui.KyuubiServerTab

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.auth.KyuubiAuthFactory
import yaooqinn.kyuubi.cli._
import yaooqinn.kyuubi.operation.{KyuubiOperation, OperationHandle, OperationManager}
import yaooqinn.kyuubi.schema.RowSet
import yaooqinn.kyuubi.ui.{KyuubiServerListener, KyuubiServerMonitor}
import yaooqinn.kyuubi.utils.ReflectUtils

/**
 * An Execution Session with [[SparkSession]] instance inside, which shares [[SparkContext]]
 * with other sessions create by the same user.
 *
 * One user, one [[SparkContext]]
 * One user, multi [[KyuubiSession]]s
 *
 * One [[KyuubiSession]], one [[SparkSession]]
 * One [[SparkContext]], multi [[SparkSession]]s
 *
 */
private[kyuubi] class KyuubiSession(
    protocol: TProtocolVersion,
    username: String,
    password: String,
    conf: SparkConf,
    ipAddress: String,
    withImpersonation: Boolean,
    sessionManager: SessionManager,
    operationManager: OperationManager) extends Logging {

  import KyuubiSession._

  private[this] var _sparkSession: SparkSession = _
  private[this] val promisedSparkContext = Promise[SparkContext]()
  private[this] val sessionHandle: SessionHandle = new SessionHandle(protocol)
  private[this] val opHandleSet = new MHSet[OperationHandle]
  private[this] var _isOperationLogEnabled = false
  private[this] var sessionLogDir: File = _
  @volatile private[this] var lastAccessTime: Long = System.currentTimeMillis()
  private[this] var lastIdleTime = 0L
  private[this] var initialDatabase: Option[String] = None

  private[this] val sessionUGI: UserGroupInformation = {
    val currentUser = UserGroupInformation.getCurrentUser
    if (withImpersonation) {
      if (UserGroupInformation.isSecurityEnabled) {
        if (conf.contains(SparkUtils.PRINCIPAL) && conf.contains(SparkUtils.KEYTAB)) {
          // If principal and keytab are configured, do re-login in case of token expiry.
          // Do not check keytab file existing as spark-submit has it done
          currentUser.reloginFromKeytab()
        }
        UserGroupInformation.createProxyUser(username, currentUser)
      } else {
        UserGroupInformation.createRemoteUser(username)
      }
    } else {
      currentUser
    }
  }

  private[this] def getOrCreateSparkSession(sessionConf: Map[String, String]): Unit = synchronized {
    var checkRound = math.max(conf.get(BACKEND_SESSION_WAIT_OTHER_TIMES.key).toInt, 15)
    val interval = conf.getTimeAsMs(BACKEND_SESSION_WAIT_OTHER_INTERVAL.key)
    // if user's sc is being constructed by another
    while (sessionManager.isSCPartiallyConstructed(getUserName)) {
      wait(interval)
      checkRound -= 1
      if (checkRound <= 0) {
        throw new KyuubiSQLException(s"A partially constructed SparkContext for [$getUserName] " +
          s"has last more than ${checkRound * interval} seconds")
      }
      info(s"A partially constructed SparkContext for [$getUserName], $checkRound times countdown.")
    }

    sessionManager.getSparkSession(getUserName) match {
      case Some((ss, times)) if !ss.sparkContext.isStopped =>
        info(s"SparkSession for [$getUserName] is reused " + times.incrementAndGet() + "times")
        _sparkSession = ss.newSession()
        configureSparkSession(sessionConf)
      case _ =>
        sessionManager.setSCPartiallyConstructed(getUserName)
        notifyAll()
        createSparkSession(sessionConf)
    }
  }

  private[this] def newContext(): Thread = {
    new Thread(s"Start-SparkContext-$getUserName") {
      override def run(): Unit = {
        try {
          promisedSparkContext.trySuccess(new SparkContext(conf))
        } catch {
          case NonFatal(e) => throw e
        }
      }
    }
  }

  private[this] def stopContext(): Unit = {
    promisedSparkContext.future.map { sc =>
      warn(s"Error occurred during initializing SparkContext for $getUserName, stopping")
      sc.stop
      System.setProperty("SPARK_YARN_MODE", "true")
    }
  }

  private[this] def createSparkSession(sessionConf: Map[String, String]): Unit = {
    info(s"--------- Create new SparkSession for $getUserName ----------")
    val appName = s"KyuubiSession[$getUserName]@" + conf.get(FRONTEND_BIND_HOST.key)
    conf.setAppName(appName)
    configureSparkConf(sessionConf)
    val totalWaitTime: Long = conf.getTimeAsSeconds(BACKEND_SESSTION_INIT_TIMEOUT.key)
    try {
      sessionUGI.doAs(new PrivilegedExceptionAction[Unit] {
        override def run(): Unit = {
          newContext().start()
          val context =
            Await.result(promisedSparkContext.future, Duration(totalWaitTime, TimeUnit.SECONDS))
          _sparkSession = ReflectUtils.newInstance(
            classOf[SparkSession].getName,
            Seq(classOf[SparkContext]),
            Seq(context)).asInstanceOf[SparkSession]
        }
      })
      sessionManager.setSparkSession(getUserName, _sparkSession)
    } catch {
      case ute: UndeclaredThrowableException =>
        ute.getCause match {
          case te: TimeoutException =>
            stopContext()
            throw new KyuubiSQLException(
              s"Get SparkSession for [$getUserName] failed: " + te, "08S01", 1001, te)
          case _ =>
            stopContext()
            throw new KyuubiSQLException(ute.toString, "08S01", ute.getCause)
        }
      case e: Exception =>
        stopContext()
        throw new KyuubiSQLException(
          s"Get SparkSession for [$getUserName] failed: " + e, "08S01", e)
    } finally {
      sessionManager.setSCFullyConstructed(getUserName)
      newContext().join()
    }

    KyuubiServerMonitor.setListener(getUserName, new KyuubiServerListener(conf))
    KyuubiServerMonitor.getListener(getUserName)
      .foreach(_sparkSession.sparkContext.addSparkListener)
    val uiTab = new KyuubiServerTab(getUserName, _sparkSession.sparkContext)
    KyuubiServerMonitor.addUITab(_sparkSession.sparkContext.sparkUser, uiTab)
  }

  private[this] def acquire(userAccess: Boolean): Unit = {
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis
    }
  }

  private[this] def release(userAccess: Boolean): Unit = {
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis
    }
    if (opHandleSet.isEmpty) {
      lastIdleTime = System.currentTimeMillis
    } else {
      lastIdleTime = 0
    }
  }

  @throws[KyuubiSQLException]
  private[this] def executeStatementInternal(statement: String) = {
    acquire(true)
    val operation =
      operationManager.newExecuteStatementOperation(this, statement)
    val opHandle = operation.getHandle
    try {
      operation.run()
      opHandleSet.add(opHandle)
      opHandle
    } catch {
      case e: KyuubiSQLException =>
        operationManager.closeOperation(opHandle)
        throw e
    } finally {
      release(true)
    }
  }

  private[this] def cleanupSessionLogDir(): Unit = {
    if (_isOperationLogEnabled) {
      try {
        FileUtils.forceDelete(sessionLogDir)
      } catch {
        case e: Exception =>
          error("Failed to cleanup session log dir: " + sessionLogDir, e)
      }
    }
  }

  /**
   * Setting configuration from connection strings before SparkConext init.
   * @param sessionConf configurations for user connection string
   */
  private[this] def configureSparkConf(sessionConf: Map[String, String]): Unit = {
    for ((key, value) <- sessionConf) {
      key match {
        case HIVE_VAR_PREFIX(DEPRECATED_QUEUE) => conf.set(QUEUE, value)
        case HIVE_VAR_PREFIX(k) =>
          if (k.startsWith(SPARK_PREFIX)) {
            conf.set(k, value)
          } else {
            conf.set(SPARK_HADOOP_PREFIX + k, value)
          }
        case "use:database" => initialDatabase = Some("use " + value)
        case _ =>
      }
    }

    // proxy user does not have rights to get token as realuser
    conf.remove(SparkUtils.KEYTAB)
    conf.remove(SparkUtils.PRINCIPAL)
  }

  /**
   * Setting configuration from connection strings for existing SparkSession
   * @param sessionConf configurations for user connection string
   */
  private[this] def configureSparkSession(sessionConf: Map[String, String]): Unit = {
    for ((key, value) <- sessionConf) {
      key match {
        case HIVE_VAR_PREFIX(k) =>
          if (k.startsWith(SPARK_PREFIX)) {
            _sparkSession.conf.set(k, value)
          } else {
            _sparkSession.conf.set(SPARK_HADOOP_PREFIX + k, value)
          }
        case "use:database" => initialDatabase = Some("use " + value)
        case _ =>
      }
    }
  }

  def sparkSession: SparkSession = this._sparkSession

  def ugi: UserGroupInformation = this.sessionUGI

  @throws[KyuubiSQLException]
  def open(sessionConf: Map[String, String]): Unit = {
    try {
      getOrCreateSparkSession(sessionConf)
      initialDatabase.foreach { db =>
        sessionUGI.doAs(new PrivilegedExceptionAction[Unit] {
          override def run(): Unit = sparkSession.sql(db)
        })
      }
    } catch {
      case ute: UndeclaredThrowableException => ute.getCause match {
        case e: HiveAccessControlException =>
          throw new KyuubiSQLException(e.getMessage, "08S01", e.getCause)
        case e: NoSuchDatabaseException =>
          throw new KyuubiSQLException(e.getMessage, "08S01", e.getCause)
        case e: KyuubiSQLException => throw e
      }
    }
    lastAccessTime = System.currentTimeMillis
    lastIdleTime = lastAccessTime
  }

  def getInfo(getInfoType: GetInfoType): GetInfoValue = {
    acquire(true)
    try {
      getInfoType match {
        case GetInfoType.SERVER_NAME => new GetInfoValue("Kyuubi Server")
        case GetInfoType.DBMS_NAME => new GetInfoValue("Spark SQL")
        case GetInfoType.DBMS_VERSION => new GetInfoValue(this._sparkSession.version)
        case _ =>
          throw new KyuubiSQLException("Unrecognized GetInfoType value: " + getInfoType.toString)
      }
    } finally {
      release(true)
    }
  }

  /**
   * execute operation handler
   *
   * @param statement sql statement
   * @return
   */
  @throws[KyuubiSQLException]
  def executeStatement(statement: String): OperationHandle = {
    executeStatementInternal(statement)
  }

  /**
   * execute operation handler
   *
   * @param statement sql statement
   * @return
   */
  @throws[KyuubiSQLException]
  def executeStatementAsync(statement: String): OperationHandle = {
    executeStatementInternal(statement)
  }

  /**
   * close the session
   */
  @throws[KyuubiSQLException]
  def close(): Unit = {
    acquire(true)
    try {
      // Iterate through the opHandles and close their operations
      opHandleSet.foreach(closeOperation)
      opHandleSet.clear()
      // Cleanup session log directory.
      cleanupSessionLogDir()
      _sparkSession = null
    } finally {
      release(true)
      try {
        FileSystem.closeAllForUGI(sessionUGI)
      } catch {
        case ioe: IOException =>
          throw new KyuubiSQLException("Could not clean up file-system handles for UGI: "
            + sessionUGI, ioe)
      }
    }
  }

  def cancelOperation(opHandle: OperationHandle): Unit = {
    acquire(true)
    try {
      operationManager.cancelOperation(opHandle)
    } finally {
      release(true)
    }
  }

  def closeOperation(opHandle: OperationHandle): Unit = {
    acquire(true)
    try {
      operationManager.closeOperation(opHandle)
      opHandleSet.remove(opHandle)
    } finally {
      release(true)
    }
  }

  def getResultSetMetadata(opHandle: OperationHandle): StructType = {
    acquire(true)
    try {
      operationManager.getResultSetSchema(opHandle)
    } finally {
      release(true)
    }
  }

  @throws[KyuubiSQLException]
  def fetchResults(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long,
      fetchType: FetchType): RowSet = {
    acquire(true)
    try {
      fetchType match {
        case FetchType.QUERY_OUTPUT =>
          operationManager.getOperationNextRowSet(opHandle, orientation, maxRows)
        case _ =>
          operationManager.getOperationLogRowSet(opHandle, orientation, maxRows)
      }
    } finally {
      release(true)
    }
  }

  @throws[KyuubiSQLException]
  def getDelegationToken(
      authFactory: KyuubiAuthFactory,
      owner: String,
      renewer: String): String = {
    authFactory.getDelegationToken(owner, renewer)
  }

  @throws[KyuubiSQLException]
  def cancelDelegationToken(authFactory: KyuubiAuthFactory, tokenStr: String): Unit = {
    authFactory.cancelDelegationToken(tokenStr)
  }

  @throws[KyuubiSQLException]
  def renewDelegationToken(authFactory: KyuubiAuthFactory, tokenStr: String): Unit = {
    authFactory.renewDelegationToken(tokenStr)
  }

  def closeExpiredOperations: Unit = {
    if (opHandleSet.nonEmpty) {
      closeTimedOutOperations(operationManager.removeExpiredOperations(opHandleSet.toSeq))
    }
  }

  private[this] def closeTimedOutOperations(operations: Seq[KyuubiOperation]): Unit = {
    acquire(false)
    try {
      operations.foreach { op =>
        opHandleSet.remove(op.getHandle)
        try {
          op.close()
        } catch {
          case e: Exception =>
            warn("Exception is thrown closing timed-out operation " + op.getHandle, e)
        }
      }
    } finally {
      release(false)
    }
  }

  def getNoOperationTime: Long = {
    if (lastIdleTime > 0) {
      System.currentTimeMillis - lastIdleTime
    } else {
      0
    }
  }

  def getProtocolVersion: TProtocolVersion = sessionHandle.getProtocolVersion

  /**
   * Check whether operation logging is enabled and session dir is created successfully
   */
  def isOperationLogEnabled: Boolean = _isOperationLogEnabled

  /**
   * Get the session dir, which is the parent dir of operation logs
   *
   * @return a file representing the parent directory of operation logs
   */
  def getSessionLogDir: File = sessionLogDir

  /**
   * Set the session dir, which is the parent dir of operation logs
   *
   * @param operationLogRootDir the parent dir of the session dir
   */
  def setOperationLogSessionDir(operationLogRootDir: File): Unit = {
    sessionLogDir = new File(operationLogRootDir,
      username + File.separator + sessionHandle.getHandleIdentifier.toString)
    _isOperationLogEnabled = true
    if (!sessionLogDir.exists) {
      if (!sessionLogDir.mkdirs) {
        warn("Unable to create operation log session directory: "
          + sessionLogDir.getAbsolutePath)
        _isOperationLogEnabled = false
      }
    }
    if (_isOperationLogEnabled) {
      info("Operation log session directory is created: " + sessionLogDir.getAbsolutePath)
    }
  }

  def getSessionHandle: SessionHandle = sessionHandle

  def getPassword: String = password

  def getIpAddress: String = ipAddress

  def getLastAccessTime: Long = lastAccessTime

  def getUserName: String = sessionUGI.getShortUserName

  def getSessionMgr: SessionManager = sessionManager
}

object KyuubiSession {
  val HIVE_VAR_PREFIX: Regex = """set:hivevar:([^=]+)""".r
  val USE_DB: Regex = """use:([^=]+)""".r

  val SPARK_APP_ID: String = "spark.app.id"
  val DEPRECATED_QUEUE = "mapred.job.queue.name"
  val QUEUE = "spark.yarn.queue"

  val SPARK_PREFIX = "spark."
  val SPARK_HADOOP_PREFIX = "spark.hadoop."
}
