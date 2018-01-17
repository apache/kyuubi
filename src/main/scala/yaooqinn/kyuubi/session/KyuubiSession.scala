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
import java.util.{Map => JMap, UUID}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashSet => MHSet}
import scala.concurrent.{Await, Promise, TimeoutException}
import scala.concurrent.duration.Duration

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ui.KyuubiServerTab
import org.apache.spark.KyuubiConf._

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.operation.OperationManager
import yaooqinn.kyuubi.ui.{KyuubiServerListener, KyuubiServerMonitor}
import yaooqinn.kyuubi.utils.{HadoopUtils, ReflectUtils}

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
  private[this] var lastAccessTime = 0L
  private[this] var lastIdleTime = 0L
  private[this] var initialDatabase: String = "use default"

  private[this] val sessionUGI: UserGroupInformation = {
    val currentUser = UserGroupInformation.getLoginUser
    if (withImpersonation) {
      if (UserGroupInformation.isSecurityEnabled) {
          UserGroupInformation.createProxyUser(username, currentUser)
      } else {
        UserGroupInformation.createRemoteUser(username)
      }
    } else {
      currentUser
    }
  }

  private[this] def getOrCreateSparkSession(): Unit = synchronized {
    val userName = sessionUGI.getShortUserName
    var checkRound = math.max(conf.get(KYUUBI_REPORT_TIMES_ON_START.key).toInt, 15)
    val interval = conf.getTimeAsMs(REPORT_INTERVAL, "1s")
    // if user's sc is being constructed by another
    while (sessionManager.isSCPartiallyConstructed(userName)) {
      wait(interval)
      checkRound -= 1
      if (checkRound <= 0) {
        throw new HiveSQLException(s"A partially constructed SparkContext for [$userName] " +
          s"has last more than ${checkRound * interval} seconds")
      }
    }

    sessionManager.getExistSparkSession(userName) match {
      case Some((ss, times)) if !ss.sparkContext.isStopped =>
        info(s"SparkSession for [$userName] is reused " + times.incrementAndGet() + "times")
        _sparkSession = ss.newSession()
      case _ =>
        sessionManager.setSCPartiallyConstructed(userName)
        notifyAll()
        createSparkSession()
    }
  }

  private[this] def startSparkContextInNewThread(): Unit = {
    new Thread("name") {
      override def run(): Unit = {
        promisedSparkContext.trySuccess(new SparkContext(conf))
      }
    }.start()
  }

  private[this] def createSparkSession(): Unit = {
    val user = sessionUGI.getShortUserName
    info(s"------ Create new SparkSession for $user -------")
    val appName = s"KyuubiSession[$user]:${UUID.randomUUID().toString}"
    conf.setAppName(appName)
    val totalWaitTime: Long = conf.getTimeAsSeconds(AM_MAX_WAIT_TIME, "60s")
    try {
      sessionUGI.doAs(new PrivilegedExceptionAction[Unit] {
        override def run(): Unit = {
          startSparkContextInNewThread()
          val context =
            Await.result(promisedSparkContext.future, Duration(totalWaitTime, TimeUnit.SECONDS))
          _sparkSession = ReflectUtils.newInstance(
            classOf[SparkSession].getName,
            Seq(classOf[SparkContext]),
            Seq(context)).asInstanceOf[SparkSession]
        }
      })

      sessionManager.setSparkSession(user, _sparkSession)
      // set sc fully constructed immediately
      sessionManager.setSCFullyConstructed(user)
      KyuubiServerMonitor.setListener(user, new KyuubiServerListener(conf))
      _sparkSession.sparkContext.addSparkListener(KyuubiServerMonitor.getListener(user))
      val uiTab = new KyuubiServerTab(user, _sparkSession.sparkContext)
      KyuubiServerMonitor.addUITab(_sparkSession.sparkContext.sparkUser, uiTab)
    } catch {
      case ute: UndeclaredThrowableException =>
        ute.getCause match {
          case te: TimeoutException =>
            sessionUGI.doAs(new PrivilegedExceptionAction[Unit] {
              override def run(): Unit = HadoopUtils.killYarnAppByName(appName)
            })
            throw new HiveSQLException(s"Get SparkSession for [$user] failed: " + te, te)
          case _ =>
            throw new HiveSQLException(ute.toString, ute.getCause)
        }
      case e: Exception =>
        throw new HiveSQLException(s"Get SparkSession for [$user] failed: " + e, e)
    } finally {
      sessionManager.setSCFullyConstructed(user)
    }
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

  @throws[HiveSQLException]
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
      case e: HiveSQLException =>
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

  private[this] def configureSession(sessionConfMap: JMap[String, String]): Unit = {
    for (entry <- sessionConfMap.entrySet.asScala) {
      val key = entry.getKey
      if (key == "set:hivevar:mapred.job.queue.name") {
        conf.set("spark.yarn.queue", entry.getValue)
      } else if (key.startsWith("set:hivevar:")) {
        val realKey = key.substring(12)
        if (realKey.startsWith("spark.")) {
          conf.set(realKey, entry.getValue)
        } else {
          conf.set("spark.hadoop." + realKey, entry.getValue)
        }
      } else if (key.startsWith("use:")) {
        // deal with database later after sparkSession initialized
        initialDatabase = "use " + entry.getValue
      } else {

      }
    }
  }

  def sparkSession(): SparkSession = this._sparkSession

  def ugi: UserGroupInformation = this.sessionUGI

  def open(sessionConfMap: JMap[String, String]): Unit = {
    configureSession(sessionConfMap)
    getOrCreateSparkSession()
    assert(_sparkSession != null)

    sessionUGI.doAs(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = {
        _sparkSession.sql(initialDatabase)
      }
    })
    lastAccessTime = System.currentTimeMillis
    lastIdleTime = lastAccessTime
  }

  def getInfo(getInfoType: GetInfoType): GetInfoValue = {
    acquire(true)
    try {
      getInfoType match {
        case GetInfoType.CLI_SERVER_NAME => new GetInfoValue("Spark SQL")
        case GetInfoType.CLI_DBMS_NAME => new GetInfoValue("Spark SQL")
        case GetInfoType.CLI_DBMS_VER => new GetInfoValue(this._sparkSession.version)
        case _ =>
          throw new HiveSQLException("Unrecognized GetInfoType value: " + getInfoType.toString)
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
  @throws[HiveSQLException]
  def executeStatement(statement: String): OperationHandle = {
    executeStatementInternal(statement)
  }

  /**
   * execute operation handler
   *
   * @param statement sql statement
   * @return
   */
  @throws[HiveSQLException]
  def executeStatementAsync(statement: String): OperationHandle = {
    executeStatementInternal(statement)
  }

  /**
   * close the session
   */
  @throws[HiveSQLException]
  def close(): Unit = {
    acquire(true)
    try {
      // Iterate through the opHandles and close their operations
      for (opHandle <- opHandleSet) {
        operationManager.closeOperation(opHandle)
      }
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
          throw new HiveSQLException("Could not clean up file-system handles for UGI: "
            + sessionUGI, ioe)
      }
    }
  }

  def cancelOperation(opHandle: OperationHandle): Unit = {
    acquire(true)
    try {
      operationManager.cancelOperation(opHandle)
    }
    finally {
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

  def getResultSetMetadata(opHandle: OperationHandle): TableSchema = {
    acquire(true)
    try {
      operationManager.getOperation(opHandle).getResultSetSchema
    } finally {
      release(true)
    }
  }

  def fetchResults(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long,
      fetchType: FetchType): RowSet = {
    acquire(true)
    try {
      if (fetchType == FetchType.QUERY_OUTPUT) {
        operationManager.getOperationNextRowSet(opHandle, orientation, maxRows)
      } else {
        operationManager.getOperationLogRowSet(opHandle, orientation, maxRows)
      }
    } finally {
      release(true)
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
    sessionLogDir = new File(operationLogRootDir, sessionHandle.getHandleIdentifier.toString)
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
  val SPARK_APP_ID = "spark.app.id"
  val AM_MAX_WAIT_TIME = "spark.yarn.am.waitTime"
  val REPORT_INTERVAL = "spark.yarn.report.interval"
}
