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
import java.security.PrivilegedExceptionAction
import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.HashSet

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ui.ThriftServerTab

import yaooqinn.kyuubi.Logging
import yaooqinn.kyuubi.monitor.{ThriftServerListener, ThriftServerMonitor}
import yaooqinn.kyuubi.operation.KyuubiOperationManager
import yaooqinn.kyuubi.utils.ReflectUtils

class KyuubiSession(
    protocol: TProtocolVersion,
    username: String,
    password: String,
    conf: SparkConf,
    ipAddress: String,
    withImpersonation: Boolean,
    sessionManager: KyuubiSessionManager,
    operationManager: KyuubiOperationManager) extends Logging {

  private[this] var _sparkSession: SparkSession = _
  private[this] val sessionHandle: SessionHandle = new SessionHandle(protocol)
  private[this] val opHandleSet = new HashSet[OperationHandle]
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

  def sparkSession(): SparkSession = this._sparkSession

  private[this] def getOrCreateSparkSession(): Unit = synchronized {
    val userName = sessionUGI.getShortUserName
    var checkRound = math.max(conf.getInt("spark.yarn.report.times.on.start", 60), 15)
    val interval = conf.getTimeAsMs("spark.yarn.report.interval", "1s")
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

  private[this] def createSparkSession(): Unit = {
    val userName = sessionUGI.getShortUserName
    info(s"------ Create new SparkSession for $userName -------")
    conf.setAppName(s"SparkThriftServer[$userName]")
    try {
      sessionUGI.doAs(new PrivilegedExceptionAction[Unit] {
        override def run() = {
          val sc = new SparkContext(conf)
          _sparkSession = ReflectUtils.instantiateClass(classOf[SparkSession].getName,
            Seq(classOf[SparkContext]), Seq(sc)).asInstanceOf[SparkSession]
        }
      })

      sessionManager.setSparkSession(userName, _sparkSession)
      // set sc fully constructed immediately
      sessionManager.setSCFullyConstructed(userName)
      ThriftServerMonitor.setListener(userName, new ThriftServerListener(conf))
      _sparkSession.sparkContext.addSparkListener(ThriftServerMonitor.getListener(userName))
      val uiTab = new ThriftServerTab(userName, _sparkSession.sparkContext)
      ThriftServerMonitor.addUITab(_sparkSession.sparkContext.sparkUser, uiTab)
    } catch {
      case e: Exception =>
        val hiveSQLException =
          new HiveSQLException(
            s"Failed initializing SparkSession for user[$userName]" + e.getMessage, e)
        throw hiveSQLException
    } finally {
      sessionManager.setSCFullyConstructed(userName)
    }

  }

  def removeSparkSession(): Unit = _sparkSession = null

  def getSessionUgi: UserGroupInformation = this.sessionUGI

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
  private[this] def executeStatementInternal(
      statement: String, confOverlay: JMap[String, String], runAsync: Boolean) = {
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
   * @param confOverlay overlay configuration
   * @return
   */
  @throws[HiveSQLException]
  def executeStatement(
      statement: String, confOverlay: JMap[String, String]): OperationHandle = {
    executeStatementInternal(statement, confOverlay, runAsync = false)
  }

  /**
   * execute operation handler
   *
   * @param statement sql statement
   * @param confOverlay overlay configuration
   * @return
   */
  @throws[HiveSQLException]
  def executeStatementAsync(
      statement: String, confOverlay: JMap[String, String]): OperationHandle = {
    executeStatementInternal(statement, confOverlay, runAsync = true)
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
      removeSparkSession()
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
  def getOperationLogSessionDir: File = sessionLogDir

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

  def getUserName: String = username

  def getSessionManager: KyuubiSessionManager = sessionManager

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
}
