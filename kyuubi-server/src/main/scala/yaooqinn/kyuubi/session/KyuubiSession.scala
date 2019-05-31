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

import scala.collection.mutable.{HashSet => MHSet}

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.{KyuubiSparkUtil, SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.auth.KyuubiAuthFactory
import yaooqinn.kyuubi.cli._
import yaooqinn.kyuubi.operation.{KyuubiOperation, OperationHandle, OperationManager}
import yaooqinn.kyuubi.schema.RowSet
import yaooqinn.kyuubi.session.security.TokenCollector
import yaooqinn.kyuubi.spark.SparkSessionWithUGI
import yaooqinn.kyuubi.utils.KyuubiHadoopUtil

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

  private val sessionHandle: SessionHandle = new SessionHandle(protocol)
  private val opHandleSet = new MHSet[OperationHandle]
  private var _isOperationLogEnabled = false
  private var sessionLogDir: File = _
  private var sessionResourcesDir: File = _
  @volatile private var lastAccessTime: Long = System.currentTimeMillis()
  private var lastIdleTime = 0L

  private val sessionUGI: UserGroupInformation = {
    val currentUser = UserGroupInformation.getLoginUser
    if (withImpersonation) {
      if (UserGroupInformation.isSecurityEnabled) {
        if (conf.contains(KyuubiSparkUtil.PRINCIPAL) && conf.contains(KyuubiSparkUtil.KEYTAB)) {
          // If principal and keytab are configured, do re-login in case of token expiry.
          // Do not check keytab file existing as spark-submit has it done
          currentUser.reloginFromKeytab()
        }
        val user = UserGroupInformation.createProxyUser(username, currentUser)
        KyuubiHadoopUtil.doAs(user)(TokenCollector.obtainTokenIfRequired(conf))
        user
      } else {
        UserGroupInformation.createRemoteUser(username)
      }
    } else {
      currentUser
    }
  }

  private val sparkSessionWithUGI =
    new SparkSessionWithUGI(sessionUGI, conf, sessionManager.getCacheMgr)

  private def acquire(userAccess: Boolean): Unit = {
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis
    }
  }

  private def release(userAccess: Boolean): Unit = {
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
  private def executeStatementInternal(statement: String): OperationHandle = {
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

  private def cleanupSessionLogDir(): Unit = {
    if (_isOperationLogEnabled) {
      try {
        FileUtils.forceDelete(sessionLogDir)
      } catch {
        case e: Exception =>
          error("Failed to cleanup session log dir: " + sessionLogDir, e)
      }
    }
  }

  def sparkSession: SparkSession = this.sparkSessionWithUGI.sparkSession

  def ugi: UserGroupInformation = this.sessionUGI

  @throws[KyuubiSQLException]
  def open(sessionConf: Map[String, String]): Unit = {
    sparkSessionWithUGI.init(sessionConf)
    lastAccessTime = System.currentTimeMillis
    lastIdleTime = lastAccessTime
  }

  def getInfo(getInfoType: GetInfoType): GetInfoValue = {
    acquire(true)
    try {
      getInfoType match {
        case GetInfoType.SERVER_NAME => new GetInfoValue("Kyuubi Server")
        case GetInfoType.DBMS_NAME => new GetInfoValue("Spark SQL")
        case GetInfoType.DBMS_VERSION =>
          new GetInfoValue(this.sparkSessionWithUGI.sparkSession.version)
        case _ =>
          throw new KyuubiSQLException("Unrecognized GetInfoType value " + getInfoType.toString)
      }
    } finally {
      release(true)
    }
  }

  def getTableTypes: OperationHandle = {
    acquire(true)
    val operation = operationManager.newGetTableTypesOperation(this)
    val handle = operation.getHandle
    try {
      operation.run()
      opHandleSet.add(handle)
      handle
    } catch {
      case e: KyuubiSQLException =>
        operationManager.closeOperation(handle)
        throw e
    } finally {
      release(true)
    }
  }

  def getTypeInfo: OperationHandle = {
    acquire(true)
    val operation = operationManager.newGetTypeInfoOperation(this)
    val handle = operation.getHandle
    try {
      operation.run()
      opHandleSet.add(handle)
      handle
    } catch {
      case e: KyuubiSQLException =>
        operationManager.closeOperation(handle)
        throw e
    } finally {
      release(true)
    }
  }

  def getCatalogs: OperationHandle = {
    acquire(true)
    val operation = operationManager.newGetCatalogsOperation(this)
    val handle = operation.getHandle
    try {
      operation.run()
      opHandleSet.add(handle)
      handle
    } catch {
      case e: KyuubiSQLException =>
        operationManager.closeOperation(handle)
        throw e
    } finally {
      release(true)
    }
  }

  def getSchemas(catalogName: String, schemaName: String): OperationHandle = {
    acquire(true)
    val operation =
      operationManager.newGetSchemasOperation(this, catalogName, schemaName)
    val handle = operation.getHandle
    try {
      operation.run()
      opHandleSet.add(handle)
      handle
    } catch {
      case e: KyuubiSQLException =>
        operationManager.closeOperation(handle)
        throw e
    } finally {
      release(true)
    }
  }

  def getTables(
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: Seq[String]): OperationHandle = {
    acquire(true)
    val operation =
      operationManager.newGetTablesOperation(this, catalogName, schemaName, tableName, tableTypes)
    val handle = operation.getHandle
    try {
      operation.run()
      opHandleSet.add(handle)
      handle
    } catch {
      case e: KyuubiSQLException =>
        operationManager.closeOperation(handle)
        throw e
    } finally {
      release(true)
    }
  }

  def getFunctions(
      catalogName: String,
      schemaName: String,
      functionName: String): OperationHandle = {
    acquire(true)
    val operation =
      operationManager.newGetFunctionsOperation(this, catalogName, schemaName, functionName)
    val handle = operation.getHandle
    try {
      operation.run()
      opHandleSet.add(handle)
      handle
    } catch {
      case e: KyuubiSQLException =>
        operationManager.closeOperation(handle)
        throw e
    } finally {
      release(true)
    }
  }

  def getColumns(
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): OperationHandle = {
    acquire(true)
    val operation =
      operationManager.newGetColumnsOperation(this, catalogName, schemaName, tableName, columnName)
    val handle = operation.getHandle
    try {
      operation.run()
      opHandleSet.add(handle)
      handle
    } catch {
      case e: KyuubiSQLException =>
        operationManager.closeOperation(handle)
        throw e
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
    } finally {
      release(true)
      try {
        FileSystem.closeAllForUGI(sessionUGI)
      } catch {
        case ioe: IOException =>
          throw new KyuubiSQLException("Could not clean up file-system handles for UGI "
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

  private def closeTimedOutOperations(operations: Seq[KyuubiOperation]): Unit = {
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
   * Get the session log dir, which is the parent dir of operation logs
   *
   * @return a file representing the parent directory of operation logs
   */
  def getSessionLogDir: File = sessionLogDir

  /**
   * Set the session log dir, which is the parent dir of operation logs
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

  /**
   * Get the session resource dir, which is the parent dir of operation logs
   *
   * @return a file representing the parent directory of operation logs
   */
  def getResourcesSessionDir: File = sessionResourcesDir

  /**
   * Set the session log dir, which is the parent dir of operation logs
   *
   * @param resourcesRootDir the parent dir of the session dir
   */
  def setResourcesSessionDir(resourcesRootDir: File): Unit = {
    sessionResourcesDir = new File(resourcesRootDir,
      username + File.separator + sessionHandle.getHandleIdentifier.toString + "_resources")
    if (sessionResourcesDir.exists() && !sessionResourcesDir.isDirectory) {
      throw new RuntimeException("The resources directory exists but is not a directory: " +
        sessionResourcesDir)
    }

    if (!sessionResourcesDir.exists() && !sessionResourcesDir.mkdirs()) {
      throw new RuntimeException("Couldn't create session resources directory " +
        sessionResourcesDir)
    }
  }

  def getSessionHandle: SessionHandle = sessionHandle

  def getPassword: String = password

  def getIpAddress: String = ipAddress

  def getLastAccessTime: Long = lastAccessTime

  def getUserName: String = sessionUGI.getShortUserName

  def getSessionMgr: SessionManager = sessionManager

  def getConf: SparkConf = conf
}
