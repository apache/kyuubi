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

package org.apache.kyuubi.session

import java.io.IOException
import java.nio.file.{Files, Paths}
import java.util.concurrent.{ConcurrentHashMap, Future, ThreadPoolExecutor, TimeUnit}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.apache.kyuubi.{KyuubiSQLException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.operation.OperationManager
import org.apache.kyuubi.service.CompositeService
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion
import org.apache.kyuubi.util.ThreadUtils
import org.apache.kyuubi.util.ThreadUtils.scheduleTolerableRunnableWithFixedDelay

/**
 * The [[SessionManager]] holds the all the connected [[Session]]s, provides us the APIs to
 * open, set, get, close [[Session]]s and cleans idled [[Session]]s with a daemon checker
 * thread.
 *
 * @param name Service Name
 */
abstract class SessionManager(name: String) extends CompositeService(name) {

  @volatile private var shutdown = false

  protected var _operationLogRoot: Option[String] = None

  def operationLogRoot: Option[String] = _operationLogRoot

  private def initOperationLogRootDir(): Unit = {
    try {
      val logRoot =
        if (isServer) {
          conf.get(SERVER_OPERATION_LOG_DIR_ROOT)
        } else {
          conf.get(ENGINE_OPERATION_LOG_DIR_ROOT)
        }
      val logPath = Files.createDirectories(Utils.getAbsolutePathFromWork(logRoot))
      _operationLogRoot = Some(logPath.toString)
    } catch {
      case e: IOException =>
        error(s"Failed to initialize operation log root directory: ${_operationLogRoot}", e)
        _operationLogRoot = None
    }
  }

  @volatile private var _latestLogoutTime: Long = System.currentTimeMillis()
  def latestLogoutTime: Long = _latestLogoutTime

  private val handleToSession = new ConcurrentHashMap[SessionHandle, Session]

  private val timeoutChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(s"$name-timeout-checker")

  protected def isServer: Boolean

  private var execPool: ThreadPoolExecutor = _

  def submitBackgroundOperation(r: Runnable): Future[_] = execPool.submit(r)

  def operationManager: OperationManager

  protected def createSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): Session

  protected def logSessionCountInfo(session: Session, action: String): Unit = {
    info(s"${session.user}'s ${session.getClass.getSimpleName} with" +
      s" ${session.handle}${session.name.map("/" + _).getOrElse("")} is $action," +
      s" current opening sessions $getActiveUserSessionCount")
  }

  def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): SessionHandle = {
    info(s"Opening session for $user@$ipAddress")
    val session = createSession(protocol, user, password, ipAddress, conf)
    val handle = session.handle
    try {
      setSession(handle, session)
      session.open()
      logSessionCountInfo(session, "opened")
      handle
    } catch {
      case e: Exception =>
        try {
          closeSession(handle)
        } catch {
          case t: Throwable =>
            warn(s"Error closing session for $user client ip: $ipAddress", t)
        }
        throw KyuubiSQLException(e)
    }
  }

  def closeSession(sessionHandle: SessionHandle): Unit = {
    val session = handleToSession.remove(sessionHandle)
    if (session == null) {
      throw KyuubiSQLException(s"Invalid $sessionHandle")
    }
    if (!session.isForAliveProbe) {
      _latestLogoutTime = System.currentTimeMillis()
    }
    logSessionCountInfo(session, "closed")
    try {
      session.close()
    } finally {
      deleteOperationLogSessionDir(sessionHandle)
    }
  }

  private def deleteOperationLogSessionDir(sessionHandle: SessionHandle): Unit = {
    _operationLogRoot.foreach(logRoot => {
      val rootPath = Paths.get(logRoot, sessionHandle.identifier.toString)
      try {
        Utils.deleteDirectoryRecursively(rootPath.toFile)
      } catch {
        case e: IOException =>
          error(s"Failed to delete session operation log directory ${rootPath.toString}", e)
      }
    })
  }

  def getSessionOption(sessionHandle: SessionHandle): Option[Session] = {
    Option(handleToSession.get(sessionHandle))
  }

  def getSession(sessionHandle: SessionHandle): Session = {
    getSessionOption(sessionHandle).getOrElse(throw KyuubiSQLException(s"Invalid $sessionHandle"))
  }

  final protected def setSession(sessionHandle: SessionHandle, session: Session): Unit = {
    handleToSession.put(sessionHandle, session)
  }

  /**
   * Get the count of active user sessions, which excludes alive probe sessions.
   */
  def getActiveUserSessionCount: Int = handleToSession.values().asScala.count(!_.isForAliveProbe)

  def allSessions(): Iterable[Session] = handleToSession.values().asScala

  def getExecPoolSize: Int = {
    assert(execPool != null)
    execPool.getPoolSize
  }

  def getActiveCount: Int = {
    assert(execPool != null)
    execPool.getActiveCount
  }

  def getWorkQueueSize: Int = {
    assert(execPool != null)
    execPool.getQueue.size()
  }

  private var _confRestrictList: Set[String] = _
  private var _confIgnoreList: Set[String] = _
  private var _batchConfIgnoreList: Set[String] = _
  private lazy val _confRestrictMatchList: Set[String] =
    _confRestrictList.filter(_.endsWith(".*")).map(_.stripSuffix(".*"))
  private lazy val _confIgnoreMatchList: Set[String] =
    _confIgnoreList.filter(_.endsWith(".*")).map(_.stripSuffix(".*"))
  private lazy val _batchConfIgnoreMatchList: Set[String] =
    _batchConfIgnoreList.filter(_.endsWith(".*")).map(_.stripSuffix(".*"))

  // strip prefix and validate whether if key is restricted, ignored or valid
  def validateKey(key: String, value: String): Option[(String, String)] = {
    val normalizedKey =
      if (key.startsWith(SET_PREFIX)) {
        val newKey = key.substring(SET_PREFIX.length)
        if (newKey.startsWith(ENV_PREFIX)) {
          throw KyuubiSQLException(s"$key is forbidden, env:* variables can not be set.")
        } else if (newKey.startsWith(SYSTEM_PREFIX)) {
          newKey.substring(SYSTEM_PREFIX.length)
        } else if (newKey.startsWith(HIVECONF_PREFIX)) {
          newKey.substring(HIVECONF_PREFIX.length)
        } else if (newKey.startsWith(HIVEVAR_PREFIX)) {
          newKey.substring(HIVEVAR_PREFIX.length)
        } else if (newKey.startsWith(METACONF_PREFIX)) {
          newKey.substring(METACONF_PREFIX.length)
        } else {
          newKey
        }
      } else {
        key
      }

    if (_confRestrictMatchList.exists(normalizedKey.startsWith) ||
      _confRestrictList.contains(normalizedKey)) {
      throw KyuubiSQLException(s"$normalizedKey is a restrict key according to the server-side" +
        s" configuration, please remove it and retry if you want to proceed")
    } else if (_confIgnoreMatchList.exists(normalizedKey.startsWith) ||
      _confIgnoreList.contains(normalizedKey)) {
      warn(s"$normalizedKey is a ignored key according to the server-side configuration")
      None
    } else {
      Some((normalizedKey, value))
    }
  }

  def validateAndNormalizeConf(config: Map[String, String]): Map[String, String] = config.flatMap {
    case (k, v) => validateKey(k, v)
  }

  // validate whether if a batch key should be ignored
  def validateBatchKey(key: String, value: String): Option[(String, String)] = {
    if (_batchConfIgnoreMatchList.exists(key.startsWith) || _batchConfIgnoreList.contains(key)) {
      warn(s"$key is a ignored batch key according to the server-side configuration")
      None
    } else {
      Some((key, value))
    }
  }

  def validateBatchConf(config: Map[String, String]): Map[String, String] = config.flatMap {
    case (k, v) => validateBatchKey(k, v)
  }

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    this.conf = conf
    addService(operationManager)
    initOperationLogRootDir()

    val poolSize: Int =
      if (isServer) {
        conf.get(SERVER_EXEC_POOL_SIZE)
      } else {
        conf.get(ENGINE_EXEC_POOL_SIZE)
      }

    val waitQueueSize: Int =
      if (isServer) {
        conf.get(SERVER_EXEC_WAIT_QUEUE_SIZE)
      } else {
        conf.get(ENGINE_EXEC_WAIT_QUEUE_SIZE)
      }
    val keepAliveMs: Long =
      if (isServer) {
        conf.get(SERVER_EXEC_KEEPALIVE_TIME)
      } else {
        conf.get(ENGINE_EXEC_KEEPALIVE_TIME)
      }

    _confRestrictList = conf.get(SESSION_CONF_RESTRICT_LIST)
    _confIgnoreList = conf.get(SESSION_CONF_IGNORE_LIST) +
      s"${SESSION_USER_SIGN_ENABLED.key}"
    _batchConfIgnoreList = conf.get(BATCH_CONF_IGNORE_LIST)

    execPool = ThreadUtils.newDaemonQueuedThreadPool(
      poolSize,
      waitQueueSize,
      keepAliveMs,
      s"$name-exec-pool")
    super.initialize(conf)
  }

  override def start(): Unit = synchronized {
    startTimeoutChecker()
    super.start()
  }

  override def stop(): Unit = synchronized {
    super.stop()
    shutdown = true
    val shutdownTimeout: Long =
      if (isServer) {
        conf.get(SERVER_EXEC_POOL_SHUTDOWN_TIMEOUT)
      } else {
        conf.get(ENGINE_EXEC_POOL_SHUTDOWN_TIMEOUT)
      }

    ThreadUtils.shutdown(timeoutChecker, Duration(shutdownTimeout, TimeUnit.MILLISECONDS))
    ThreadUtils.shutdown(execPool, Duration(shutdownTimeout, TimeUnit.MILLISECONDS))
  }

  private def startTimeoutChecker(): Unit = {
    val interval = conf.get(SESSION_CHECK_INTERVAL)

    val checkTask = new Runnable {
      override def run(): Unit = {
        info(s"Checking sessions timeout, current count: $getActiveUserSessionCount")
        val current = System.currentTimeMillis
        if (!shutdown) {
          for (session <- handleToSession.values().asScala) {
            try {
              if (session.lastAccessTime + session.sessionIdleTimeoutThreshold <= current &&
                session.getNoOperationTime > session.sessionIdleTimeoutThreshold) {
                info(s"Closing session ${session.handle.identifier} that has been idle for more" +
                  s" than ${session.sessionIdleTimeoutThreshold} ms")
                closeSession(session.handle)
              } else {
                session.closeExpiredOperations()
              }
            } catch {
              case NonFatal(e) => warn(s"Error checking session ${session.handle} timeout", e)
            }
          }
        }
      }
    }

    scheduleTolerableRunnableWithFixedDelay(
      timeoutChecker,
      checkTask,
      interval,
      interval,
      TimeUnit.MILLISECONDS)
  }

  private[kyuubi] def startTerminatingChecker(stop: () => Unit): Unit = if (!isServer) {
    // initialize `_latestLogoutTime` at start
    _latestLogoutTime = System.currentTimeMillis()
    val interval = conf.get(ENGINE_CHECK_INTERVAL)
    val idleTimeout = conf.get(ENGINE_IDLE_TIMEOUT)
    if (idleTimeout > 0) {
      val checkTask = new Runnable {
        override def run(): Unit = {
          if (!shutdown && System.currentTimeMillis() - latestLogoutTime > idleTimeout &&
            getActiveUserSessionCount <= 0) {
            info(s"Idled for more than $idleTimeout ms, terminating")
            stop()
          }
        }
      }
      scheduleTolerableRunnableWithFixedDelay(
        timeoutChecker,
        checkTask,
        interval,
        interval,
        TimeUnit.MILLISECONDS)
    }
  }
}
