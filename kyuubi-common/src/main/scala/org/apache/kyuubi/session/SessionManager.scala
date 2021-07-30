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

import java.util.concurrent.{ConcurrentHashMap, Future, ThreadPoolExecutor, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.operation.OperationManager
import org.apache.kyuubi.service.CompositeService
import org.apache.kyuubi.util.ThreadUtils

/**
 * The [[SessionManager]] holds the all the connected [[Session]]s, provides us the APIs to
 * open, set, get, close [[Session]]s and cleans idled [[Session]]s with a daemon checker
 * thread.
 *
 * @param name Service Name
 */
abstract class SessionManager(name: String) extends CompositeService(name) {

  @volatile private var shutdown = false

  @volatile private var _latestLogoutTime: Long = System.currentTimeMillis()
  def latestLogoutTime: Long = _latestLogoutTime

  private val handleToSession = new ConcurrentHashMap[SessionHandle, Session]

  private val timeoutChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(s"$name-timeout-checker")

  protected def isServer: Boolean

  private var execPool: ThreadPoolExecutor = _

  def submitBackgroundOperation(r: Runnable): Future[_] = execPool.submit(r)

  def operationManager: OperationManager

  def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): SessionHandle

  def closeSession(sessionHandle: SessionHandle): Unit = {
    _latestLogoutTime = System.currentTimeMillis()
    val session = handleToSession.remove(sessionHandle)
    if (session == null) {
      throw KyuubiSQLException(s"Invalid $sessionHandle")
    }
    info(s"$sessionHandle is closed, current opening sessions $getOpenSessionCount")
    session.close()
  }

  def getSession(sessionHandle: SessionHandle): Session = {
    val session = handleToSession.get(sessionHandle)
    if (session == null) {
      throw KyuubiSQLException(s"Invalid $sessionHandle")
    }
    session
  }

  protected final def setSession(sessionHandle: SessionHandle, session: Session): Unit = {
    handleToSession.put(sessionHandle, session)
  }

  def getOpenSessionCount: Int = handleToSession.size()

  def getExecPoolSize: Int = {
    assert(execPool != null)
    execPool.getPoolSize
  }

  def getActiveCount: Int = {
    assert(execPool != null)
    execPool.getActiveCount
  }

  private var _confRestrictList: Set[String] = _
  private var _confIgnoreList: Set[String] = _

  // strip prefix and validate whether if key is restricted, ignored or valid
  def validateKey(key: String, value: String): Option[(String, String)] = {
    val normalizedKey = if (key.startsWith(SET_PREFIX)) {
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

    if (_confRestrictList.contains(normalizedKey)) {
      throw KyuubiSQLException(s"$normalizedKey is a restrict key according to the server-side" +
        s" configuration, please remove it and retry if you want to proceed")
    } else if (_confIgnoreList.contains(normalizedKey)) {
      warn(s"$normalizedKey is a ignored key according to the server-side configuration")
      None
    } else {
      Some((normalizedKey, value))
    }
  }

  def validateAndNormalizeConf(config: Map[String, String]): Map[String, String] = config.flatMap {
    case(k, v) => validateKey(k, v)
  }

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    addService(operationManager)

    val poolSize: Int = if (isServer) {
      conf.get(SERVER_EXEC_POOL_SIZE)
    } else {
      conf.get(ENGINE_EXEC_POOL_SIZE)
    }

    val waitQueueSize: Int = if (isServer) {
      conf.get(SERVER_EXEC_WAIT_QUEUE_SIZE)
    } else {
      conf.get(ENGINE_EXEC_WAIT_QUEUE_SIZE)
    }
    val keepAliveMs: Long = if (isServer) {
      conf.get(SERVER_EXEC_KEEPALIVE_TIME)
    } else {
      conf.get(ENGINE_EXEC_KEEPALIVE_TIME)
    }

    _confRestrictList = conf.get(SESSION_CONF_RESTRICT_LIST).toSet
    _confIgnoreList = conf.get(SESSION_CONF_IGNORE_LIST).toSet

    execPool = ThreadUtils.newDaemonQueuedThreadPool(
      poolSize, waitQueueSize, keepAliveMs, s"$name-exec-pool")
    super.initialize(conf)
  }

  override def start(): Unit = synchronized {
    startTimeoutChecker()
    super.start()
  }

  override def stop(): Unit = synchronized {
    super.stop()
    shutdown = true
    val shutdownTimeout: Long = if (isServer) {
      conf.get(ENGINE_EXEC_POOL_SHUTDOWN_TIMEOUT)
    } else {
      conf.get(SERVER_EXEC_POOL_SHUTDOWN_TIMEOUT)
    }
    timeoutChecker.shutdown()
    try {
      timeoutChecker.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)
    } catch {
      case i: InterruptedException =>
        warn(s"Exceeded to shutdown session timeout checker ", i)
    }

    if (execPool != null) {
      execPool.shutdown()
      try {
        execPool.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)
      } catch {
        case e: InterruptedException =>
          warn(s"Exceeded timeout($shutdownTimeout ms) to wait the exec-pool shutdown gracefully",
            e)
      }
    }
  }

  private def startTimeoutChecker(): Unit = {
    val interval = conf.get(SESSION_CHECK_INTERVAL)
    val timeout = conf.get(SESSION_IDLE_TIMEOUT)

    val checkTask = new Runnable {
      override def run(): Unit = {
        val current = System.currentTimeMillis
        if (!shutdown) {
          for (session <- handleToSession.values().asScala) {
            if (session.lastAccessTime + timeout <= current &&
              session.getNoOperationTime > timeout) {
              try {
                closeSession(session.handle)
              } catch {
                case e: KyuubiSQLException =>
                  warn(s"Error closing idle session ${session.handle}", e)
              }
            } else {
              session.closeExpiredOperations
            }
          }
        }
      }
    }

    timeoutChecker.scheduleWithFixedDelay(checkTask, interval, interval, TimeUnit.MILLISECONDS)
  }

  private[kyuubi] def startTerminatingChecker(): Unit = if (!isServer) {
    // initialize `_latestLogoutTime` at start
    _latestLogoutTime = System.currentTimeMillis()
    val interval = conf.get(ENGINE_CHECK_INTERVAL)
    val idleTimeout = conf.get(ENGINE_IDLE_TIMEOUT)
    val checkTask = new Runnable {
      override def run(): Unit = {
        if (!shutdown &&
          System.currentTimeMillis() - latestLogoutTime > idleTimeout && getOpenSessionCount <= 0) {
          info(s"Idled for more than $idleTimeout ms, terminating")
          sys.exit(0)
        }
      }
    }
    timeoutChecker.scheduleWithFixedDelay(checkTask, interval, interval, TimeUnit.MILLISECONDS)
  }
}
