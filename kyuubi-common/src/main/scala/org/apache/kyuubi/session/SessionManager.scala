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

import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.config.KyuubiConf
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

  private val handleToSession = new ConcurrentHashMap[SessionHandle, Session]
  private val timeoutChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(s"$name-timeout-checker")

  def operationManager: OperationManager

  def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): SessionHandle

  def closeSession(sessionHandle: SessionHandle): Unit = {
    val session = handleToSession.remove(sessionHandle)
    if (session == null) {
      throw KyuubiSQLException(s"Invalid $sessionHandle")
    }
    info(s"Session closed, $sessionHandle, current sessions:$getOpenSessionCount")
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

  override def initialize(conf: KyuubiConf): Unit = {
    addService(operationManager)
    super.initialize(conf)
  }

  override def start(): Unit = {
    startTimeoutChecker()
    super.start()
  }

  override def stop(): Unit = {
    super.stop()
    shutdown = true
    timeoutChecker.shutdown()
    try {
      timeoutChecker.awaitTermination(10, TimeUnit.SECONDS)
    } catch {
      case i: InterruptedException =>
        warn(s"Exceeded to shutdown session timeout checker ", i)
    }
  }

  def scheduleTimeoutChecker(r: Runnable, interval: Long, unit: TimeUnit): ScheduledFuture[_] =
    timeoutChecker.scheduleWithFixedDelay(r, interval, interval, unit)

  private def startTimeoutChecker(): Unit = {
    val interval = conf.get(KyuubiConf.SESSION_CHECK_INTERVAL)
    val timeout = conf.get(KyuubiConf.SESSION_TIMEOUT)

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
}
