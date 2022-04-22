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

import java.util.UUID

import com.codahale.metrics.MetricRegistry
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.{KyuubiSQLException, Utils}
import org.apache.kyuubi.cli.HandleIdentifier
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.credentials.HadoopCredentialsManager
import org.apache.kyuubi.engine.KyuubiApplicationManager
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.KyuubiOperationManager
import org.apache.kyuubi.plugin.{PluginLoader, SessionConfAdvisor}
import org.apache.kyuubi.server.api.v1.BatchRequest

class KyuubiSessionManager private (name: String) extends SessionManager(name) {
  import KyuubiSessionManager._

  def this() = this(classOf[KyuubiSessionManager].getSimpleName)

  val operationManager = new KyuubiOperationManager()
  val credentialsManager = new HadoopCredentialsManager()
  // this lazy is must be specified since the conf is null when the class initialization
  lazy val sessionConfAdvisor: SessionConfAdvisor = PluginLoader.loadSessionConfAdvisor(conf)
  val applicationManager = new KyuubiApplicationManager()

  private var limiter: Option[SessionLimiter] = None

  override def initialize(conf: KyuubiConf): Unit = {
    addService(applicationManager)
    addService(credentialsManager)
    val absPath = Utils.getAbsolutePathFromWork(conf.get(SERVER_OPERATION_LOG_DIR_ROOT))
    _operationLogRoot = Some(absPath.toAbsolutePath.toString)
    initSessionLimiter(conf)
    super.initialize(conf)
  }

  override protected def createSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): Session = {
    // inject client ip into session conf
    val newConf = conf + (CLIENT_IP_KEY -> ipAddress)
    new KyuubiSessionImpl(
      protocol,
      user,
      password,
      ipAddress,
      newConf,
      this,
      this.getConf.getUserDefaults(user))
  }

  override def openSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): SessionHandle = {
    val username = Option(user).filter(_.nonEmpty).getOrElse("anonymous")
    limiter.foreach(_.increment(UserIpAddress(username, ipAddress)))
    try {
      super.openSession(protocol, username, password, ipAddress, conf)
    } catch {
      case e: Throwable =>
        MetricsSystem.tracing { ms =>
          ms.incCount(CONN_FAIL)
          ms.incCount(MetricRegistry.name(CONN_FAIL, user))
        }
        throw KyuubiSQLException(
          s"Error opening session for $username client ip $ipAddress, due to ${e.getMessage}",
          e)
    }
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    val session = getSession(sessionHandle)
    super.closeSession(sessionHandle)
    limiter.foreach(_.decrement(UserIpAddress(session.user, session.ipAddress)))
  }

  def openBatchSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String],
      batchRequest: BatchRequest): SessionHandle = {
    val username = Option(user).filter(_.nonEmpty).getOrElse("anonymous")
    val batchSession = new KyuubiBatchSessionImpl(
      protocol,
      user,
      password,
      ipAddress,
      conf,
      this,
      // TODO: user defaults conf for batch session
      this.getConf.getUserDefaults(user),
      batchRequest)
    try {
      val handle = batchSession.handle
      batchSession.open()
      setSession(handle, batchSession)
      info(s"$user's batch session with $handle is opened, current opening sessions" +
        s" $getOpenSessionCount")
      handle
    } catch {
      case e: Exception =>
        try {
          batchSession.close()
        } catch {
          case t: Throwable =>
            warn(s"Error closing batch session for $user client ip: $ipAddress", t)
        }
        MetricsSystem.tracing { ms =>
          ms.incCount(CONN_FAIL)
          ms.incCount(MetricRegistry.name(CONN_FAIL, user))
        }
        throw KyuubiSQLException(
          s"Error opening batch session for $username client ip $ipAddress, due to ${e.getMessage}",
          e)
    }
  }

  def newBatchSessionHandle(protocol: TProtocolVersion): SessionHandle = {
    SessionHandle(HandleIdentifier(UUID.randomUUID(), STATIC_BATCH_SECRET_UUID), protocol)
  }

  override def start(): Unit = synchronized {
    MetricsSystem.tracing { ms =>
      ms.registerGauge(CONN_OPEN, getOpenSessionCount, 0)
      ms.registerGauge(EXEC_POOL_ALIVE, getExecPoolSize, 0)
      ms.registerGauge(EXEC_POOL_ACTIVE, getActiveCount, 0)
    }
    super.start()
  }

  override protected def isServer: Boolean = true

  private def initSessionLimiter(conf: KyuubiConf): Unit = {
    val userLimit = conf.get(SERVER_LIMIT_CONNECTIONS_PER_USER).getOrElse(0)
    val ipAddressLimit = conf.get(SERVER_LIMIT_CONNECTIONS_PER_IPADDRESS).getOrElse(0)
    val userIpAddressLimit = conf.get(SERVER_LIMIT_CONNECTIONS_PER_USER_IPADDRESS).getOrElse(0)
    if (userLimit > 0 || ipAddressLimit > 0 || userIpAddressLimit > 0) {
      limiter = Some(SessionLimiter(userLimit, ipAddressLimit, userIpAddressLimit))
    }
  }
}

object KyuubiSessionManager {

  /**
   * The static session secret UUID used for batch session handle.
   * To keep compatibility, please do not change it.
   */
  val STATIC_BATCH_SECRET_UUID: UUID = UUID.fromString("c2ee5b97-3ea0-41fc-ac16-9bd708ed8f38")
}
