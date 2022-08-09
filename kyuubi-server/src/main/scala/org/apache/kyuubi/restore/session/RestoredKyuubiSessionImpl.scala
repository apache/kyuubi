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

package org.apache.kyuubi.restore.session

import scala.collection.JavaConverters._

import com.codahale.metrics.MetricRegistry
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.{KyuubiSQLException, Utils}
import org.apache.kyuubi.client.KyuubiSyncThriftClient
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SECURITY_ENABLED, ENGINE_SHARE_LEVEL, ENGINE_TYPE}
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_CREDENTIALS_KEY
import org.apache.kyuubi.engine.{KyuubiApplicationManager, ShareLevel}
import org.apache.kyuubi.engine.ShareLevel.{GROUP, SERVER, ShareLevel}
import org.apache.kyuubi.events.{EventBus, KyuubiSessionEvent}
import org.apache.kyuubi.ha.client.DiscoveryClientProvider.withDiscoveryClient
import org.apache.kyuubi.metrics.MetricsConstants.{CONN_OPEN, CONN_TOTAL}
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.service.authentication.InternalSecurityAccessor
import org.apache.kyuubi.session.{KyuubiSession, KyuubiSessionManager, SessionHandle, SessionType}
import org.apache.kyuubi.session.SessionType.SessionType

class RestoredKyuubiSessionImpl(
    protocol: TProtocolVersion,
    user: String,
    password: String,
    ipAddress: String,
    conf: Map[String, String],
    sessionManager: KyuubiSessionManager,
    sessionConf: KyuubiConf,
    engineSpace: String,
    engineSessionHandle: String,
    sessionHandle: String)
  extends KyuubiSession(protocol, user, password, ipAddress, conf, sessionManager) {

  override val handle: SessionHandle = SessionHandle.fromUUID(sessionHandle)

  override val sessionType: SessionType = SessionType.SQL

  private[kyuubi] val optimizedConf: Map[String, String] = {
    val confOverlay = sessionManager.sessionConfAdvisor.getConfOverlay(
      user,
      normalizedConf.asJava)
    if (confOverlay != null) {
      normalizedConf ++ confOverlay.asScala
    } else {
      warn(s"the server plugin return null value for user: $user, ignore it")
      normalizedConf
    }
  }

  // TODO: needs improve the hardcode
  optimizedConf.foreach {
    case ("use:database", _) =>
    case ("kyuubi.engine.pool.size.threshold", _) =>
    case (key, value) => sessionConf.set(key, value)
  }

  private var _client: KyuubiSyncThriftClient = _
  override def client: KyuubiSyncThriftClient = _client

  private val _engineSessionHandle: SessionHandle = SessionHandle.fromUUID(engineSessionHandle)

  private lazy val engineCredentials = renewEngineCredentials()

  override def open(): Unit = {
    throw new UnsupportedOperationException("Can not open restored session.")
  }

  private val sessionEvent = KyuubiSessionEvent(this)
  EventBus.post(sessionEvent)

  override def getSessionEvent: Option[KyuubiSessionEvent] = {
    Option(sessionEvent)
  }

  override def checkSessionAccessPathURIs(): Unit = {
    KyuubiApplicationManager.checkApplicationAccessPaths(
      sessionConf.get(ENGINE_TYPE),
      sessionConf.getAll,
      sessionManager.getConf)
  }

  override def close(): Unit = {
    super.close()
    sessionManager.credentialsManager.removeSessionCredentialsEpoch(handle.identifier.toString)
    try {
      if (_client != null) _client.closeSession()
    } finally {
      sessionEvent.endTime = System.currentTimeMillis()
      EventBus.post(sessionEvent)
      MetricsSystem.tracing(_.decCount(MetricRegistry.name(CONN_OPEN, user)))
    }
  }

  private[restore] def reopen(): Unit = {
    MetricsSystem.tracing { ms =>
      ms.incCount(CONN_TOTAL)
      ms.incCount(MetricRegistry.name(CONN_OPEN, user))
    }

    checkSessionAccessPathURIs()

    // we should call super.open before running launch engine operation
    super.open()

    withDiscoveryClient(sessionConf) { discoveryClient =>
      var openEngineSessionConf = optimizedConf
      if (engineCredentials.nonEmpty) {
        sessionConf.set(KYUUBI_ENGINE_CREDENTIALS_KEY, engineCredentials)
        openEngineSessionConf =
          optimizedConf ++ Map(KYUUBI_ENGINE_CREDENTIALS_KEY -> engineCredentials)
      }
      val (host, port) = discoveryClient.getServerHost(engineSpace).getOrElse {
        throw KyuubiSQLException(s"Engine [$engineSpace] server host not found.")
      }
      val passwd =
        if (sessionManager.getConf.get(ENGINE_SECURITY_ENABLED)) {
          InternalSecurityAccessor.get().issueToken()
        } else {
          Option(password).filter(_.nonEmpty).getOrElse("anonymous")
        }
      try {
        _client = KyuubiSyncThriftClient.createClient(user, passwd, host, port, sessionConf)
        _client.setRemoteSessionHandle(_engineSessionHandle)
      } catch {
        case e: Throwable =>
          error(s"Opening engine [$host:$port]  for $user session failed", e)
          throw e
      }
      logSessionInfo(s"Connected to engine [$host:$port] with ${_engineSessionHandle}]")
    }
  }

  // Share level of the engine
  private val shareLevel: ShareLevel = ShareLevel.withName(sessionConf.get(ENGINE_SHARE_LEVEL))

  // Launcher of the engine
  val _appUser: String = shareLevel match {
    case SERVER => Utils.currentUser
    case GROUP =>
      val clientUGI = UserGroupInformation.createRemoteUser(user)
      // Similar to `clientUGI.getPrimaryGroupName` (avoid IOE) to get the Primary GroupName of
      // the client user mapping to
      clientUGI.getGroupNames.headOption match {
        case Some(primaryGroup) => primaryGroup
        case None =>
          warn(s"There is no primary group for $user, use the client user name as group directly")
          user
      }
    case _ => user
  }

  override def appUser: String = _appUser

  private def renewEngineCredentials(): String = {
    try {
      sessionManager.credentialsManager.renewCredentials(appUser)
    } catch {
      case e: Exception =>
        error(s"Failed to renew engine credentials for $handle", e)
        ""
    }
  }
}
