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
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.KyuubiOperationManager
import org.apache.kyuubi.plugin.{PluginLoader, SessionConfAdvisor}
import org.apache.kyuubi.server.api.v1.BatchRequest

class KyuubiSessionManager private (name: String) extends SessionManager(name) {

  def this() = this(classOf[KyuubiSessionManager].getSimpleName)

  val operationManager = new KyuubiOperationManager()
  val credentialsManager = new HadoopCredentialsManager()
  // this lazy is must be specified since the conf is null when the class initialization
  lazy val sessionConfAdvisor: SessionConfAdvisor = PluginLoader.loadSessionConfAdvisor(conf)
  lazy val staticBatchSecretId: UUID = conf.get(BATCH_STATIC_SECRET_ID)
    .map(UUID.fromString).getOrElse(UUID.randomUUID())

  override def initialize(conf: KyuubiConf): Unit = {
    addService(credentialsManager)
    val absPath = Utils.getAbsolutePathFromWork(conf.get(SERVER_OPERATION_LOG_DIR_ROOT))
    _operationLogRoot = Some(absPath.toAbsolutePath.toString)
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
    SessionHandle(HandleIdentifier(UUID.randomUUID(), staticBatchSecretId), protocol)
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
}
