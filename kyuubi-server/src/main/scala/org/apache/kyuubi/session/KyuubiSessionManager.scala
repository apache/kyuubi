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

import scala.collection.JavaConverters._

import com.codahale.metrics.MetricRegistry
import com.google.common.annotations.VisibleForTesting
import org.apache.hive.service.rpc.thrift.TProtocolVersion

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.client.api.v1.dto.{Batch, BatchRequest}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.credentials.HadoopCredentialsManager
import org.apache.kyuubi.engine.KyuubiApplicationManager
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.{KyuubiOperationManager, OperationState}
import org.apache.kyuubi.plugin.{PluginLoader, SessionConfAdvisor}
import org.apache.kyuubi.server.metadata.{MetadataManager, MetadataRequestsRetryRef}
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.server.metadata.jdbc.JDBCUserCustomStore

class KyuubiSessionManager private (name: String) extends SessionManager(name) {

  def this() = this(classOf[KyuubiSessionManager].getSimpleName)

  val operationManager = new KyuubiOperationManager()
  val credentialsManager = new HadoopCredentialsManager()
  // this lazy is must be specified since the conf is null when the class initialization
  lazy val sessionConfAdvisor: SessionConfAdvisor = PluginLoader.loadSessionConfAdvisor(conf)
  val applicationManager = new KyuubiApplicationManager()
  private lazy val metadataManager: Option[MetadataManager] = {
    // Currently, the metadata manager is used by the REST frontend which provides batch job APIs,
    // so we initialize it only when Kyuubi starts with the REST frontend.
    if (conf.get(FRONTEND_PROTOCOLS).map(FrontendProtocols.withName)
        .contains(FrontendProtocols.REST)) {
      Option(new MetadataManager())
    } else {
      None
    }
  }

  private var limiter: Option[SessionLimiter] = None

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
    addService(applicationManager)
    addService(credentialsManager)
    metadataManager.foreach(addService)
    initSessionLimiter(conf)
    super.initialize(conf)
  }

  override protected def createSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): Session = {
    new KyuubiSessionImpl(
      protocol,
      user,
      password,
      ipAddress,
      conf,
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
    try {
      super.closeSession(sessionHandle)
    } finally {
      limiter.foreach(_.decrement(UserIpAddress(session.user, session.ipAddress)))
    }
  }

  private def createBatchSession(
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String],
      batchRequest: BatchRequest,
      recoveryMetadata: Option[Metadata] = None): KyuubiBatchSessionImpl = {
    val username = Option(user).filter(_.nonEmpty).getOrElse("anonymous")
    new KyuubiBatchSessionImpl(
      username,
      password,
      ipAddress,
      conf,
      this,
      this.getConf.getUserDefaults(user),
      batchRequest,
      recoveryMetadata)
  }

  private[kyuubi] def openBatchSession(batchSession: KyuubiBatchSessionImpl): SessionHandle = {
    val user = batchSession.user
    val ipAddress = batchSession.ipAddress
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
          s"Error opening batch session for $user client ip $ipAddress, due to ${e.getMessage}",
          e)
    }
  }

  def openBatchSession(
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String],
      batchRequest: BatchRequest): SessionHandle = {
    val batchSession = createBatchSession(user, password, ipAddress, conf, batchRequest)
    openBatchSession(batchSession)
  }

  def getBatchSessionImpl(sessionHandle: SessionHandle): KyuubiBatchSessionImpl = {
    getSessionOption(sessionHandle).map(_.asInstanceOf[KyuubiBatchSessionImpl]).orNull
  }

  def insertMetadata(metadata: Metadata): Unit = {
    metadataManager.foreach(_.insertMetadata(metadata))
  }

  def updateMetadata(metadata: Metadata): Unit = {
    metadataManager.foreach(_.updateMetadata(metadata))
  }

  def getMetadataRequestsRetryRef(identifier: String): Option[MetadataRequestsRetryRef] = {
    Option(metadataManager.map(_.getMetadataRequestsRetryRef(identifier)).orNull)
  }

  def deRegisterMetadataRequestsRetryRef(identifier: String): Unit = {
    metadataManager.foreach(_.deRegisterRequestsRetryRef(identifier))
  }

  def getBatchFromMetadataStore(batchId: String): Batch = {
    metadataManager.map(_.getBatch(batchId)).orNull
  }

  def getBatchesFromMetadataStore(
      batchType: String,
      batchUser: String,
      batchState: String,
      createTime: Long,
      endTime: Long,
      from: Int,
      size: Int): Seq[Batch] = {
    metadataManager.map(
      _.getBatches(batchType, batchUser, batchState, createTime, endTime, from, size))
      .getOrElse(Seq.empty)
  }

  def getBatchMetadata(batchId: String): Metadata = {
    metadataManager.map(_.getBatchSessionMetadata(batchId)).orNull
  }

  @VisibleForTesting
  def cleanupMetadata(identifier: String): Unit = {
    metadataManager.foreach(_.cleanupMetadataById(identifier))
  }

  override def start(): Unit = synchronized {
    MetricsSystem.tracing { ms =>
      ms.registerGauge(CONN_OPEN, getOpenSessionCount, 0)
      ms.registerGauge(EXEC_POOL_ALIVE, getExecPoolSize, 0)
      ms.registerGauge(EXEC_POOL_ACTIVE, getActiveCount, 0)
    }
    super.start()
  }

  def getBatchSessionsToRecover(kyuubiInstance: String): Seq[KyuubiBatchSessionImpl] = {
    Seq(OperationState.PENDING, OperationState.RUNNING).flatMap { stateToRecover =>
      metadataManager.map(_.getBatchesRecoveryMetadata(
        stateToRecover.toString,
        kyuubiInstance,
        0,
        Int.MaxValue).map { metadata =>
        val batchRequest = new BatchRequest(
          metadata.engineType,
          metadata.resource,
          metadata.className,
          metadata.requestName,
          metadata.requestConf.asJava,
          metadata.requestArgs.asJava)

        createBatchSession(
          metadata.username,
          "anonymous",
          metadata.ipAddress,
          metadata.requestConf,
          batchRequest,
          Some(metadata))
      }).getOrElse(Seq.empty)
    }
  }

  def getPeerInstanceClosedBatchSessions(kyuubiInstance: String): Seq[Metadata] = {
    Seq(OperationState.PENDING, OperationState.RUNNING).flatMap { stateToKill =>
      metadataManager.map(_.getPeerInstanceClosedBatchesMetadata(
        stateToKill.toString,
        kyuubiInstance,
        0,
        Int.MaxValue)).getOrElse(Seq.empty)
    }
  }

  override protected def isServer: Boolean = true

  private def initSessionLimiter(conf: KyuubiConf): Unit = {
    val userLimit = conf.get(SERVER_LIMIT_CONNECTIONS_PER_USER).getOrElse(0)
    val ipAddressLimit = conf.get(SERVER_LIMIT_CONNECTIONS_PER_IPADDRESS).getOrElse(0)
    val userIpAddressLimit = conf.get(SERVER_LIMIT_CONNECTIONS_PER_USER_IPADDRESS).getOrElse(0)
    val userMap: Map[String, Int] = try {
      JDBCUserCustomStore().getUserLimitList.toMap
    } catch {
      case _: Throwable => Map()
    }
    if (userLimit > 0 || ipAddressLimit > 0 || userIpAddressLimit > 0) {
      limiter = Some(SessionLimiter(userMap, userLimit, ipAddressLimit, userIpAddressLimit))
    }
  }
}
