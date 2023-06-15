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

import java.util.concurrent.TimeUnit

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
import org.apache.kyuubi.plugin.{GroupProvider, PluginLoader, SessionConfAdvisor}
import org.apache.kyuubi.server.metadata.{MetadataManager, MetadataRequestsRetryRef}
import org.apache.kyuubi.server.metadata.api.Metadata
import org.apache.kyuubi.sql.parser.server.KyuubiParser
import org.apache.kyuubi.util.{SignUtils, ThreadUtils}

class KyuubiSessionManager private (name: String) extends SessionManager(name) {

  def this() = this(classOf[KyuubiSessionManager].getSimpleName)

  private val parser = new KyuubiParser()

  val operationManager = new KyuubiOperationManager()
  val credentialsManager = new HadoopCredentialsManager()
  val applicationManager = new KyuubiApplicationManager()

  // Currently, the metadata manager is used by the REST frontend which provides batch job APIs,
  // so we initialize it only when Kyuubi starts with the REST frontend.
  lazy val metadataManager: Option[MetadataManager] =
    if (conf.isRESTEnabled) Some(new MetadataManager()) else None

  // lazy is required for plugins since the conf is null when this class initialization
  lazy val sessionConfAdvisor: SessionConfAdvisor = PluginLoader.loadSessionConfAdvisor(conf)
  lazy val groupProvider: GroupProvider = PluginLoader.loadGroupProvider(conf)

  private var limiter: Option[SessionLimiter] = None
  private var batchLimiter: Option[SessionLimiter] = None
  lazy val (signingPrivateKey, signingPublicKey) = SignUtils.generateKeyPair()

  private val engineAliveChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(s"$name-engine-alive-checker")

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
      this.getConf.getUserDefaults(user),
      parser)
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
          ms.incCount(MetricRegistry.name(CONN_FAIL, SessionType.INTERACTIVE.toString))
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
      session match {
        case _: KyuubiBatchSession =>
          batchLimiter.foreach(_.decrement(UserIpAddress(session.user, session.ipAddress)))
        case _ =>
          limiter.foreach(_.decrement(UserIpAddress(session.user, session.ipAddress)))
      }
    }
  }

  // scalastyle:off
  def createBatchSession(
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String],
      batchType: String,
      batchName: Option[String],
      resource: String,
      className: String,
      batchConf: Map[String, String],
      batchArgs: Seq[String],
      recoveryMetadata: Option[Metadata] = None,
      shouldRunAsync: Boolean): KyuubiBatchSession = {
    // scalastyle:on
    val username = Option(user).filter(_.nonEmpty).getOrElse("anonymous")
    val sessionConf = this.getConf.getUserDefaults(user)
    new KyuubiBatchSession(
      username,
      password,
      ipAddress,
      conf,
      this,
      sessionConf,
      batchType,
      batchName,
      resource,
      className,
      batchConf,
      batchArgs,
      recoveryMetadata,
      shouldRunAsync)
  }

  private[kyuubi] def openBatchSession(batchSession: KyuubiBatchSession): SessionHandle = {
    val user = batchSession.user
    val ipAddress = batchSession.ipAddress
    batchLimiter.foreach(_.increment(UserIpAddress(user, ipAddress)))
    val handle = batchSession.handle
    try {
      batchSession.open()
      setSession(handle, batchSession)
      logSessionCountInfo(batchSession, "opened")
      handle
    } catch {
      case e: Exception =>
        try {
          batchSession.close()
        } catch {
          case t: Throwable =>
            warn(s"Error closing batch session[$handle] for $user client ip: $ipAddress", t)
        }
        MetricsSystem.tracing { ms =>
          ms.incCount(CONN_FAIL)
          ms.incCount(MetricRegistry.name(CONN_FAIL, user))
          ms.incCount(MetricRegistry.name(CONN_FAIL, SessionType.BATCH.toString))
        }
        throw KyuubiSQLException(
          s"Error opening batch session[$handle] for $user client ip $ipAddress," +
            s" due to ${e.getMessage}",
          e)
    }
  }

  def openBatchSession(
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String],
      batchRequest: BatchRequest,
      shouldRunAsync: Boolean = true): SessionHandle = {
    val batchSession = createBatchSession(
      user,
      password,
      ipAddress,
      conf,
      batchRequest.getBatchType,
      Option(batchRequest.getName),
      batchRequest.getResource,
      batchRequest.getClassName,
      batchRequest.getConf.asScala.toMap,
      batchRequest.getArgs.asScala,
      None,
      shouldRunAsync)
    openBatchSession(batchSession)
  }

  def getBatchSession(sessionHandle: SessionHandle): Option[KyuubiBatchSession] = {
    getSessionOption(sessionHandle).map(_.asInstanceOf[KyuubiBatchSession])
  }

  def insertMetadata(metadata: Metadata): Unit = {
    metadataManager.foreach(_.insertMetadata(metadata))
  }

  def updateMetadata(metadata: Metadata): Unit = {
    metadataManager.foreach(_.updateMetadata(metadata))
  }

  def getMetadataRequestsRetryRef(identifier: String): Option[MetadataRequestsRetryRef] = {
    metadataManager.flatMap(mm => Option(mm.getMetadataRequestsRetryRef(identifier)))
  }

  def deRegisterMetadataRequestsRetryRef(identifier: String): Unit = {
    metadataManager.foreach(_.deRegisterRequestsRetryRef(identifier))
  }

  def getBatchFromMetadataStore(batchId: String): Option[Batch] = {
    metadataManager.flatMap(mm => mm.getBatch(batchId))
  }

  def getBatchesFromMetadataStore(
      batchType: String,
      batchUser: String,
      batchState: String,
      createTime: Long,
      endTime: Long,
      from: Int,
      size: Int): Seq[Batch] = {
    metadataManager.map { mm =>
      mm.getBatches(batchType, batchUser, batchState, createTime, endTime, from, size)
    }.getOrElse(Seq.empty)
  }

  def getBatchMetadata(batchId: String): Option[Metadata] = {
    metadataManager.flatMap(_.getBatchSessionMetadata(batchId))
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
      ms.registerGauge(EXEC_POOL_WORK_QUEUE_SIZE, getWorkQueueSize, 0)
    }
    super.start()
    startEngineAliveChecker()
  }

  def getBatchSessionsToRecover(kyuubiInstance: String): Seq[KyuubiBatchSession] = {
    Seq(OperationState.PENDING, OperationState.RUNNING).flatMap { stateToRecover =>
      metadataManager.map(_.getBatchesRecoveryMetadata(
        stateToRecover.toString,
        kyuubiInstance,
        0,
        Int.MaxValue).map { metadata =>
        createBatchSession(
          metadata.username,
          "anonymous",
          metadata.ipAddress,
          metadata.requestConf,
          metadata.engineType,
          Option(metadata.requestName),
          metadata.resource,
          metadata.className,
          metadata.requestConf,
          metadata.requestArgs,
          Some(metadata),
          shouldRunAsync = true)
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
    val userUnlimitedList = conf.get(SERVER_LIMIT_CONNECTIONS_USER_UNLIMITED_LIST)
    limiter = applySessionLimiter(userLimit, ipAddressLimit, userIpAddressLimit, userUnlimitedList)

    val userBatchLimit = conf.get(SERVER_LIMIT_BATCH_CONNECTIONS_PER_USER).getOrElse(0)
    val ipAddressBatchLimit = conf.get(SERVER_LIMIT_BATCH_CONNECTIONS_PER_IPADDRESS).getOrElse(0)
    val userIpAddressBatchLimit =
      conf.get(SERVER_LIMIT_BATCH_CONNECTIONS_PER_USER_IPADDRESS).getOrElse(0)
    batchLimiter = applySessionLimiter(
      userBatchLimit,
      ipAddressBatchLimit,
      userIpAddressBatchLimit,
      userUnlimitedList)
  }

  private[kyuubi] def getUnlimitedUsers(): Set[String] = {
    limiter.orElse(batchLimiter).map(SessionLimiter.getUnlimitedUsers).getOrElse(Set.empty)
  }

  private[kyuubi] def refreshUnlimitedUsers(conf: KyuubiConf): Unit = {
    val unlimitedUsers = conf.get(SERVER_LIMIT_CONNECTIONS_USER_UNLIMITED_LIST).toSet
    limiter.foreach(SessionLimiter.resetUnlimitedUsers(_, unlimitedUsers))
    batchLimiter.foreach(SessionLimiter.resetUnlimitedUsers(_, unlimitedUsers))
  }

  private def applySessionLimiter(
      userLimit: Int,
      ipAddressLimit: Int,
      userIpAddressLimit: Int,
      userUnlimitedList: Seq[String]): Option[SessionLimiter] = {
    Seq(userLimit, ipAddressLimit, userIpAddressLimit).find(_ > 0).map(_ =>
      SessionLimiter(userLimit, ipAddressLimit, userIpAddressLimit, userUnlimitedList.toSet))
  }

  private def startEngineAliveChecker(): Unit = {
    val interval = conf.get(KyuubiConf.ENGINE_ALIVE_PROBE_INTERVAL)
    val checkTask: Runnable = () => {
      allSessions().foreach { session =>
        if (!session.asInstanceOf[KyuubiSessionImpl].checkEngineAlive()) {
          try {
            closeSession(session.handle)
            logger.info(s"The session ${session.handle} has been closed " +
              s"due to engine unresponsiveness (checked by the engine alive checker).")
          } catch {
            case e: KyuubiSQLException =>
              warn(s"Error closing session ${session.handle}", e)
          }
        }
      }
    }
    engineAliveChecker.scheduleWithFixedDelay(checkTask, interval, interval, TimeUnit.MILLISECONDS)
  }

}
