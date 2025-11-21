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

import java.util.concurrent.{Semaphore, TimeUnit}

import scala.collection.JavaConverters._

import com.codahale.metrics.MetricRegistry
import com.google.common.annotations.VisibleForTesting

import org.apache.kyuubi.KyuubiSQLException
import org.apache.kyuubi.client.api.v1.dto.{Batch, BatchRequest}
import org.apache.kyuubi.client.util.BatchUtils.KYUUBI_BATCH_ID_KEY
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.{KYUUBI_BATCH_PRIORITY, KYUUBI_SESSION_REAL_USER_KEY}
import org.apache.kyuubi.credentials.HadoopCredentialsManager
import org.apache.kyuubi.engine.KyuubiApplicationManager
import org.apache.kyuubi.metrics.MetricsConstants._
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.{KyuubiOperationManager, OperationState}
import org.apache.kyuubi.plugin.{GroupProvider, PluginLoader, SessionConfAdvisor}
import org.apache.kyuubi.server.metadata.{MetadataManager, MetadataRequestsRetryRef}
import org.apache.kyuubi.server.metadata.api.{Metadata, MetadataFilter}
import org.apache.kyuubi.service.TempFileService
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion
import org.apache.kyuubi.sql.parser.server.KyuubiParser
import org.apache.kyuubi.util.{SignUtils, ThreadUtils}
import org.apache.kyuubi.util.ThreadUtils.scheduleTolerableRunnableWithFixedDelay

class KyuubiSessionManager private (name: String) extends SessionManager(name) {

  def this() = this(classOf[KyuubiSessionManager].getSimpleName)

  private val parser = new KyuubiParser()

  val operationManager = new KyuubiOperationManager()
  val credentialsManager = new HadoopCredentialsManager()

  // Currently, the metadata manager is used by the REST frontend which provides batch job APIs,
  // so we initialize it only when Kyuubi starts with the REST frontend.
  var metadataManager: Option[MetadataManager] = None
  var applicationManager: KyuubiApplicationManager = _

  // lazy is required for plugins since the conf is null when this class initialization
  lazy val sessionConfAdvisor: Seq[SessionConfAdvisor] = PluginLoader.loadSessionConfAdvisor(conf)
  lazy val groupProvider: GroupProvider = PluginLoader.loadGroupProvider(conf)

  private var limiter: Option[SessionLimiter] = None
  private var batchLimiter: Option[SessionLimiter] = None
  lazy val (signingPrivateKey, signingPublicKey) = SignUtils.generateKeyPair()

  var engineStartupProcessSemaphore: Option[Semaphore] = None

  private val engineConnectionAliveChecker =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor(s"$name-engine-alive-checker")

  val tempFileService = new TempFileService()

  override def initialize(conf: KyuubiConf): Unit = {
    this.conf = conf
    if (conf.isRESTEnabled) metadataManager = Some(new MetadataManager())
    applicationManager = new KyuubiApplicationManager(metadataManager)
    addService(applicationManager)
    addService(credentialsManager)
    addService(tempFileService)
    metadataManager.foreach(addService)
    initSessionLimiter(conf)
    initEngineStartupProcessSemaphore(conf)
    super.initialize(conf)
  }

  override protected def createSession(
      protocol: TProtocolVersion,
      user: String,
      password: String,
      ipAddress: String,
      conf: Map[String, String]): Session = {
    val userConf = this.getConf.getUserDefaults(user)
    new KyuubiSessionImpl(
      protocol,
      user,
      password,
      ipAddress,
      conf,
      this,
      userConf,
      userConf.get(ENGINE_DO_AS_ENABLED),
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
      batchArgs: Seq[String],
      metadata: Option[Metadata] = None,
      fromRecovery: Boolean): KyuubiBatchSession = {
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
      batchArgs,
      metadata,
      fromRecovery)
  }

  private[kyuubi] def openBatchSession(batchSession: KyuubiBatchSession): SessionHandle = {
    val user = batchSession.user
    val ipAddress = batchSession.ipAddress
    batchLimiter.foreach(_.increment(UserIpAddress(user, ipAddress)))
    val handle = batchSession.handle
    try {
      setSession(handle, batchSession)
      batchSession.open()
      logSessionCountInfo(batchSession, "opened")
      handle
    } catch {
      case e: Exception =>
        try {
          closeSession(handle)
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
      batchRequest: BatchRequest): SessionHandle = {
    val batchSession = createBatchSession(
      user,
      password,
      ipAddress,
      batchRequest.getConf.asScala.toMap,
      batchRequest.getBatchType,
      Option(batchRequest.getName),
      batchRequest.getResource,
      batchRequest.getClassName,
      batchRequest.getArgs.asScala.toSeq,
      None,
      fromRecovery = false)
    openBatchSession(batchSession)
  }

  def initializeBatchState(
      user: String,
      ipAddress: String,
      conf: Map[String, String],
      batchRequest: BatchRequest): String = {
    val realUser = conf.getOrElse(KYUUBI_SESSION_REAL_USER_KEY, user)
    val username = Option(user).filter(_.nonEmpty).getOrElse("anonymous")
    val batchId = conf(KYUUBI_BATCH_ID_KEY)
    val metadata = Metadata(
      identifier = batchId,
      sessionType = SessionType.BATCH,
      realUser = realUser,
      username = username,
      ipAddress = ipAddress,
      state = OperationState.INITIALIZED.toString,
      resource = batchRequest.getResource,
      className = batchRequest.getClassName,
      requestName = batchRequest.getName,
      requestConf = conf,
      requestArgs = batchRequest.getArgs.asScala.toSeq,
      createTime = System.currentTimeMillis(),
      engineType = batchRequest.getBatchType,
      priority = conf.get(KYUUBI_BATCH_PRIORITY).map(_.toInt).getOrElse(10))

    // there is a chance that operation failed w/ duplicated key error
    metadataManager.foreach(_.insertMetadata(metadata, asyncRetryOnError = false))
    batchId
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
      filter: MetadataFilter,
      from: Int,
      size: Int,
      desc: Boolean = false): Seq[Batch] = {
    metadataManager.map(_.getBatches(filter, from, size, desc)).getOrElse(Seq.empty)
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
      ms.registerGauge(CONN_OPEN, getActiveUserSessionCount, 0)
      ms.registerGauge(EXEC_POOL_ALIVE, getExecPoolSize, 0)
      ms.registerGauge(EXEC_POOL_ACTIVE, getActiveCount, 0)
      ms.registerGauge(EXEC_POOL_WORK_QUEUE_SIZE, getWorkQueueSize, 0)
      this.engineStartupProcessSemaphore.foreach { semaphore =>
        ms.markMeter(ENGINE_STARTUP_PERMIT_LIMIT, semaphore.availablePermits)
        ms.registerGauge(
          ENGINE_STARTUP_PERMIT_AVAILABLE,
          semaphore.availablePermits,
          semaphore.availablePermits)
        ms.registerGauge(ENGINE_STARTUP_PERMIT_WAITING, semaphore.getQueueLength, 0)
      }
    }
    super.start()
    startEngineAliveChecker()
  }

  def getBatchSessionsToRecover(kyuubiInstance: String, offset: Int, batchSize: Int): Seq[KyuubiBatchSession] = {
    Seq(OperationState.PENDING, OperationState.RUNNING).flatMap { stateToRecover =>
      metadataManager.map(_.getBatchesRecoveryMetadata(
        stateToRecover.toString,
        kyuubiInstance,
        offset,
        batchSize).map { metadata =>
        createBatchSessionFromRecovery(metadata)
      }).getOrElse(Seq.empty)
    }
  }

  def getSpecificBatchSessionsToRecover(
      batchIds: Seq[String],
      kyuubiInstance: String): Seq[KyuubiBatchSession] = {
    val batchStatesToRecovery = Set(OperationState.PENDING, OperationState.RUNNING)
    batchIds.flatMap { batchId =>
      getBatchSession(SessionHandle.fromUUID(batchId)) match {
        case Some(_) =>
          warn(s"Batch session $batchId is already active, skipping recovery.")
          None
        case None =>
          getBatchMetadata(batchId)
            .filter(m =>
              m.kyuubiInstance == kyuubiInstance && batchStatesToRecovery.contains(m.opState))
            .flatMap { metadata => Some(createBatchSessionFromRecovery(metadata)) }
      }
    }
  }

  private def createBatchSessionFromRecovery(metadata: Metadata): KyuubiBatchSession = {
    createBatchSession(
      metadata.username,
      "anonymous",
      metadata.ipAddress,
      metadata.requestConf,
      metadata.engineType,
      Option(metadata.requestName),
      metadata.resource,
      metadata.className,
      metadata.requestArgs,
      Some(metadata),
      fromRecovery = true)
  }

  def reassignBatchSessions(
      kyuubiInstance: String,
      newKyuubiInstance: String): Seq[String] = {
    Seq(OperationState.PENDING, OperationState.RUNNING).flatMap { stateToRecover =>
      metadataManager.map(_.getBatchesRecoveryMetadata(
        stateToRecover.toString,
        kyuubiInstance,
        0,
        Int.MaxValue).map { metadata =>
        updateMetadata(Metadata(
          identifier = metadata.identifier,
          kyuubiInstance = newKyuubiInstance))
        info(s"Reassign batch ${metadata.identifier} from $kyuubiInstance to $newKyuubiInstance")
        metadata.identifier
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
    val userUnlimitedList =
      conf.get(SERVER_LIMIT_CONNECTIONS_USER_UNLIMITED_LIST).filter(_.nonEmpty)
    val userDenyList = conf.get(SERVER_LIMIT_CONNECTIONS_USER_DENY_LIST).filter(_.nonEmpty)
    val ipDenyList = conf.get(SERVER_LIMIT_CONNECTIONS_IP_DENY_LIST).filter(_.nonEmpty)
    limiter = applySessionLimiter(
      userLimit,
      ipAddressLimit,
      userIpAddressLimit,
      userUnlimitedList,
      userDenyList,
      ipDenyList)

    val userBatchLimit = conf.get(SERVER_LIMIT_BATCH_CONNECTIONS_PER_USER).getOrElse(0)
    val ipAddressBatchLimit = conf.get(SERVER_LIMIT_BATCH_CONNECTIONS_PER_IPADDRESS).getOrElse(0)
    val userIpAddressBatchLimit =
      conf.get(SERVER_LIMIT_BATCH_CONNECTIONS_PER_USER_IPADDRESS).getOrElse(0)
    batchLimiter = applySessionLimiter(
      userBatchLimit,
      ipAddressBatchLimit,
      userIpAddressBatchLimit,
      userUnlimitedList,
      userDenyList,
      ipDenyList)
  }

  private[kyuubi] def getUnlimitedUsers: Set[String] = {
    limiter.orElse(batchLimiter).map(SessionLimiter.getUnlimitedUsers).getOrElse(Set.empty)
  }

  private[kyuubi] def refreshUnlimitedUsers(conf: KyuubiConf): Unit = {
    val unlimitedUsers =
      conf.get(SERVER_LIMIT_CONNECTIONS_USER_UNLIMITED_LIST).filter(_.nonEmpty)
    limiter.foreach(SessionLimiter.resetUnlimitedUsers(_, unlimitedUsers))
    batchLimiter.foreach(SessionLimiter.resetUnlimitedUsers(_, unlimitedUsers))
  }

  private[kyuubi] def getDenyUsers: Set[String] = {
    limiter.orElse(batchLimiter).map(SessionLimiter.getDenyUsers).getOrElse(Set.empty)
  }

  private[kyuubi] def refreshDenyUsers(conf: KyuubiConf): Unit = {
    val denyUsers = conf.get(SERVER_LIMIT_CONNECTIONS_USER_DENY_LIST).filter(_.nonEmpty)
    limiter.foreach(SessionLimiter.resetDenyUsers(_, denyUsers))
    batchLimiter.foreach(SessionLimiter.resetDenyUsers(_, denyUsers))
  }

  private[kyuubi] def getDenyIps: Set[String] = {
    limiter.orElse(batchLimiter).map(SessionLimiter.getDenyIps).getOrElse(Set.empty)
  }

  private[kyuubi] def refreshDenyIps(conf: KyuubiConf): Unit = {
    val denyIps = conf.get(SERVER_LIMIT_CONNECTIONS_IP_DENY_LIST).filter(_.nonEmpty)
    limiter.foreach(SessionLimiter.resetDenyIps(_, denyIps))
    batchLimiter.foreach(SessionLimiter.resetDenyIps(_, denyIps))
  }

  private def applySessionLimiter(
      userLimit: Int,
      ipAddressLimit: Int,
      userIpAddressLimit: Int,
      userUnlimitedList: Set[String],
      userDenyList: Set[String],
      ipDenyList: Set[String]): Option[SessionLimiter] = {
    if (Seq(userLimit, ipAddressLimit, userIpAddressLimit).exists(_ > 0) ||
      userDenyList.nonEmpty || ipDenyList.nonEmpty) {
      Some(SessionLimiter(
        userLimit,
        ipAddressLimit,
        userIpAddressLimit,
        userUnlimitedList,
        userDenyList,
        ipDenyList))
    } else {
      None
    }
  }

  private def startEngineAliveChecker(): Unit = {
    val interval = conf.get(KyuubiConf.ENGINE_ALIVE_PROBE_INTERVAL)
    val checkTask: Runnable = () => {
      allSessions().foreach {
        case session: KyuubiSessionImpl =>
          try {
            if (!session.checkEngineConnectionAlive()) {
              closeSession(session.handle)
              logger.info(s"The session ${session.handle} has been closed " +
                s"due to engine unresponsiveness (checked by the engine alive checker).")
            }
          } catch {
            case e: Throwable => warn(s"Error closing session ${session.handle}", e)
          }
        case _ =>
      }
    }
    scheduleTolerableRunnableWithFixedDelay(
      engineConnectionAliveChecker,
      checkTask,
      interval,
      interval,
      TimeUnit.MILLISECONDS)
  }

  private def initEngineStartupProcessSemaphore(conf: KyuubiConf): Unit = {
    val engineCreationLimit = conf.get(KyuubiConf.SERVER_LIMIT_ENGINE_CREATION)
    engineCreationLimit.filter(_ > 0).foreach { limit =>
      engineStartupProcessSemaphore = Some(new Semaphore(limit))
    }
  }
}
