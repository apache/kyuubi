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

package org.apache.kyuubi.engine

import java.util.concurrent.{Semaphore, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.Random

import com.codahale.metrics.MetricRegistry
import com.google.common.annotations.VisibleForTesting
import org.apache.commons.lang3.StringUtils

import org.apache.kyuubi.{KYUUBI_VERSION, KyuubiSQLException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_ENGINE_SUBMIT_TIME_KEY
import org.apache.kyuubi.engine.EngineType._
import org.apache.kyuubi.engine.ShareLevel.{CONNECTION, GROUP, SERVER, SERVER_LOCAL, ShareLevel}
import org.apache.kyuubi.engine.chat.ChatProcessBuilder
import org.apache.kyuubi.engine.flink.FlinkProcessBuilder
import org.apache.kyuubi.engine.hive.HiveProcessBuilder
import org.apache.kyuubi.engine.jdbc.JdbcProcessBuilder
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.engine.trino.TrinoProcessBuilder
import org.apache.kyuubi.ha.HighAvailabilityConf.{HA_ENGINE_REF_ID, HA_NAMESPACE}
import org.apache.kyuubi.ha.client.{DiscoveryClient, DiscoveryClientProvider, DiscoveryPaths, ServiceNodeInfo}
import org.apache.kyuubi.metrics.MetricsConstants.{ENGINE_FAIL, ENGINE_TIMEOUT, ENGINE_TOTAL}
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.operation.log.OperationLog
import org.apache.kyuubi.plugin.GroupProvider
import org.apache.kyuubi.util.JavaUtils

/**
 * The description and functionality of an engine at server side
 *
 * @param conf Engine configuration
 * @param sessionUser Caller of the engine
 * @param engineRefId Id of the corresponding session in which the engine is created
 */
private[kyuubi] class EngineRef(
    conf: KyuubiConf,
    sessionUser: String,
    doAsEnabled: Boolean,
    groupProvider: GroupProvider,
    engineRefId: String,
    engineManager: KyuubiApplicationManager,
    startupProcessSemaphore: Option[Semaphore] = None)
  extends Logging {
  // The corresponding ServerSpace where the engine belongs to
  private val serverSpace: String = conf.get(HA_NAMESPACE)

  private val timeout: Long = conf.get(ENGINE_INIT_TIMEOUT)

  // Share level of the engine
  private val shareLevel: ShareLevel = ShareLevel.withName(conf.get(ENGINE_SHARE_LEVEL))

  private val engineType: EngineType = EngineType.withName(conf.get(ENGINE_TYPE))

  // Server-side engine pool size threshold
  private val poolThreshold: Int =
    Option(engineManager).map(_.getConf).getOrElse(KyuubiConf()).get(ENGINE_POOL_SIZE_THRESHOLD)

  private val clientPoolSize: Int = conf.get(ENGINE_POOL_SIZE)

  private val clientPoolName: String = conf.get(ENGINE_POOL_NAME)

  private val enginePoolIgnoreSubdomain: Boolean = conf.get(ENGINE_POOL_IGNORE_SUBDOMAIN)

  private val enginePoolSelectPolicy: String = conf.get(ENGINE_POOL_SELECT_POLICY)

  private lazy val localHostAddr = {
    val host = JavaUtils.findLocalInetAddress.getHostAddress
    if (StringUtils.isBlank(host)) {
      throw KyuubiSQLException(
        s"Local host address can not be empty if ShareLevel set to SERVER_LOCAL")
    }
    host
  }

  // In case the multi kyuubi instances have the small gap of timeout, here we add
  // a small amount of time for timeout
  private val LOCK_TIMEOUT_SPAN_FACTOR = if (Utils.isTesting) 0.5 else 0.1

  private var builder: ProcBuilder = _

  private[kyuubi] def getEngineRefId: String = engineRefId

  // user for routing session to the engine
  private[kyuubi] val routingUser: String = shareLevel match {
    case SERVER | SERVER_LOCAL => Utils.currentUser
    case GROUP => groupProvider.primaryGroup(sessionUser, conf.getAll.asJava)
    case _ => sessionUser
  }

  // user for launching engine
  private[kyuubi] val appUser: String = if (doAsEnabled) routingUser else Utils.currentUser

  @VisibleForTesting
  private[kyuubi] val subdomain: String = conf.get(ENGINE_SHARE_LEVEL_SUBDOMAIN) match {
    case subdomain if clientPoolSize > 0 && (subdomain.isEmpty || enginePoolIgnoreSubdomain) =>
      val poolSize = math.min(clientPoolSize, poolThreshold)
      if (poolSize < clientPoolSize) {
        warn(s"Request engine pool size($clientPoolSize) exceeds, fallback to " +
          s"system threshold $poolThreshold")
      }
      val seqNum = enginePoolSelectPolicy match {
        case "POLLING" =>
          val snPath =
            DiscoveryPaths.makePath(
              s"${serverSpace}_${KYUUBI_VERSION}_${shareLevel}_${engineType}_seqNum",
              routingUser,
              clientPoolName)
          DiscoveryClientProvider.withDiscoveryClient(conf) { client =>
            client.getAndIncrement(snPath)
          }
        case "RANDOM" =>
          Random.nextInt(poolSize)
      }
      s"$clientPoolName-${seqNum % poolSize}"
    case Some(_subdomain) => _subdomain
    case _ => "default" // [KYUUBI #1293]
  }

  /**
   * The default engine name, used as default `spark.app.name` if not set
   */
  @VisibleForTesting
  private[kyuubi] val defaultEngineName: String = {
    val commonNamePrefix = s"kyuubi_${shareLevel}_${engineType}_${routingUser}"
    shareLevel match {
      case CONNECTION => s"${commonNamePrefix}_$engineRefId"
      case SERVER_LOCAL => s"${commonNamePrefix}_${localHostAddr}_${subdomain}_$engineRefId"
      case _ => s"${commonNamePrefix}_${subdomain}_$engineRefId"
    }
  }

  /**
   * The EngineSpace used to expose itself to the KyuubiServers in `serverSpace`
   *
   * For `CONNECTION` share level:
   *   /`serverSpace_version_CONNECTION_engineType`/`user`/`engineRefId`
   * For `USER` share level:
   *   /`serverSpace_version_USER_engineType`/`user`[/`subdomain`]
   * For `GROUP` share level:
   *   /`serverSpace_version_GROUP_engineType`/`primary group name`[/`subdomain`]
   * For `SERVER_LOCAL` share level:
   *   /`serverSpace_version_SERVER_LOCAL_engineType`/`kyuubi server user`/`hostAddress_subdomain`
   * For `SERVER` share level:
   *   /`serverSpace_version_SERVER_engineType`/`kyuubi server user`[/`subdomain`]
   */
  @VisibleForTesting
  private[kyuubi] lazy val engineSpace: String = {
    val commonParent = s"${serverSpace}_${KYUUBI_VERSION}_${shareLevel}_$engineType"
    shareLevel match {
      case CONNECTION => DiscoveryPaths.makePath(commonParent, routingUser, engineRefId)
      case SERVER_LOCAL =>
        DiscoveryPaths.makePath(commonParent, routingUser, s"${localHostAddr}_$subdomain")
      case _ => DiscoveryPaths.makePath(commonParent, routingUser, subdomain)
    }
  }

  /**
   * The distributed lock path used to ensure only once engine being created for non-CONNECTION
   * share level.
   */
  private def tryWithLock[T](discoveryClient: DiscoveryClient)(f: => T): T =
    shareLevel match {
      case CONNECTION => f
      case SERVER_LOCAL =>
        val lockPath =
          DiscoveryPaths.makePath(
            s"${serverSpace}_${KYUUBI_VERSION}_${shareLevel}_${engineType}_lock",
            routingUser,
            s"${localHostAddr}_$subdomain")
        discoveryClient.tryWithLock(
          lockPath,
          timeout + (LOCK_TIMEOUT_SPAN_FACTOR * timeout).toLong)(f)
      case _ =>
        val lockPath =
          DiscoveryPaths.makePath(
            s"${serverSpace}_${KYUUBI_VERSION}_${shareLevel}_${engineType}_lock",
            routingUser,
            subdomain)
        discoveryClient.tryWithLock(
          lockPath,
          timeout + (LOCK_TIMEOUT_SPAN_FACTOR * timeout).toLong)(f)
    }

  private def create(
      discoveryClient: DiscoveryClient,
      extraEngineLog: Option[OperationLog]): (String, Int) = tryWithLock(discoveryClient) {
    // Get the engine address ahead if another process has succeeded
    var engineRef = discoveryClient.getServerHost(engineSpace)
    if (engineRef.nonEmpty) return engineRef.get

    conf.set(HA_NAMESPACE, engineSpace)
    conf.set(HA_ENGINE_REF_ID, engineRefId)
    val started = System.currentTimeMillis()
    conf.set(KYUUBI_ENGINE_SUBMIT_TIME_KEY, String.valueOf(started))
    builder = engineType match {
      case SPARK_SQL =>
        conf.setIfMissing(SparkProcessBuilder.APP_KEY, defaultEngineName)
        new SparkProcessBuilder(appUser, doAsEnabled, conf, engineRefId, extraEngineLog)
      case FLINK_SQL =>
        conf.setIfMissing(FlinkProcessBuilder.APP_KEY, defaultEngineName)
        new FlinkProcessBuilder(appUser, doAsEnabled, conf, engineRefId, extraEngineLog)
      case TRINO =>
        new TrinoProcessBuilder(appUser, doAsEnabled, conf, engineRefId, extraEngineLog)
      case HIVE_SQL =>
        conf.setIfMissing(HiveProcessBuilder.HIVE_ENGINE_NAME, defaultEngineName)
        HiveProcessBuilder(
          appUser,
          doAsEnabled,
          conf,
          engineRefId,
          extraEngineLog,
          defaultEngineName)
      case JDBC =>
        conf.setIfMissing(JdbcProcessBuilder.JDBC_ENGINE_NAME, defaultEngineName)
        JdbcProcessBuilder(
          appUser,
          doAsEnabled,
          conf,
          engineRefId,
          extraEngineLog,
          defaultEngineName)
      case CHAT =>
        new ChatProcessBuilder(appUser, doAsEnabled, conf, engineRefId, extraEngineLog)
    }

    MetricsSystem.tracing(_.incCount(ENGINE_TOTAL))
    var acquiredPermit = false
    try {
      if (!startupProcessSemaphore.forall(_.tryAcquire(timeout, TimeUnit.MILLISECONDS))) {
        MetricsSystem.tracing(_.incCount(MetricRegistry.name(ENGINE_TIMEOUT, appUser)))
        throw KyuubiSQLException(
          s"Timeout($timeout ms, you can modify ${ENGINE_INIT_TIMEOUT.key} to change it) to" +
            s" acquires a permit from engine builder semaphore.")
      }
      acquiredPermit = true
      val redactedCmd = builder.toString
      info(s"Launching engine:\n$redactedCmd")
      builder.validateConf()
      val process = builder.start
      var exitValue: Option[Int] = None
      var lastApplicationInfo: Option[ApplicationInfo] = None
      while (engineRef.isEmpty) {
        if (exitValue.isEmpty && process.waitFor(1, TimeUnit.SECONDS)) {
          exitValue = Some(process.exitValue())
          if (exitValue.contains(0)) {
            acquiredPermit = false
            startupProcessSemaphore.foreach(_.release())
          } else {
            val error = builder.getError
            MetricsSystem.tracing { ms =>
              ms.incCount(MetricRegistry.name(ENGINE_FAIL, appUser))
              ms.incCount(MetricRegistry.name(ENGINE_FAIL, error.getClass.getSimpleName))
            }
            throw error
          }
        }

        if (started + timeout <= System.currentTimeMillis()) {
          val killMessage =
            engineManager.killApplication(builder.appMgrInfo(), engineRefId, Some(appUser))
          builder.close(true)
          MetricsSystem.tracing(_.incCount(MetricRegistry.name(ENGINE_TIMEOUT, appUser)))
          throw KyuubiSQLException(
            s"Timeout($timeout ms, you can modify ${ENGINE_INIT_TIMEOUT.key} to change it) to" +
              s" launched $engineType engine with $redactedCmd. $killMessage",
            builder.getError)
        }
        engineRef = discoveryClient.getEngineByRefId(engineSpace, engineRefId)

        // even the submit process succeeds, the application might meet failure when initializing,
        // check the engine application state from engine manager and fast fail on engine terminate
        if (engineRef.isEmpty && exitValue.contains(0)) {
          Option(engineManager).foreach { engineMgr =>
            if (lastApplicationInfo.isDefined) {
              TimeUnit.SECONDS.sleep(1)
            }

            val applicationInfo = engineMgr.getApplicationInfo(
              builder.appMgrInfo(),
              engineRefId,
              Some(appUser),
              Some(started))

            applicationInfo.foreach { appInfo =>
              if (ApplicationState.isTerminated(appInfo.state)) {
                MetricsSystem.tracing { ms =>
                  ms.incCount(MetricRegistry.name(ENGINE_FAIL, appUser))
                  ms.incCount(MetricRegistry.name(ENGINE_FAIL, "ENGINE_TERMINATE"))
                }
                throw new KyuubiSQLException(
                  s"""
                     |The engine application has been terminated. Please check the engine log.
                     |ApplicationInfo: ${appInfo.toMap.mkString("(\n", ",\n", "\n)")}
                     |""".stripMargin,
                  builder.getError)
              }
            }

            lastApplicationInfo = applicationInfo
          }
        }
      }
      engineRef.get
    } finally {
      if (acquiredPermit) startupProcessSemaphore.foreach(_.release())
      val destroyProcess = !builder.waitEngineCompletion
      if (destroyProcess) {
        info("Destroy the builder process because waitCompletion is false" +
          " and the engine is running in cluster mode.")
      }
      // we must close the process builder whether session open is success or failure since
      // we have a log capture thread in process builder.
      builder.close(destroyProcess)
    }
  }

  /**
   * Get the engine ref from engine space first or create a new one
   *
   * @param discoveryClient the zookeeper client to get or create engine instance
   * @param extraEngineLog the launch engine operation log, used to inject engine log into it
   * @return engine host and port
   */
  def getOrCreate(
      discoveryClient: DiscoveryClient,
      extraEngineLog: Option[OperationLog] = None): (String, Int) = {
    discoveryClient.getServerHost(engineSpace)
      .getOrElse {
        create(discoveryClient, extraEngineLog)
      }
  }

  /**
   * Deregister the engine from engine space with the given host and port on connection failure.
   *
   * @param discoveryClient the zookeeper client to get or create engine instance
   * @param hostPort the existing engine host and port
   * @return deregister result and message
   */
  def deregister(discoveryClient: DiscoveryClient, hostPort: (String, Int)): (Boolean, String) =
    tryWithLock(discoveryClient) {
      // refer the DiscoveryClient::getServerHost implementation
      discoveryClient.getServiceNodesInfo(engineSpace, Some(1), silent = true) match {
        case Seq(sn) =>
          if ((sn.host, sn.port) == hostPort) {
            val msg = s"Deleting engine node:$sn"
            info(msg)
            discoveryClient.delete(s"$engineSpace/${sn.nodeName}")

            if (shareLevel != CONNECTION && builder != null) {
              try {
                val appMgrInfo = builder.appMgrInfo()
                engineManager.killApplication(appMgrInfo, engineRefId, Some(appUser))
                info(s"Killed engine process for $engineRefId with share level $shareLevel")
              } catch {
                case e: Exception =>
                  warn(s"Error killing engine process for $engineRefId", e)
              }
            }

            (true, msg)
          } else {
            val msg = s"Engine node:$sn is not matched with host&port[$hostPort]"
            warn(msg)
            (false, msg)
          }
        case _ =>
          val msg = s"No engine node found in $engineSpace"
          warn(msg)
          (false, msg)
      }
    }

  def getServiceNode(
      discoveryClient: DiscoveryClient,
      hostPort: (String, Int)): Option[ServiceNodeInfo] = {
    val serviceNodes = discoveryClient.getServiceNodesInfo(engineSpace)
    serviceNodes.find { sn => (sn.host, sn.port) == hostPort }
  }

  def close(): Unit = {
    if (shareLevel == CONNECTION && builder != null) {
      try {
        val appMgrInfo = builder.appMgrInfo()
        builder.close(true)
        engineManager.killApplication(appMgrInfo, engineRefId, Some(appUser))
      } catch {
        case e: Exception =>
          warn(s"Error closing engine builder, engineRefId: $engineRefId", e)
      }
    }
  }
}
