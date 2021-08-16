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

import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.google.common.annotations.VisibleForTesting
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex
import org.apache.curator.utils.ZKPaths

import org.apache.kyuubi.{KyuubiSQLException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_INIT_TIMEOUT, ENGINE_SHARE_LEVEL, ENGINE_SHARE_LEVEL_SUB_DOMAIN}
import org.apache.kyuubi.engine.ShareLevel.{CONNECTION, SERVER, ShareLevel}
import org.apache.kyuubi.engine.spark.SparkProcessBuilder
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_ENGINE_SESSION_ID
import org.apache.kyuubi.ha.HighAvailabilityConf.HA_ZK_NAMESPACE
import org.apache.kyuubi.ha.client.ServiceDiscovery.getServerHost
import org.apache.kyuubi.metrics.MetricsConstants.{ENGINE_FAIL, ENGINE_TIMEOUT, ENGINE_TOTAL}
import org.apache.kyuubi.metrics.MetricsSystem
import org.apache.kyuubi.session.SessionHandle

/**
 * The description and functionality of an engine at server side
 *
 * @param conf Engine configuration
 * @param user Caller of the engine
 * @param sessionId Id of the corresponding session in which the engine is created
 */
private[kyuubi] class EngineRef private(conf: KyuubiConf, user: String, sessionId: String)
  extends Logging {

  // The corresponding ServerSpace where the engine belongs to
  private val serverSpace: String = conf.get(HA_ZK_NAMESPACE)

  private val timeout: Long = conf.get(ENGINE_INIT_TIMEOUT)

  // Share level of the engine
  private val shareLevel: ShareLevel = ShareLevel.withName(conf.get(ENGINE_SHARE_LEVEL))

  private val subDomain: Option[String] = conf.get(ENGINE_SHARE_LEVEL_SUB_DOMAIN)

  // Launcher of the engine
  private val appUser: String = shareLevel match {
    case SERVER => Utils.currentUser
    case _ => user
  }

  /**
   * The default engine name, used as default `spark.app.name` if not set
   */
  @VisibleForTesting
  private[kyuubi] val defaultEngineName: String = shareLevel match {
    case CONNECTION => s"kyuubi_${shareLevel}_${appUser}_$sessionId"
    case _ => subDomain match {
      case Some(domain) => s"kyuubi_${shareLevel}_${appUser}_${domain}_$sessionId"
      case _ => s"kyuubi_${shareLevel}_${appUser}_$sessionId"
    }
  }

  /**
   * The EngineSpace used to expose itself to the KyuubiServers in `serverSpace`
   *
   * For `CONNECTION` share level:
   *   /$serverSpace_CONNECTION/$user/$sessionId
   * For `USER` share level:
   *   /$serverSpace_USER/$user[/$subDomain]
   *
   */
  @VisibleForTesting
  private[kyuubi] lazy val engineSpace: String = shareLevel match {
    case CONNECTION => ZKPaths.makePath(s"${serverSpace}_$shareLevel", appUser, sessionId)
    case _ => subDomain match {
      case Some(domain) => ZKPaths.makePath(s"${serverSpace}_$shareLevel", appUser, domain)
      case None => ZKPaths.makePath(s"${serverSpace}_$shareLevel", appUser)
    }
  }

  /**
   * The distributed lock path used to ensure only once engine being created for non-CONNECTION
   * share level.
   */
  private def tryWithLock[T](zkClient: CuratorFramework)(f: => T): T = shareLevel match {
    case CONNECTION => f
    case _ =>
      val lockPath =
        ZKPaths.makePath(s"${serverSpace}_$shareLevel", "lock", appUser, subDomain.orNull)
      var lock: InterProcessSemaphoreMutex = null
      try {
        try {
          lock = new InterProcessSemaphoreMutex(zkClient, lockPath)
          // Acquire a lease. If no leases are available, this method blocks until either the
          // maximum number of leases is increased or another client/process closes a lease
          lock.acquire(timeout, TimeUnit.MILLISECONDS)
        } catch {
          case e: Exception => throw KyuubiSQLException(s"Lock failed on path [$lockPath]", e)
        }
        f
      } finally {
        try {
          if (lock != null) {
            lock.release()
          }
        } catch {
          case _: Exception =>
        }
      }
  }

  private def get(zkClient: CuratorFramework): Option[(String, Int)] = {
    getServerHost(zkClient, engineSpace)
      .filter(_.sessionId.exists(_.equals(sessionId)))
      .map(data => (data.host, data.port))
  }

  private def create(zkClient: CuratorFramework): (String, Int) = tryWithLock(zkClient) {
    var engineRef = get(zkClient)
    // Get the engine address ahead if another process has succeeded
    if (engineRef.nonEmpty) return engineRef.get

    conf.setIfMissing(SparkProcessBuilder.APP_KEY, defaultEngineName)
    // tag is a seq type with comma-separated
    conf.set(SparkProcessBuilder.TAG_KEY,
      conf.getOption(SparkProcessBuilder.TAG_KEY).map(_ + ",").getOrElse("") + "KYUUBI")
    conf.set(HA_ZK_NAMESPACE, engineSpace)
    conf.set(HA_ZK_ENGINE_SESSION_ID, sessionId)
    val builder = new SparkProcessBuilder(appUser, conf)
    MetricsSystem.tracing(_.incCount(ENGINE_TOTAL))
    try {
      info(s"Launching engine:\n$builder")
      val process = builder.start
      val started = System.currentTimeMillis()
      var exitValue: Option[Int] = None
      while (engineRef.isEmpty) {
        if (exitValue.isEmpty && process.waitFor(1, TimeUnit.SECONDS)) {
          exitValue = Some(process.exitValue())
          if (exitValue.get != 0) {
            val error = builder.getError
            MetricsSystem.tracing { ms =>
              ms.incCount(MetricRegistry.name(ENGINE_FAIL, appUser))
              ms.incCount(MetricRegistry.name(ENGINE_FAIL, error.getClass.getSimpleName))
            }
            throw error
          }
        }
        if (started + timeout <= System.currentTimeMillis()) {
          process.destroyForcibly()
          MetricsSystem.tracing(_.incCount(MetricRegistry.name(ENGINE_TIMEOUT, appUser)))
          throw KyuubiSQLException(
            s"Timeout($timeout ms) to launched Spark with $builder",
            builder.getError)
        }
        engineRef = get(zkClient)
      }
      engineRef.get
    } finally {
      // we must close the process builder whether session open is success or failure since
      // we have a log capture thread in process builder.
      builder.close()
    }
  }

  /**
   * Get the engine ref from engine space first first or create a new one
   */
  def getOrCreate(zkClient: CuratorFramework): (String, Int) = {
    getServerHost(zkClient, engineSpace)
      .map(data => (data.host, data.port))
      .getOrElse {
        create(zkClient)
      }
  }
}

private[kyuubi] object EngineRef {
  def apply(conf: KyuubiConf, user: String, handle: SessionHandle): EngineRef = {
    new EngineRef(conf, user, handle.identifier.toString)
  }
}
