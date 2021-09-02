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

package org.apache.kyuubi.credentials

import java.time.Duration
import java.util.ServiceLoader
import java.util.concurrent._

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials

import org.apache.kyuubi.{KyuubiException, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.{KyuubiHadoopUtils, ThreadUtils}

class HadoopCredentialsManager private (name: String) extends AbstractService(name)
    with Logging {

  def this() = this(classOf[HadoopCredentialsManager].getSimpleName)

  private val userRefMap = new ConcurrentHashMap[String, UserCredentialsRef]()

  private var engineRefCache: LoadingCache[String, EngineCredentialsRef] = _

  private var providers: Map[String, HadoopDelegationTokenProvider] = _
  private var requiredProviders: Map[String, HadoopDelegationTokenProvider] = _

  private var renewalInterval: Long = _
  private var renewalRetryWait: Long = _
  private var renewalExecutor: ScheduledExecutorService = _
  private var hadoopConf: Configuration = _

  override def initialize(conf: KyuubiConf): Unit = {
    val cacheLoader = new CacheLoader[String, EngineCredentialsRef] {
      override def load(key: String): EngineCredentialsRef = new EngineCredentialsRef
    }
    engineRefCache = CacheBuilder.newBuilder()
      .concurrencyLevel(conf.get(KyuubiConf.SERVER_EXEC_POOL_SIZE))
      .expireAfterAccess(conf.get(KyuubiConf.ENGINE_IDLE_TIMEOUT), TimeUnit.MILLISECONDS)
      .build(cacheLoader)

    hadoopConf = KyuubiHadoopUtils.newHadoopConf(conf)
    providers = loadProviders(conf)
    requiredProviders = providers.filter { case (_, provider) =>
      val required = provider.delegationTokensRequired(hadoopConf, conf)
      if (!required) {
        info(s"Service ${provider.serviceName} does not require a token." +
          s" Check your configuration to see if security is disabled or not.")
      }
      required
    }
    info("Using the following builtin delegation token providers: " +
      s"${providers.keys.mkString(", ")}.")

    renewalInterval = conf.get(CREDENTIALS_RENEWAL_INTERVAL)
    renewalRetryWait = conf.get(CREDENTIALS_RENEWAL_RETRY_WAIT)
    super.initialize(conf)
  }

  override def start(): Unit = {
    renewalExecutor =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("Delegation Token Renewal Thread")
    super.start()
  }

  override def stop(): Unit = {
    if (renewalExecutor != null) {
      renewalExecutor.shutdownNow()
      try {
        renewalExecutor.awaitTermination(10, TimeUnit.SECONDS)
      } catch {
        case _: InterruptedException =>
      }
    }
    super.stop()
  }

  /**
   * Send credentials to SQL engine if [[HadoopCredentialsManager]] had a newer version.
   * `appUser`'s credentials are created if not exist.
   * [[HadoopCredentialsManager]] takes care created credentials' renewal.
   * @param engineId SQL engine identifier
   * @param appUser  User identity that the SQL engine uses.
   * @param send     Function to send encoded credentials to SQL engine
   * @return `true` if credentials are sent successfully. Else, false.
   * @throws KyuubiException if sending credentials fails.
   */
  def sendCredentialsIfNeeded(
      engineId: String,
      appUser: String,
      send: String => Unit): Boolean = {
    require(renewalExecutor != null, "renewalExecutor should be initialized")

    val userRef = getUserRef(appUser)
    val engineRef = getEngineRef(engineId)

    var sent = false
    if (userRef.getEpoch != engineRef.getEpoch) {
      engineRef synchronized {
        if (userRef.getEpoch != engineRef.getEpoch) {
          val currentEpoch = userRef.getEpoch
          val currentCreds = userRef.getEncodedCredentials
          info(s"Send new credentials with epoch $currentEpoch to SQL engine $engineId")
          Try(send(currentCreds)) match {
            case Success(_) =>
              info(s"Update SQL engine epoch from ${engineRef.getEpoch} to $currentEpoch")
              engineRef.setEpoch(currentEpoch)
              sent = true
            case Failure(exception) =>
              throw new KyuubiException(
                s"Failed to send new credentials to SQL engine $engineId",
                exception)
          }
        }
      }
    }
    sent
  }

  // Visible for testing.
  private[credentials] def getUserRef(appUser: String): UserCredentialsRef =
    userRefMap.computeIfAbsent(
      appUser,
      appUser => {
        val ref = new UserCredentialsRef(appUser)
        scheduleRenewal(ref, 0)
        info(s"Created CredentialsRef for user $appUser and scheduled a renewal task")
        ref
      })

  // Visible for testing.
  private[credentials] def getEngineRef(engineId: String): EngineCredentialsRef =
    engineRefCache.getUnchecked(engineId)

  // Visible for testing.
  private[credentials] def isProviderLoaded(serviceName: String): Boolean =
    providers.contains(serviceName)

  // Visible for testing.
  private[credentials] def isProviderRequired(serviceName: String): Boolean =
    requiredProviders.contains(serviceName)

  private def scheduleRenewal(userRef: UserCredentialsRef, delay: Long): Future[_] = {
    val _delay = math.max(0, delay)
    info(s"Scheduling renewal in ${Duration.ofMillis(_delay)}.")

    val renewalTask = new Runnable {
      override def run(): Unit = {
        try {
          val creds = new Credentials()
          requiredProviders.values
            .foreach(_.obtainDelegationTokens(hadoopConf, conf, userRef.getAppUser, creds))
          userRef.updateCredentials(creds)
          scheduleRenewal(userRef, renewalInterval)
        } catch {
          case _: InterruptedException =>
          // Server is shutting down
          case e: Exception =>
            val duration = Duration.ofMillis(renewalRetryWait)
            warn(s"Failed to update tokens, will try again in ${duration}", e)
            scheduleRenewal(userRef, renewalRetryWait)
        }
      }
    }

    renewalExecutor.schedule(renewalTask, _delay, TimeUnit.MILLISECONDS)
  }

  private def loadProviders(kyuubiConf: KyuubiConf): Map[String, HadoopDelegationTokenProvider] = {
    val loader =
      ServiceLoader.load(classOf[HadoopDelegationTokenProvider], getClass.getClassLoader)
    val providers = mutable.ArrayBuffer[HadoopDelegationTokenProvider]()

    val iterator = loader.iterator
    while (iterator.hasNext) {
      try {
        providers += iterator.next
      } catch {
        case t: Throwable =>
          warn(s"Failed to load built in provider.", t)
      }
    }

    // Filter out providers for which kyuubi.credentials.{service}.enabled is false.
    providers
      .filter { p => HadoopCredentialsManager.isServiceEnabled(kyuubiConf, p.serviceName) }
      .map { p => (p.serviceName, p) }
      .toMap
  }

}

object HadoopCredentialsManager {
  private val providerEnabledConfig = "kyuubi.credentials.%s.enabled"

  def isServiceEnabled(kyuubiConf: KyuubiConf, serviceName: String): Boolean = {
    val key = providerEnabledConfig.format(serviceName)
    kyuubiConf
      .getOption(key)
      .map(_.toBoolean)
      .getOrElse(true)
  }

}
