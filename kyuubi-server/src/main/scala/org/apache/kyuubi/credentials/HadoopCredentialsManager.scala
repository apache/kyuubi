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

import java.util.ServiceLoader
import java.util.concurrent._

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.{KyuubiHadoopUtils, ThreadUtils}

/**
 * [[HadoopCredentialsManager]] manages and renews delegation tokens, which are used by SQL engines
 * to access kerberos secured services.
 *
 * Delegation tokens are sent to SQL engines by calling [[sendCredentialsIfNeeded]].
 * [[sendCredentialsIfNeeded]] executes the following steps:
 * <ol>
 * <li>
 *   Get or create a cached [[CredentialsRef]](contains delegation tokens) object by key
 *   appUser. If [[CredentialsRef]] is newly created, spawn a scheduled task to renew the
 *   delegation tokens.
 * </li>
 * <li>
 *   Get or create a cached session credentials epoch object by key sessionId.
 * </li>
 * <li>
 *   Compare [[CredentialsRef]] epoch with session credentials epoch. (Both epochs are set
 *   to -1 when created. [[CredentialsRef]] epoch is increased when delegation tokens are
 *   renewed.)
 * </li>
 * <li>
 *   If epochs are equal, return. Else, send delegation tokens to the SQL engine.
 * </li>
 * <li>
 *   If sending succeeds, set session credentials epoch to [[CredentialsRef]] epoch. Else,
 *   record the exception and return.
 * </li>
 * </ol>
 *
 * @note
 * <ol>
 * <li>
 *   Session credentials epochs are created in session scope and should be removed using
 *   [[removeSessionCredentialsEpoch]] when session closes.
 * </li>
 * <li>
 *   [[HadoopCredentialsManager]] does not renew and send credentials if no provider is left after
 *   initialize.
 * </li>
 * </ol>
 */
class HadoopCredentialsManager private (name: String) extends AbstractService(name)
    with Logging {

  def this() = this(classOf[HadoopCredentialsManager].getSimpleName)

  private val userCredentialsRefMap = new ConcurrentHashMap[String, CredentialsRef]()
  private val sessionCredentialsEpochMap = new ConcurrentHashMap[String, Long]()

  private var providers: Map[String, HadoopDelegationTokenProvider] = _
  private var renewalInterval: Long = _
  private var renewalRetryWait: Long = _
  private var hadoopConf: Configuration = _

  private[credentials] var renewalExecutor: Option[ScheduledExecutorService] = None

  override def initialize(conf: KyuubiConf): Unit = {
    hadoopConf = KyuubiHadoopUtils.newHadoopConf(conf)
    providers = HadoopCredentialsManager.loadProviders(conf)
      .filter { case (_, provider) =>
        provider.initialize(hadoopConf, conf)
        val required = provider.delegationTokensRequired()
        if (!required) {
          warn(s"Service ${provider.serviceName} does not require a token." +
            s" Check your configuration to see if security is disabled or not.")
          provider.close()
        }
        required
      }

    if (providers.isEmpty) {
      warn("No delegation token is required by services.")
    } else {
      info("Using the following builtin delegation token providers: " +
        s"${providers.keys.mkString(", ")}.")
    }

    renewalInterval = conf.get(CREDENTIALS_RENEWAL_INTERVAL)
    renewalRetryWait = conf.get(CREDENTIALS_RENEWAL_RETRY_WAIT)
    super.initialize(conf)
  }

  override def start(): Unit = {
    if (providers.nonEmpty) {
      renewalExecutor =
        Some(ThreadUtils.newDaemonSingleThreadScheduledExecutor("Delegation Token Renewal Thread"))
    }
    super.start()
  }

  override def stop(): Unit = {
    providers.values.foreach(_.close())
    renewalExecutor.foreach { executor =>
      executor.shutdownNow()
      try {
        executor.awaitTermination(10, TimeUnit.SECONDS)
      } catch {
        case _: InterruptedException =>
      }
    }
    super.stop()
  }

  /**
   * Send credentials to SQL engine which the specified session is talking to if
   * [[HadoopCredentialsManager]] has a newer credentials.
   *
   * @param sessionId Specify the session which is talking with SQL engine
   * @param appUser  User identity that the SQL engine uses.
   * @param send     Function to send encoded credentials to SQL engine
   */
  def sendCredentialsIfNeeded(
      sessionId: String,
      appUser: String,
      send: String => Unit): Unit = {
    if (renewalExecutor.isEmpty) {
      return
    }

    val userRef = getOrCreateUserCredentialsRef(appUser)
    val sessionEpoch = getSessionCredentialsEpoch(sessionId)

    if (userRef.getEpoch > sessionEpoch) {
      val currentEpoch = userRef.getEpoch
      val currentCreds = userRef.getEncodedCredentials
      info(s"Send new credentials with epoch $currentEpoch to SQL engine through session " +
        s"$sessionId")
      Try(send(currentCreds)) match {
        case Success(_) =>
          info(s"Update session credentials epoch from $sessionEpoch to $currentEpoch")
          sessionCredentialsEpochMap.put(sessionId, currentEpoch)
        case Failure(exception) =>
          warn(
            s"Failed to send new credentials to SQL engine through session $sessionId",
            exception)
      }
    }
  }

  /**
   * Remove session credentials epoch corresponding to `sessionId`.
   *
   * @param sessionId KyuubiSession id
   */
  def removeSessionCredentialsEpoch(sessionId: String): Unit = {
    sessionCredentialsEpochMap.remove(sessionId)
  }

  // Visible for testing.
  private[credentials] def getOrCreateUserCredentialsRef(appUser: String): CredentialsRef =
    userCredentialsRefMap.computeIfAbsent(
      appUser,
      appUser => {
        val ref = new CredentialsRef(appUser)
        scheduleRenewal(ref, 0)
        info(s"Created CredentialsRef for user $appUser and scheduled a renewal task")
        ref
      })

  // Visible for testing.
  private[credentials] def getSessionCredentialsEpoch(sessionId: String): Long = {
    sessionCredentialsEpochMap.getOrDefault(sessionId, CredentialsRef.UNSET_EPOCH)
  }

  // Visible for testing.
  private[credentials] def containsProvider(serviceName: String): Boolean = {
    providers.contains(serviceName)
  }

  private def scheduleRenewal(userRef: CredentialsRef, delay: Long): Unit = {
    val renewalTask = new Runnable {
      override def run(): Unit = {
        try {
          val creds = new Credentials()
          providers.values
            .foreach(_.obtainDelegationTokens(userRef.getAppUser, creds))
          userRef.updateCredentials(creds)
          scheduleRenewal(userRef, renewalInterval)
        } catch {
          case _: InterruptedException =>
          // Server is shutting down
          case e: Exception =>
            warn(
              s"Failed to update tokens for ${userRef.getAppUser}, try again in" +
                s" $renewalRetryWait ms",
              e)
            scheduleRenewal(userRef, renewalRetryWait)
        }
      }
    }

    renewalExecutor.foreach { executor =>
      info(s"Scheduling renewal in $delay ms.")
      executor.schedule(renewalTask, delay, TimeUnit.MILLISECONDS)
    }
  }

}

object HadoopCredentialsManager extends Logging {

  private val providerEnabledConfig = "kyuubi.credentials.%s.enabled"

  def loadProviders(kyuubiConf: KyuubiConf): Map[String, HadoopDelegationTokenProvider] = {
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

  def isServiceEnabled(kyuubiConf: KyuubiConf, serviceName: String): Boolean = {
    val key = providerEnabledConfig.format(serviceName)
    kyuubiConf
      .getOption(key)
      .map(_.toBoolean)
      .getOrElse(true)
  }

}
