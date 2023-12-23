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

import java.util.concurrent._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials

import org.apache.kyuubi.{Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.{KyuubiHadoopUtils, ThreadUtils}
import org.apache.kyuubi.util.ThreadUtils.scheduleTolerableRunnableWithFixedDelay
import org.apache.kyuubi.util.reflect.ReflectUtils._

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

  private[credentials] val userCredentialsRefMap = new ConcurrentHashMap[String, CredentialsRef]()
  private val sessionCredentialsEpochMap = new ConcurrentHashMap[String, Long]()

  private var providers: Map[String, HadoopDelegationTokenProvider] = _
  private var renewalInterval: Long = _
  private var renewalRetryWait: Long = _
  private var credentialsWaitTimeout: Long = _
  private var hadoopConf: Configuration = _

  private var credentialsCheckInterval: Long = _
  private var credentialsTimeout: Long = _

  private[credentials] var renewalExecutor: Option[ScheduledExecutorService] = None
  private[credentials] var credentialsTimeoutChecker: Option[ScheduledExecutorService] = None

  override def initialize(conf: KyuubiConf): Unit = {
    hadoopConf = KyuubiHadoopUtils.newHadoopConf(conf)
    providers = HadoopCredentialsManager.loadProviders(conf)
      .filter { case (_, provider) =>
        provider.initialize(hadoopConf, conf)
        val required = provider.delegationTokensRequired()
        if (!required) {
          warn(s"Service ${provider.serviceName} does not require a token." +
            s" Check your configuration to see if security is disabled or not." +
            s" If security is enabled, some configurations of ${provider.serviceName} " +
            s" might be missing, please check the configurations in " +
            s" https://kyuubi.readthedocs.io/en/master/security" +
            s"/hadoop_credentials_manager.html#required-security-configs")
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
    credentialsWaitTimeout = conf.get(CREDENTIALS_UPDATE_WAIT_TIMEOUT)

    credentialsCheckInterval = conf.get(CREDENTIALS_CHECK_INTERVAL)
    credentialsTimeout = conf.get(CREDENTIALS_IDLE_TIMEOUT)

    super.initialize(conf)
  }

  override def start(): Unit = {
    if (providers.nonEmpty) {
      renewalExecutor =
        Some(ThreadUtils.newDaemonSingleThreadScheduledExecutor("Delegation Token Renewal Thread"))

      credentialsTimeoutChecker =
        Some(ThreadUtils.newDaemonSingleThreadScheduledExecutor("User Credentials Timeout Checker"))
      startTimeoutChecker()
    }
    super.start()
  }

  override def stop(): Unit = {
    providers.values.foreach(_.close())
    renewalExecutor.foreach { executor =>
      ThreadUtils.shutdown(executor, Duration(10, TimeUnit.SECONDS))
    }
    credentialsTimeoutChecker.foreach { executor =>
      ThreadUtils.shutdown(executor, Duration(10, TimeUnit.SECONDS))
    }
    super.stop()
  }

  def renewCredentials(appUser: String): String = {
    if (renewalExecutor.isEmpty) {
      return ""
    }
    val userRef = getOrCreateUserCredentialsRef(appUser, true)
    userRef.getEncodedCredentials
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
  private[credentials] def getOrCreateUserCredentialsRef(
      appUser: String,
      waitUntilCredentialsReady: Boolean = false): CredentialsRef = {
    val ref = userCredentialsRefMap.computeIfAbsent(
      appUser,
      appUser => {
        val ref = new CredentialsRef(appUser)
        val credentialsFuture: Future[Unit] = scheduleRenewal(ref, 0, waitUntilCredentialsReady)
        ref.setFuture(credentialsFuture)
        info(s"Created CredentialsRef for user $appUser and scheduled a renewal task")
        ref
      })
    ref.updateLastAccessTime()

    if (waitUntilCredentialsReady) {
      ref.waitUntilReady(Duration(credentialsWaitTimeout, TimeUnit.MILLISECONDS))
    }

    ref
  }

  // Visible for testing.
  private[credentials] def getSessionCredentialsEpoch(sessionId: String): Long = {
    sessionCredentialsEpochMap.getOrDefault(sessionId, CredentialsRef.UNSET_EPOCH)
  }

  // Visible for testing.
  private[credentials] def containsProvider(serviceName: String): Boolean = {
    providers.contains(serviceName)
  }

  private def updateCredentials(userRef: CredentialsRef): Unit = {
    val creds = new Credentials()
    providers.values
      .foreach(_.obtainDelegationTokens(userRef.getAppUser, creds))
    userRef.updateCredentials(creds)
  }

  private def scheduleRenewal(
      userRef: CredentialsRef,
      delay: Long,
      waitUntilCredentialsReady: Boolean = false): Future[Unit] = {
    val promise = Promise[Unit]()

    val renewalTask = new Runnable {
      override def run(): Unit = {
        try {
          promise.trySuccess(updateCredentials(userRef))

          if (userRef.getNoOperationTime < credentialsTimeout) {
            scheduleRenewal(userRef, renewalInterval)
          }
        } catch {
          case _: InterruptedException =>
          // Server is shutting down
          case e: Exception =>
            warn(
              s"Failed to update tokens for ${userRef.getAppUser}, try again in" +
                s" $renewalRetryWait ms",
              e)
            if (userRef.getNoOperationTime < credentialsTimeout) {
              scheduleRenewal(userRef, renewalRetryWait)
            }

            if (waitUntilCredentialsReady) {
              promise.tryFailure(e)
            }
        }
      }
    }

    renewalExecutor.foreach { executor =>
      info(s"Scheduling renewal in $delay ms.")
      executor.schedule(renewalTask, delay, TimeUnit.MILLISECONDS)
    }

    promise.future
  }

  private def startTimeoutChecker(): Unit = {
    val checkTask = new Runnable {
      override def run(): Unit = {
        for ((user, userCred) <- userCredentialsRefMap.asScala) {
          if (userCred.getNoOperationTime >= credentialsTimeout) {
            userCredentialsRefMap.remove(user)
          }
        }
      }
    }

    credentialsTimeoutChecker.foreach { executor =>
      scheduleTolerableRunnableWithFixedDelay(
        executor,
        checkTask,
        credentialsCheckInterval,
        credentialsCheckInterval,
        TimeUnit.MILLISECONDS)
    }
  }

}

object HadoopCredentialsManager extends Logging {

  private val providerEnabledConfig = "kyuubi.credentials.%s.enabled"

  def loadProviders(kyuubiConf: KyuubiConf): Map[String, HadoopDelegationTokenProvider] = {
    val providers = mutable.ArrayBuffer[HadoopDelegationTokenProvider]()

    val iterator =
      loadFromServiceLoader[HadoopDelegationTokenProvider](Utils.getContextOrKyuubiClassLoader)
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
