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

package org.apache.kyuubi.security

import java.time.Duration
import java.util.ServiceLoader
import java.util.concurrent._

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.util.ThreadUtils

class HadoopDelegationTokenManager(kyuubiConf: KyuubiConf, hadoopConf: Configuration)
    extends Logging {

  private val delegationTokenProviders = loadProviders()
  debug("Using the following builtin delegation token providers: " +
    s"${delegationTokenProviders.keys.mkString(", ")}.")

  private val credentialsMap = new ConcurrentHashMap[String, CredentialsRef]()
  private val renewalInterval = kyuubiConf.get(CREDENTIALS_RENEWAL_INTERVAL)
  private val renewalRetryWait = kyuubiConf.get(CREDENTIALS_RENEWAL_RETRY_WAIT)
  private var renewalExecutor: ScheduledExecutorService = _

  def start(): Unit = {
    renewalExecutor =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("Delegation Token Renewal Thread")
  }

  def stop(): Unit = {
    if (renewalExecutor != null) {
      renewalExecutor.shutdownNow()
      try {
        renewalExecutor.awaitTermination(10, TimeUnit.SECONDS)
      } catch {
        case _: InterruptedException =>
      }
    }
  }

  def obtainDelegationTokensRef(appUser: String): CredentialsRef = {
    require(renewalExecutor != null, "renewalExecutor should be initialized")

    val credsRef = credentialsMap.computeIfAbsent(appUser, appUser => new CredentialsRef(appUser))

    if (credsRef.getEpoch == -1) {
      credsRef synchronized {
        if (credsRef.getEpoch == -1) {
          val task = new Runnable {
            override def run(): Unit = {
              credsRef.updateCredentials(obtainDelegationTokens(appUser))
              scheduleRenewal(credsRef, renewalInterval)
            }
          }
          renewalExecutor.schedule(task, 0, TimeUnit.MILLISECONDS).get()
        }
      }
    }
    credsRef
  }

  // Visible for testing.
  def isProviderLoaded(serviceName: String): Boolean = {
    delegationTokenProviders.contains(serviceName)
  }

  private def scheduleRenewal(credentialsRef: CredentialsRef, delay: Long): Future[_] = {
    val _delay = math.max(0, delay)
    info(s"Scheduling renewal in ${Duration.ofMillis(_delay)}.")

    val renewalTask = new Runnable {
      override def run(): Unit = {
        try {
          val creds = obtainDelegationTokens(credentialsRef.getAppUser)
          credentialsRef.updateCredentials(creds)
          scheduleRenewal(credentialsRef, renewalInterval)
        } catch {
          case _: InterruptedException =>
          // Server is shutting down
          case e: Exception =>
            val duration = Duration.ofMillis(renewalRetryWait)
            warn(s"Failed to update tokens, will try again in ${duration}", e)
            scheduleRenewal(credentialsRef, renewalRetryWait)
        }
      }
    }

    renewalExecutor.schedule(renewalTask, _delay, TimeUnit.MILLISECONDS)
  }

  private def obtainDelegationTokens(appUser: String): Credentials = {
    val creds = new Credentials()
    delegationTokenProviders.values
      .foreach { provider =>
        if (provider.delegationTokensRequired(hadoopConf)) {
          provider.obtainDelegationTokens(hadoopConf, kyuubiConf, appUser, creds)
        } else {
          debug(s"Service ${provider.serviceName} does not require a token." +
            s" Check your configuration to see if security is disabled or not.")
        }
      }
    creds
  }

  private def loadProviders(): Map[String, HadoopDelegationTokenProvider] = {
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

    // Filter out providers for which kyuubi.security.credentials.{service}.enabled is false.
    providers
      .filter { p => HadoopDelegationTokenManager.isServiceEnabled(kyuubiConf, p.serviceName) }
      .map { p => (p.serviceName, p) }
      .toMap
  }

}

object HadoopDelegationTokenManager {
  private val providerEnabledConfig = "kyuubi.security.credentials.%s.enabled"

  def isServiceEnabled(kyuubiConf: KyuubiConf, serviceName: String): Boolean = {
    val key = providerEnabledConfig.format(serviceName)
    kyuubiConf
      .getOption(key)
      .map(_.toBoolean)
      .getOrElse(true)
  }

}
