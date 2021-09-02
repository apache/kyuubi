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

import java.io.IOException
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf

class HadoopCredentialsManagerSuite extends KyuubiFunSuite {

  private val engineId = UUID.randomUUID().toString
  private val appUser = "who"
  private val send = (_: String) => {}

  private def withStartedManager(kyuubiConf: KyuubiConf)(f: HadoopCredentialsManager => Unit)
      : Unit = {
    val manager = new HadoopCredentialsManager()
    manager.initialize(kyuubiConf)
    manager.start()

    try f(manager)
    finally manager.stop()
  }

  test("default configuration") {
    ExceptionThrowingDelegationTokenProvider.constructed = false
    val manager = new HadoopCredentialsManager()
    manager.initialize(new KyuubiConf(false))

    assert(manager.isProviderLoaded("unstable"))
    assert(manager.isProviderRequired("unstable"))
    assert(manager.isProviderLoaded("unrequired"))
    assert(!manager.isProviderRequired("unrequired"))
    // This checks that providers are loaded independently and they have no effect on each other
    assert(ExceptionThrowingDelegationTokenProvider.constructed)
    assert(!manager.isProviderLoaded("throw"))
  }

  test("disable unstable credential provider") {
    val kyuubiConf =
      new KyuubiConf(false).set("kyuubi.credentials.unstable.enabled", "false")
    val manager = new HadoopCredentialsManager()
    manager.initialize(kyuubiConf)
    assert(!manager.isProviderLoaded("unstable"))
  }

  test("schedule credentials renewal") {
    val kyuubiConf = new KyuubiConf(false)
      .set(KyuubiConf.CREDENTIALS_RENEWAL_INTERVAL, 1000L)
    withStartedManager(kyuubiConf) { manager =>
      val userRef = manager.getUserRef(appUser)
      // Tolerate 100 ms delay
      eventually(timeout(1100.milliseconds), interval(100.milliseconds)) {
        assert(userRef.getEpoch == 1)
      }
    }
  }

  test("schedule credentials renewal retry when failed") {
    val kyuubiConf = new KyuubiConf(false)
      .set(KyuubiConf.CREDENTIALS_RENEWAL_INTERVAL, 1000L)
      .set(KyuubiConf.CREDENTIALS_RENEWAL_RETRY_WAIT, 1000L)
    withStartedManager(kyuubiConf) { manager =>
      try {
        UnstableDelegationTokenProvider.throwException = true

        val userRef = manager.getUserRef(appUser)
        // Tolerate 100 ms delay
        eventually(timeout(2100.milliseconds), interval(100.milliseconds)) {
          // 1 scheduled call and 2 scheduled retrying call
          assert(UnstableDelegationTokenProvider.exceptionCount == 3)
        }
        assert(userRef.getEpoch == -1)
      } finally {
        UnstableDelegationTokenProvider.throwException = false
      }
    }
  }

  test("send credentials if needed") {
    val kyuubiConf = new KyuubiConf(false)
      .set(KyuubiConf.CREDENTIALS_RENEWAL_INTERVAL, 1000L)
    withStartedManager(kyuubiConf) { manager =>
      // Trigger UserCredentialsRef's initialization
      val userRef = manager.getUserRef(appUser)
      eventually(interval(100.milliseconds)) {
        assert(userRef.getEpoch == 0)
      }

      val succeed = manager.sendCredentialsIfNeeded(engineId, appUser, send)
      assert(succeed)

      val engineRef = manager.getEngineRef(engineId)
      assert(engineRef.getEpoch == userRef.getEpoch)
    }
  }

  test("credentials sending failure") {
    withStartedManager(new KyuubiConf(false)) { manager =>
      // Trigger UserCredentialsRef's initialization
      val userRef = manager.getUserRef(appUser)
      eventually(interval(100.milliseconds)) {
        assert(userRef.getEpoch == 0)
      }

      val thrown = intercept[KyuubiException] {
        manager.sendCredentialsIfNeeded(engineId, appUser, _ => throw new IOException)
      }

      assert(thrown.isInstanceOf[KyuubiException])
      assert(thrown.getMessage == s"Failed to send new credentials to SQL engine $engineId")
    }
  }
}

private class ExceptionThrowingDelegationTokenProvider extends HadoopDelegationTokenProvider {
  ExceptionThrowingDelegationTokenProvider.constructed = true
  throw new IllegalArgumentException

  override def serviceName: String = "throw"

  override def delegationTokensRequired(
      hadoopConf: Configuration,
      kyuubiConf: KyuubiConf): Boolean =
    throw new IllegalArgumentException

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      kyuubiConf: KyuubiConf,
      owner: String,
      creds: Credentials): Unit = throw new IllegalArgumentException

}

private object ExceptionThrowingDelegationTokenProvider {
  var constructed = false
}

private class UnRequiredDelegationTokenProvider extends HadoopDelegationTokenProvider {

  override def serviceName: String = "unrequired"

  override def delegationTokensRequired(
      hadoopConf: Configuration,
      kyuubiConf: KyuubiConf): Boolean = false

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      kyuubiConf: KyuubiConf,
      owner: String,
      creds: Credentials): Unit = {}

}

private class UnstableDelegationTokenProvider extends HadoopDelegationTokenProvider {

  override def serviceName: String = "unstable"

  override def delegationTokensRequired(
      hadoopConf: Configuration,
      kyuubiConf: KyuubiConf): Boolean = true

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      kyuubiConf: KyuubiConf,
      owner: String,
      creds: Credentials): Unit = {
    if (UnstableDelegationTokenProvider.throwException) {
      UnstableDelegationTokenProvider.exceptionCount += 1
      throw new IllegalArgumentException
    }
  }

}

private object UnstableDelegationTokenProvider {

  @volatile
  var throwException: Boolean = false

  @volatile
  var exceptionCount = 0

}
