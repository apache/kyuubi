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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf

class HadoopDelegationTokenManagerSuite extends KyuubiFunSuite {

  test("default configuration") {
    ExceptionThrowingDelegationTokenProvider.constructed = false
    val manager = new HadoopDelegationTokenManager(new KyuubiConf(false), new Configuration())
    assert(manager.isProviderLoaded("hadoopfs"))
    assert(manager.isProviderLoaded("hive"))
    assert(manager.isProviderLoaded("unstable"))
    // This checks that providers are loaded independently and they have no effect on each other
    assert(ExceptionThrowingDelegationTokenProvider.constructed)
    assert(!manager.isProviderLoaded("throw"))
  }

  test("disable hadoopfs credential provider") {
    val kyuubiConf =
      new KyuubiConf(false).set("kyuubi.security.credentials.hadoopfs.enabled", "false")
    val manager = new HadoopDelegationTokenManager(kyuubiConf, new Configuration())
    assert(!manager.isProviderLoaded("hadoopfs"))
  }

  test("obtain delegation tokens and schedule update") {
    val conf = new KyuubiConf(false)
      .set(KyuubiConf.CREDENTIALS_RENEWAL_INTERVAL, 1000L)
    val manager = new HadoopDelegationTokenManager(conf, new Configuration())
    manager.start()

    try {
      val credentialsRef = manager.obtainDelegationTokensRef("who")
      assert(credentialsRef.getEpoch == 0)

      // Tolerate 100 ms delay
      eventually(timeout(1100.milliseconds), interval(100.milliseconds)) {
        assert(credentialsRef.getEpoch == 1)
      }
    } finally {
      manager.stop()
    }
  }

  test("schedule retry when failed to update") {
    val kyuubiConf = new KyuubiConf(false)
      .set(KyuubiConf.CREDENTIALS_RENEWAL_INTERVAL, 1000L)
      .set(KyuubiConf.CREDENTIALS_RENEWAL_RETRY_WAIT, 1000L)
    val manager = new HadoopDelegationTokenManager(kyuubiConf, new Configuration())
    manager.start()

    try {
      val credentialsRef = manager.obtainDelegationTokensRef("who")
      assert(credentialsRef.getEpoch == 0)

      UnstableDelegationTokenProvider.throwException = true
      // Tolerate 100 ms delay
      eventually(timeout(2100.milliseconds), interval(100.milliseconds)) {
        // At least 1 scheduled call and 1 scheduled retrying call have take place.
        assert(UnstableDelegationTokenProvider.exceptionCount == 2)
      }
      assert(credentialsRef.getEpoch == 0)
    } finally {
      UnstableDelegationTokenProvider.throwException = false
      manager.stop()
    }
  }
}

private class ExceptionThrowingDelegationTokenProvider extends HadoopDelegationTokenProvider {
  ExceptionThrowingDelegationTokenProvider.constructed = true
  throw new IllegalArgumentException

  override def serviceName: String = "throw"

  override def delegationTokensRequired(hadoopConf: Configuration): Boolean =
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

private class UnstableDelegationTokenProvider extends HadoopDelegationTokenProvider {

  override def serviceName: String = "unstable"

  override def delegationTokensRequired(hadoopConf: Configuration): Boolean = true

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
