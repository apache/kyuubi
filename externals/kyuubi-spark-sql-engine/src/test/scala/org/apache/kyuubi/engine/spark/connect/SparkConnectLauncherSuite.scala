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

package org.apache.kyuubi.engine.spark.connect

import org.apache.spark.SparkConf

import org.apache.kyuubi.{KyuubiException, KyuubiFunSuite}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_SHARE_LEVEL, ENGINE_SPARK_CONNECT_ENABLED}
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.spark.connect.SparkConnectLauncher._

class SparkConnectLauncherSuite extends KyuubiFunSuite {

  private val connectPlugin = "org.apache.spark.sql.connect.SparkConnectPlugin"
  private val fakePluginClass = FakeConnectPlugin.getClass.getName
  private val fakeConfigClass = FakeConnectConfig.getClass.getName
  private val fakeServiceClass = FakeConnectService.getClass.getName

  test("disabled unless the engine is configured for it") {
    assert(!isEnabled(KyuubiConf()))
    assert(isEnabled(KyuubiConf().set(ENGINE_SPARK_CONNECT_ENABLED, true)))
  }

  test("add the Connect plugin and bind an ephemeral port") {
    val overrides = sparkConfOverrides(new SparkConf(false)).toMap
    assert(overrides(PLUGINS_KEY) === connectPlugin)
    assert(overrides(BINDING_PORT_KEY) === "0")
  }

  test("keep the plugins and the port the deployment configured") {
    val sparkConf = new SparkConf(false)
      .set(PLUGINS_KEY, "org.apache.spark.custom.Plugin")
      .set(BINDING_PORT_KEY, "15002")
    val overrides = sparkConfOverrides(sparkConf).toMap
    assert(overrides(PLUGINS_KEY) === s"org.apache.spark.custom.Plugin,$connectPlugin")
    assert(!overrides.contains(BINDING_PORT_KEY))
  }

  test("do not add the Connect plugin twice") {
    val sparkConf = new SparkConf(false).set(PLUGINS_KEY, connectPlugin)
    assert(!sparkConfOverrides(sparkConf).toMap.contains(PLUGINS_KEY))
  }

  test("only the USER share level is supported") {
    checkShareLevel(KyuubiConf().set(ENGINE_SHARE_LEVEL, ShareLevel.USER.toString))
    Seq(ShareLevel.CONNECTION, ShareLevel.GROUP, ShareLevel.SERVER).foreach { shareLevel =>
      val e = intercept[KyuubiException] {
        checkShareLevel(KyuubiConf().set(ENGINE_SHARE_LEVEL, shareLevel.toString))
      }
      assert(e.getMessage.contains(s"requires ${ENGINE_SHARE_LEVEL.key}=${ShareLevel.USER}"))
    }
  }

  test("refuse to open the endpoint without a token") {
    val e = intercept[KyuubiException] {
      checkAuthenticateToken(new SparkConf(false))
    }
    assert(e.getMessage.contains(AUTHENTICATE_TOKEN_KEY))
    checkAuthenticateToken(new SparkConf(false).set(AUTHENTICATE_TOKEN_KEY, "a-token"))
  }

  test("an empty token is no token, the way Spark reads it") {
    // Connect.getAuthenticateToken takes the config first and only falls back to the environment
    // when it is absent, so an empty config value authenticates nobody
    intercept[KyuubiException] {
      checkAuthenticateToken(new SparkConf(false).set(AUTHENTICATE_TOKEN_KEY, ""))
    }
  }

  test("probe the runtime for Spark Connect authentication support") {
    assert(authenticationSupported(fakeConfigClass))
    assert(!authenticationSupported(fakeServiceClass))
    assert(!authenticationSupported("org.apache.kyuubi.engine.spark.connect.NoSuchClass$"))
  }

  test("accept a runtime whose Spark Connect authenticates, refuse one that does not") {
    checkRuntimeSupport(fakePluginClass, fakeConfigClass)
    val e = intercept[KyuubiException](checkRuntimeSupport(fakePluginClass, fakeServiceClass))
    assert(e.getMessage.contains("without authentication support"))
  }

  test("read the bound endpoint back from the Connect service") {
    val sparkConf = new SparkConf(false).set(DRIVER_HOST_KEY, "10.0.0.1")
    assert(boundEndpoint(sparkConf, fakeServiceClass) === Some("10.0.0.1" -> 15002))
    // nothing to advertise when the driver host is unknown
    assert(boundEndpoint(new SparkConf(false), fakeServiceClass).isEmpty)
    // nor when the service is not there at all
    assert(boundEndpoint(sparkConf).isEmpty)
  }

  test("refuse to start when the Spark distribution has no Spark Connect") {
    // the engine module does not depend on spark-connect, so this is the 'no Connect' runtime
    val e = intercept[KyuubiException](checkRuntimeSupport())
    assert(e.getMessage.contains(connectPlugin))
  }
}
