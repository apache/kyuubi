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

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

class SessionManagerValidationSuite extends KyuubiFunSuite {

  private var sessionManager: NoopSessionManager = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = KyuubiConf(false)
      .set(SESSION_CONF_RESTRICT_LIST, Set("spark.*"))
      .set(SESSION_CONF_IGNORE_LIST, Set("session.engine.*"))
      .set(BATCH_CONF_IGNORE_LIST, Set("spark.master"))
    sessionManager = new NoopSessionManager()
    sessionManager.initialize(conf)
    sessionManager.start()
  }

  override def afterAll(): Unit = {
    sessionManager.stop()
    super.afterAll()
  }

  test("validateAndNormalizeConf drops immutable keys") {
    val conf = Map(
      FRONTEND_THRIFT_BINARY_BIND_PORT.key -> "10009",
      "kyuubi.operation.idle.timeout" -> "5h")
    val result = sessionManager.validateAndNormalizeConf(conf)
    assert(!result.contains(FRONTEND_THRIFT_BINARY_BIND_PORT.key))
    assert(result("kyuubi.operation.idle.timeout") === "5h")
  }

  test("validateAndNormalizeConf drops immutable keys with set prefix") {
    val conf = Map(
      s"set:hiveconf:${FRONTEND_THRIFT_BINARY_BIND_PORT.key}" -> "10009",
      "set:hiveconf:kyuubi.operation.idle.timeout" -> "5h")
    val result = sessionManager.validateAndNormalizeConf(conf)
    assert(!result.contains(FRONTEND_THRIFT_BINARY_BIND_PORT.key))
    assert(result("kyuubi.operation.idle.timeout") === "5h")
  }

  test("validateBatchConf drops immutable keys") {
    val conf = Map(
      FRONTEND_THRIFT_BINARY_BIND_PORT.key -> "10009",
      "spark.executor.memory" -> "4g")
    val result = sessionManager.validateBatchConf(conf)
    assert(!result.contains(FRONTEND_THRIFT_BINARY_BIND_PORT.key))
    assert(result("spark.executor.memory") === "4g")
  }

  test("validateBatchConf drops batch ignore list keys") {
    val conf = Map(
      "spark.master" -> "local",
      "spark.executor.memory" -> "4g")
    val result = sessionManager.validateBatchConf(conf)
    assert(!result.contains("spark.master"))
    assert(result("spark.executor.memory") === "4g")
  }

  test("restrict takes priority over immutable in validateAndNormalizeConf") {
    // spark.* is in SESSION_CONF_RESTRICT_LIST; restrict check must fire before immutable check
    val conf = Map(
      "spark.driver.memory" -> "2G",
      "kyuubi.operation.idle.timeout" -> "5h")
    val e = intercept[org.apache.kyuubi.KyuubiSQLException] {
      sessionManager.validateAndNormalizeConf(conf)
    }
    assert(e.getMessage.contains("restrict"))
  }

  test("validateAndNormalizeConf drops immutable keys specified by alternative name") {
    val conf = Map(
      AUTHENTICATION_LDAP_BASE_DN.alternatives.head -> "dc=example,dc=com",
      "kyuubi.operation.idle.timeout" -> "5h")
    val result = sessionManager.validateAndNormalizeConf(conf)
    assert(!result.contains(AUTHENTICATION_LDAP_BASE_DN.alternatives.head))
    assert(result("kyuubi.operation.idle.timeout") === "5h")
  }

  test("validateBatchConf drops immutable keys specified by alternative name") {
    val conf = Map(
      AUTHENTICATION_LDAP_BASE_DN.alternatives.head -> "dc=example,dc=com",
      "spark.executor.memory" -> "4g")
    val result = sessionManager.validateBatchConf(conf)
    assert(!result.contains(AUTHENTICATION_LDAP_BASE_DN.alternatives.head))
    assert(result("spark.executor.memory") === "4g")
  }

}
