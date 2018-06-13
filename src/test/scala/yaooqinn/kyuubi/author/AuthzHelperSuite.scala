/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.author

import org.apache.spark.{KyuubiConf, SparkConf, SparkFunSuite}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

import yaooqinn.kyuubi.server.KyuubiServer

class AuthzHelperSuite extends SparkFunSuite {

  test("test Rule") {

    val conf = new SparkConf(loadDefaults = true)
    val authzHelper1 = new AuthzHelper(conf)
    assert(authzHelper1.rule.isEmpty)
    KyuubiServer.setupCommonConfig(conf)
    val authzHelper2 = new AuthzHelper(conf)
    assert(authzHelper2.rule.isEmpty)
    conf.set(KyuubiConf.AUTHORIZATION_METHOD.key, "yaooqinn.kyuubi.TestRule")
    val authzHelper3 = new AuthzHelper(conf)
    assert(authzHelper3.rule.nonEmpty)
    assert(authzHelper3.rule.head.isInstanceOf[Rule[LogicalPlan]])

  }

  test("test Get") {
    assert(AuthzHelper.get.isEmpty)
  }

  test("test Init") {
    val conf = new SparkConf(loadDefaults = true)
      .set(KyuubiConf.AUTHORIZATION_METHOD.key, "yaooqinn.kyuubi.TestRule")
      .set(KyuubiConf.AUTHORIZATION_ENABLE.key, "false")
    AuthzHelper.init(conf)
    assert(AuthzHelper.get.isEmpty)

    conf.set(KyuubiConf.AUTHORIZATION_ENABLE.key, "true")
    AuthzHelper.init(conf)
    assert(AuthzHelper.get.nonEmpty)
    assert(AuthzHelper.get.get.rule.nonEmpty)
    assert(AuthzHelper.get.get.rule.head.isInstanceOf[Rule[LogicalPlan]])
  }
}
