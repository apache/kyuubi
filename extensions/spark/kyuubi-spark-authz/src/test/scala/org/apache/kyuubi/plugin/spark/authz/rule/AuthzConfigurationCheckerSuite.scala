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

package org.apache.kyuubi.plugin.spark.authz.rule

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.spark.authz.{AccessControlException, SparkSessionProvider}
import org.apache.kyuubi.plugin.spark.authz.ParanoidMode.UNCLASSIFIED_NODE_BEHAVIOR_KEY
import org.apache.kyuubi.plugin.spark.authz.ranger.RuleAuthorization
import org.apache.kyuubi.plugin.spark.authz.rule.config.AuthzConfigurationChecker

class AuthzConfigurationCheckerSuite extends KyuubiFunSuite with SparkSessionProvider {

  override protected val catalogImpl: String = "in-memory"
  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("apply spark configuration restriction rules") {
    sql("set spark.kyuubi.conf.restricted.list=spark.sql.abc,spark.sql.xyz")
    val extension = AuthzConfigurationChecker(spark)
    val p1 = sql("set spark.sql.runSQLOnFiles=true").queryExecution.analyzed
    intercept[AccessControlException](extension.apply(p1))
    val p2 = sql("set spark.sql.runSQLOnFiles=false").queryExecution.analyzed
    intercept[AccessControlException](extension.apply(p2))
    val p3 = sql("set spark.sql.runSQLOnFiles").queryExecution.analyzed
    extension.apply(p3)
    val p4 = sql("set spark.sql.abc=xyz").queryExecution.analyzed
    intercept[AccessControlException](extension.apply(p4))
    val p5 = sql("set spark.sql.xyz=abc").queryExecution.analyzed
    intercept[AccessControlException](extension.apply(p5))
    val p6 = sql("set spark.kyuubi.conf.restricted.list=123").queryExecution.analyzed
    intercept[AccessControlException](extension.apply(p6))
    val p7 = sql("set spark.sql.efg=hijk").queryExecution.analyzed
    extension.apply(p7)
    val p8 = sql(s"set spark.sql.optimizer.excludedRules=${classOf[RuleAuthorization].getName}")
      .queryExecution.analyzed
    intercept[AccessControlException](extension.apply(p8))
    val p9 = sql(
      "set spark.kyuubi.authz.skipCataloglessV2Relation.enabled=true").queryExecution.analyzed
    intercept[AccessControlException](extension.apply(p9))
    val p10 = sql(
      s"set $UNCLASSIFIED_NODE_BEHAVIOR_KEY=allow").queryExecution.analyzed
    intercept[AccessControlException](extension.apply(p10))
  }

  test("apply spark configuration restriction rules for RESET") {
    sql("set spark.kyuubi.conf.restricted.list=spark.sql.abc,spark.sql.xyz")
    val extension = AuthzConfigurationChecker(spark)

    // RESET of hardcoded restricted keys must be blocked
    val pReset1 = sql("reset spark.sql.runSQLOnFiles").queryExecution.analyzed
    intercept[AccessControlException](extension.apply(pReset1))
    val pReset2 = sql(s"reset ${extension.RESTRICT_LIST_KEY}").queryExecution.analyzed
    intercept[AccessControlException](extension.apply(pReset2))
    val pReset3 = sql(
      "reset spark.kyuubi.authz.skipCataloglessV2Relation.enabled").queryExecution.analyzed
    intercept[AccessControlException](extension.apply(pReset3))
    val pReset6 = sql(s"reset $UNCLASSIFIED_NODE_BEHAVIOR_KEY").queryExecution.analyzed
    intercept[AccessControlException](extension.apply(pReset6))

    // RESET of admin-configured restricted keys must be blocked
    val pReset4 = sql("reset spark.sql.abc").queryExecution.analyzed
    intercept[AccessControlException](extension.apply(pReset4))

    // RESET of a non-restricted key must be allowed
    val pReset5 = sql("reset spark.sql.efg").queryExecution.analyzed
    extension.apply(pReset5)

    // Bare RESET (no key) must be blocked - it would wipe all session configs at once
    val pResetAll = sql("reset").queryExecution.analyzed
    intercept[AccessControlException](extension.apply(pResetAll))
  }
}
