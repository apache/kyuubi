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

import org.apache.spark.SparkConf
import org.apache.spark.authz.AuthzConf.CONF_RESTRICTED_LIST
import org.scalatest.BeforeAndAfterAll
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.{AccessControlException, SparkSessionProvider}
import org.apache.kyuubi.plugin.spark.authz.ranger.RuleAuthorization
import org.apache.kyuubi.plugin.spark.authz.rule.config.AuthzConfigurationChecker

class AuthzConfigurationCheckerSuite extends AnyFunSuite with SparkSessionProvider
  with BeforeAndAfterAll {

  override protected val catalogImpl: String = "in-memory"

  override protected val extraSparkConf: SparkConf = new SparkConf()
    .set(CONF_RESTRICTED_LIST.key, "spark.sql.abc,spark.sql.xyz")

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("apply spark configuration restriction rules") {
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
    val p8 = sql(
      s"set spark.sql.optimizer.excludedRules=${classOf[RuleAuthorization].getName}").queryExecution.analyzed
    intercept[AccessControlException](extension.apply(p8))
  }
}
