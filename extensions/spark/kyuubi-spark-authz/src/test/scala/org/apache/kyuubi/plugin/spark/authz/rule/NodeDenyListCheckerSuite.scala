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

import org.scalatest.BeforeAndAfterAll

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.plugin.spark.authz.{AccessControlException, SparkSessionProvider}
import org.apache.kyuubi.plugin.spark.authz.rule.config.NodeDenyListChecker

class NodeDenyListCheckerSuite extends KyuubiFunSuite with SparkSessionProvider
  with BeforeAndAfterAll {

  // TRANSFORM ... USING requires Hive catalog support
  override protected val catalogImpl: String = "hive"

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("plain SELECT is allowed") {
    sql("CREATE TABLE IF NOT EXISTS ndl_plain (k INT, v STRING) USING hive")
    try {
      val plan = sql("SELECT k, v FROM ndl_plain").queryExecution.analyzed
      NodeDenyListChecker(spark).apply(plan)
    } finally {
      sql("DROP TABLE IF EXISTS ndl_plain PURGE")
    }
  }

  test("TRANSFORM ... USING is rejected") {
    sql("CREATE TABLE IF NOT EXISTS ndl_transform (k INT, v STRING) USING hive")
    try {
      val plan =
        sql("SELECT TRANSFORM(k, v) USING 'cat' AS (k STRING, v STRING) FROM ndl_transform")
          .queryExecution.analyzed
      intercept[AccessControlException](NodeDenyListChecker(spark).apply(plan))
    } finally {
      sql("DROP TABLE IF EXISTS ndl_transform PURGE")
    }
  }

  test("TRANSFORM ... USING inside a subquery is rejected") {
    sql("CREATE TABLE IF NOT EXISTS ndl_sub (k INT, v STRING) USING hive")
    try {
      val plan = sql(
        """SELECT * FROM (
          |  SELECT TRANSFORM(k, v) USING 'cat' AS (k STRING, v STRING) FROM ndl_sub
          |) t""".stripMargin).queryExecution.analyzed
      intercept[AccessControlException](NodeDenyListChecker(spark).apply(plan))
    } finally {
      sql("DROP TABLE IF EXISTS ndl_sub PURGE")
    }
  }
}
