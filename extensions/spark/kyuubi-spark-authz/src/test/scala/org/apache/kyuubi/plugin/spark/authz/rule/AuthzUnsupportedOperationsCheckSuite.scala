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
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.{AccessControlException, SparkSessionProvider}

class AuthzUnsupportedOperationsCheckSuite extends AnyFunSuite with SparkSessionProvider
  with BeforeAndAfterAll {
  // scalastyle:on

  override protected val catalogImpl: String = "in-memory"
  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }

  test("disable script transformation") {
    val extension = new AuthzUnsupportedOperationsCheck
    val p1 = sql("SELECT TRANSFORM('') USING 'ls /'").queryExecution.analyzed
    intercept[AccessControlException](extension.apply(p1))
  }
}
