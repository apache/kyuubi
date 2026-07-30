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

package org.apache.kyuubi.plugin.spark.authz.ranger.rowfiltering

import org.apache.spark.SparkConf
import org.scalactic.source
import org.scalatest.Tag

import org.apache.kyuubi.Utils
import org.apache.kyuubi.plugin.spark.authz.util.AuthZUtils._
import org.apache.kyuubi.tags.PaimonTest

@PaimonTest
class RowFilteringForPaimonSuite extends RowFilteringTestBase {
  private def isSupportedVersion = isScalaV212 || isSparkV40OrGreater
  override protected val sqlExtensions: String =
    if (isSupportedVersion) "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions"
    else ""

  override protected val extraSparkConf: SparkConf = {
    new SparkConf()
      .set("spark.sql.defaultCatalog", "testcat")
      .set(
        "spark.sql.catalog.testcat",
        "org.apache.paimon.spark.SparkCatalog")
      .set(
        s"spark.sql.catalog.testcat.warehouse",
        Utils.createTempDir("paimon-hadoop").toString)
  }

  override protected val catalogImpl: String = "in-memory"

  override protected val supportPurge: Boolean = false

  override protected def format: String = "USING paimon"

  override protected def test(testName: String, testTags: Tag*)(
      testFun: => Any)(implicit pos: source.Position): Unit = {
    if (isSupportedVersion) {
      super.test(testName, testTags: _*)(testFun)(pos)
    }
  }

  override def beforeAll(): Unit = {
    if (isSupportedVersion) {
      super.beforeAll()
    }
  }

  override def afterAll(): Unit = {
    if (isSupportedVersion) {
      super.afterAll()
    }
  }
}