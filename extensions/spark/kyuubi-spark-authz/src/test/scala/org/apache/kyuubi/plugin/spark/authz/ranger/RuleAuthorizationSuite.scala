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

package org.apache.kyuubi.plugin.spark.authz.ranger

import java.io.File
import java.nio.file.Files

import scala.reflect.io.Path.jfile2path

import org.apache.ranger.plugin.policyengine.RangerAccessRequest
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import org.apache.kyuubi.plugin.spark.authz.SparkSessionProvider

class RuleAuthorizationSuite extends AnyFunSuite
  with SparkSessionProvider with BeforeAndAfterAll with MockitoSugar {
  // scalastyle:on
  override protected val catalogImpl: String = "hive"

  private var tempDir: File = _

  override def beforeAll(): Unit = {
    tempDir = Files.createTempDirectory("kyuubi-test-").toFile
  }


  override def afterAll(): Unit = {
    if (tempDir != null) {
      tempDir.deleteRecursively()
    }
    spark.stop()
    super.afterAll()
  }

  // scalastyle:on
  test("KYUUBI #6754: improve the performance of ranger access requests") {
    val outputPath = tempDir.getAbsolutePath + "/small_files"
    spark.range(1, 1000, 1, 1000).write.parquet(outputPath)

    val plugin = mock[SparkRangerAdminPlugin.type]
    when(plugin.verify(Seq(any[RangerAccessRequest]), any[SparkRangerAuditHandler]))
      .thenAnswer(_ => ())

    val df = spark.read.parquet(outputPath + "/*.parquet")
    val plan = df.queryExecution.optimizedPlan
    val start = System.currentTimeMillis()
    RuleAuthorization(spark).checkPrivileges(spark, plan)
    val end = System.currentTimeMillis()
    assert(end - start < 10000, "RuleAuthorization.checkPrivileges() timed out")
  }
}
