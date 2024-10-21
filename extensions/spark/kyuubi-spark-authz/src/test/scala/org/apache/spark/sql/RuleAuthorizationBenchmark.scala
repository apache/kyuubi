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

package org.apache.spark.sql

import java.io.File
import java.nio.file.Files

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.reflect.io.Path.jfile2path

import org.apache.spark.benchmark.Benchmark
import org.scalatest.BeforeAndAfterAll
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.plugin.spark.authz.SparkSessionProvider
import org.apache.kyuubi.plugin.spark.authz.benchmark.KyuubiBenchmarkBase
import org.apache.kyuubi.plugin.spark.authz.ranger.RuleAuthorization
import org.apache.kyuubi.util.ThreadUtils

/**
 * Benchmark to measure performance of collecting ranger access requests.
 *
 * {{{
 *   RUN_BENCHMARK=1 ./build/mvn clean test \
 *   -pl extensions/spark/kyuubi-spark-authz -am \
 *   -Dtest=none -DwildcardSuites=org.apache.spark.sql.RuleAuthorizationBenchmark
 * }}}
 */
class RuleAuthorizationBenchmark extends AnyFunSuite
  with SparkSessionProvider with BeforeAndAfterAll
  with KyuubiBenchmarkBase {
  // scalastyle:on

  override protected val catalogImpl: String = "hive"
  private val runBenchmark = sys.env.contains("RUN_BENCHMARK")

  private val fileNumbers = 50000

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

  test("Collecting files ranger access request") {
    assume(runBenchmark)

    val futures = (1 to fileNumbers).map { i =>
      Future {
        val file = new File(tempDir, s"file_$i.txt")
        file.createNewFile()
      }
    }
    val allFutures = Future.sequence(futures)
    ThreadUtils.awaitResult(allFutures, Duration.Inf)

    val df = spark.read.text(tempDir + "/file_*.txt")
    val plan = df.queryExecution.optimizedPlan

    withHeader {
      val benchmark = new Benchmark(s"Collecting files ranger access request", -1, output = output)
      benchmark.addCase(s"$fileNumbers files benchmark", 3) { _ =>
        RuleAuthorization(spark).checkPrivileges(spark, plan)
      }
      benchmark.run()
    }
  }
}
