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

package org.apache.spark.kyuubi

import scala.concurrent.duration._

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.kyuubi.benchmark.KyuubiBenchmarkBase
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connector.tpcds._

/**
 * Benchmark to measure the performance of generate TPCDSTable.
 *
 * {{{
 *   RUN_BENCHMARK=1 ./build/mvn clean test \
 *   -pl extensions/spark/kyuubi-spark-connector-tpcds -am \
 *   -Dtest=none -DwildcardSuites=org.apache.spark.kyuubi.TPCDSTableGenerateBenchmark
 * }}}
 */
class TPCDSTableGenerateBenchmark extends KyuubiFunSuite with KyuubiBenchmarkBase {

  private val runBenchmark = sys.env.contains("RUN_BENCHMARK")
  private val numRows: Long = 1 * 1000 * 1000
  private val scale = TPCDSSchemaUtils.SCALES.last.toDouble

  test("benchmark") {
    assume(runBenchmark)
    withHeader {
      numRowsTableBenchmark()
    }
  }

  private def numRowsTableBenchmark(): Unit = {
    val benchmark =
      new Benchmark(
        s"TPCDS table generates $numRows rows benchmark",
        numRows,
        minTime = 30.seconds,
        output = output)
    TPCDSSchemaUtils.BASE_TABLES
      .filter(TPCDSStatisticsUtils.numRows(_, scale) >= numRows)
      .map(_.getName)
      .foreach { table =>
        benchmark.addCase(s"$table benchmark", 3) { _ =>
          generateTable(table, numRows)
        }
      }
    benchmark.run()
  }

  private def generateTable(tableName: String, rowCount: Long): Unit = {
    val table = new TPCDSTable(
      tableName,
      scale,
      TPCDSConf(SparkSession.active, CaseInsensitiveStringMap.empty()))
    val reader = new TPCDSPartitionReader(
      tableName,
      scale,
      1,
      1,
      table.schema)
    var count = 0
    while (reader.next() && count < rowCount) {
      count += 1
    }
    assert(count == rowCount)
  }

}
