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

package org.apache.kyuubi.engine.spark.schema

import scala.math.BigDecimal.RoundingMode

import org.apache.commons.lang3.time.StopWatch
import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.hive.service.rpc.thrift.TProtocolVersion._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.apache.kyuubi.KyuubiFunSuite

/**
 * Benchmark to measure the performance of generate TRowSet.
 *
 * {{{
 *   RUN_BENCHMARK=1 ./build/mvn clean test \
 *   -pl externals/kyuubi-spark-sql-engine -am \
 *   -Dtest=none -DwildcardSuites=org.apache.kyuubi.engine.spark.schema.TRowSetBenchmark
 * }}}
 */
class TRowSetBenchmark extends KyuubiFunSuite with RowSetHelper {
  private val runBenchmark = sys.env.contains("RUN_BENCHMARK")

  private val rowCount = 3000
  private lazy val allRows = (0 until rowCount).map(genRow)

  test("row-based toTRowSet benchmark") {
    assume(runBenchmark)
    tRowSetGenerationBenchmark(HIVE_CLI_SERVICE_PROTOCOL_V5)
  }

  test("column-based toTRowSet benchmark") {
    assume(runBenchmark)
    tRowSetGenerationBenchmark(HIVE_CLI_SERVICE_PROTOCOL_V6)
  }

  private def tRowSetGenerationBenchmark(protocolVersion: TProtocolVersion): Unit = {
    val rowSetType = if (protocolVersion.getValue < HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      "row-based"
    } else {
      "column-based"
    }
    // scalastyle:off
    println(
      s"Benchmark result for $rowSetType RowSet.toTRowSet on $protocolVersion with $rowCount rows.")
    printf("%20s %20s %20s\n", "Type(s)", "Time Cost", "Rows/ms")
    // scalastyle:on

    val totalMs = schemaStructFields.zipWithIndex.map { case (field, idx) =>
      // run benchmark with rows of single column with one data type
      val rowsOfSingleType = allRows.map(row => Row(row.get(idx)))
      val schemaOfSingleType = StructType(Seq(field))
      benchmarkToTRowSet(
        field.getComment().getOrElse(field.dataType.typeName),
        rowsOfSingleType,
        schemaOfSingleType,
        protocolVersion)
    }.sum
    val totalRowsPerMs: BigDecimal = (BigDecimal(rowCount) / totalMs)
      .setScale(3, RoundingMode.HALF_UP)
    // scalastyle:off
    println()
    printf("%20s %20s %20s\n", "sum(all types)", s"$totalMs ms", s"$totalRowsPerMs rows/ms")

    // run benchmark with rows of columns with all data types
    benchmarkToTRowSet("with all types", allRows, schema, protocolVersion)

    println()
    println()
    // scalastyle:on
  }

  private def benchmarkToTRowSet(
      clue: String,
      rows: Seq[Row],
      schema: StructType,
      protocolVersion: TProtocolVersion): BigDecimal = {
    val sw = StopWatch.createStarted()
    RowSet.toTRowSet(rows, schema, protocolVersion)
    sw.stop()
    val msTimeCost: BigDecimal = (BigDecimal(sw.getNanoTime) / BigDecimal(1000000))
      .setScale(3, RoundingMode.HALF_UP)
    val rowsPerMilliSecond: BigDecimal = (BigDecimal(rows.size) / msTimeCost)
      .setScale(3, RoundingMode.HALF_UP)
    // scalastyle:off
    printf(
      "%20s %20s %20s\n",
      clue,
      s"$msTimeCost ms",
      s"$rowsPerMilliSecond rows/ms")
    // scalastyle:on
    msTimeCost
  }
}
