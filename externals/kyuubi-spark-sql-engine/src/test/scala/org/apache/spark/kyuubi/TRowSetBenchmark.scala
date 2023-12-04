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

import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.hive.service.rpc.thrift.TProtocolVersion._
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.kyuubi.benchmark.KyuubiBenchmarkBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.HiveResult
import org.apache.spark.sql.types._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.engine.spark.schema.{RowSet, RowSetHelper}

/**
 * Benchmark to measure the performance of generate TRowSet.
 *
 * {{{
 *   RUN_BENCHMARK=1 ./build/mvn clean test \
 *   -pl externals/kyuubi-spark-sql-engine -am \
 *   -Dtest=none -DwildcardSuites=org.apache.kyuubi.engine.spark.schema.TRowSetBenchmark
 * }}}
 */
class TRowSetBenchmark extends KyuubiFunSuite with RowSetHelper with KyuubiBenchmarkBase {
  private val runBenchmark = sys.env.contains("RUN_BENCHMARK") || true

  private val rowCount = 3000
  private lazy val allRows = (0 until rowCount).map(genRow)

  test("row-based toTRowSet benchmark") {
    assume(runBenchmark)
    withHeader {
      tRowSetGenerationBenchmark(HIVE_CLI_SERVICE_PROTOCOL_V5)
    }
  }

  test("column-based toTRowSet benchmark") {
    assume(runBenchmark)
    withHeader {
      tRowSetGenerationBenchmark(HIVE_CLI_SERVICE_PROTOCOL_V6)
    }
  }

  private def tRowSetGenerationBenchmark(protocolVersion: TProtocolVersion): Unit = {
    val rowSetType = if (protocolVersion.getValue < HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      "row-based"
    } else {
      "column-based"
    }

    val benchmark =
      new Benchmark(
        s"TRowSet toHiveString benchmark",
        rowCount,
        minTime = 30.seconds,
        output = output)

    schemaStructFields.zipWithIndex.foreach {
      case (field, idx) =>
        val rowsOfSingleType = allRows.map(row => Row(row.get(idx)))
        val schemaOfSingleType = StructType(Seq(field))

        val commentOrName = field.getComment().getOrElse(field.dataType.typeName)
        benchmark.addCase(s"$rowSetType $commentOrName benchmark", 10) {
          _ =>
            benchmarkToTRowSet(
              commentOrName,
              rowsOfSingleType,
              schemaOfSingleType,
              protocolVersion)
        }
    }
    benchmark.run()

//    val totalMs = schemaStructFields.zipWithIndex.map { case (field, idx) =>
//      // run benchmark with rows of single column with one data type
//      val rowsOfSingleType = allRows.map(row => Row(row.get(idx)))
//      val schemaOfSingleType = StructType(Seq(field))
//      benchmarkToTRowSet(
//        field.getComment().getOrElse(field.dataType.typeName),
//        rowsOfSingleType,
//        schemaOfSingleType,
//        protocolVersion)
//
//      benchmark.addCase("toHiveString benchmark", 3) { _ =>
//        rowsOfSingleType.map(row => RowSet.toHiveString((row.get(0), field.dataType)))
//      }
//    }.sum
//    val totalRowsPerMs: BigDecimal = (BigDecimal(rowCount) / totalMs)
//      .setScale(3, RoundingMode.HALF_UP)
//    // scalastyle:off
//    println()
//    printf("%20s %20s %20s\n", "sum(all types)", s"$totalMs ms", s"$totalRowsPerMs rows/ms")
//
//    // run benchmark with rows of columns with all data types
//    benchmarkToTRowSet("with all types", allRows, schema, protocolVersion)
//
//    println()
//    println()
//    // scalastyle:on
  }

  private def benchmarkToTRowSet(
      clue: String,
      rows: Seq[Row],
      schema: StructType,
      protocolVersion: TProtocolVersion): Unit = {
    val timeFormatters = HiveResult.getTimeFormatters
    RowSet.toTRowSet(rows, schema, protocolVersion, timeFormatters)
  }
}
