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

import org.apache.spark.kyuubi.benchmark.{Benchmark, KyuubiBenchmarkBase}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.engine.spark.schema.{RowSet, RowSetHelper}
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion._

/**
 * Benchmark to measure the performance of generate TRowSet.
 *
 * {{{
 *   RUN_BENCHMARK=1 ./build/mvn clean test \
 *   -pl externals/kyuubi-spark-sql-engine -am \
 *   -Dtest=none -DwildcardSuites=org.apache.spark.kyuubi.TRowSetBenchmark
 * }}}
 */
class TRowSetBenchmark extends KyuubiFunSuite with RowSetHelper with KyuubiBenchmarkBase {
  private val runBenchmark = sys.env.contains("RUN_BENCHMARK")

  private val rowCount = 1000
  private val rotations = 5
  private lazy val allRows = (0 until rowCount).map(genRow)

  test("row-based toTRowSet benchmark") {
    assume(runBenchmark)
    val rowSetType = "column-based"
    withHeader(rowSetType) {
      tRowSetGenerationBenchmark(HIVE_CLI_SERVICE_PROTOCOL_V5, rowSetType)
    }
  }

  test("column-based toTRowSet benchmark") {
    assume(runBenchmark)
    val rowSetType = "column-based"
    withHeader(rowSetType) {
      tRowSetGenerationBenchmark(HIVE_CLI_SERVICE_PROTOCOL_V6, rowSetType)
    }
  }

  private def tRowSetGenerationBenchmark(
      protocolVersion: TProtocolVersion,
      rowSetType: String): Unit = {
    val benchmark =
      new Benchmark(
        s"$rowSetType TRowSet benchmark",
        rowCount,
        warmupTime = 3.seconds,
        output = output)

    schemaStructFields.zipWithIndex.foreach {
      case (field, idx) =>
        val rowsOfSingleType = allRows.map(row => Row(row.get(idx)))
        val schemaOfSingleType = StructType(Seq(field))

        val commentOrName = field.getComment().getOrElse(field.dataType.typeName)
        benchmark.addCase(s"$commentOrName", rotations) { _ =>
          benchmarkToTRowSet(
            rowsOfSingleType,
            schemaOfSingleType,
            protocolVersion)
        }
    }

    benchmark.addCase(s"with all types", rotations) { _ =>
      benchmarkToTRowSet(allRows, schema, protocolVersion)
    }

    benchmark.run()
  }

  private def benchmarkToTRowSet(
      rows: Seq[Row],
      schema: StructType,
      protocolVersion: TProtocolVersion): Unit = {
    RowSet.toTRowSet(rows, schema, protocolVersion)
  }
}
