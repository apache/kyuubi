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

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.benchmark.KyuubiBenchmarkBase
import org.apache.spark.sql.internal.StaticSQLConf

import org.apache.kyuubi.sql.zorder.ZorderBytesUtils

/**
 * Benchmark to measure performance with zorder core.
 *
 * {{{
 *   RUN_BENCHMARK=1 ./build/mvn clean test \
 *   -pl dev/kyuubi-extension-spark-3-1 -am \
 *   -Pspark-3.1,kyuubi-extension-spark-3-1 \
 *   -Dtest=none -DwildcardSuites=org.apache.spark.sql.ZorderCoreBenchmark
 * }}}
 */
class ZorderCoreBenchmark extends KyuubiSparkSQLExtensionTest with KyuubiBenchmarkBase {
  private val runBenchmark = sys.env.contains("RUN_BENCHMARK")
  private val numRows = 1 * 1000 * 1000

  private def randomInt(numColumns: Int): Seq[Array[Any]] = {
    (1 to numRows).map { l =>
      val arr = new Array[Any](numColumns)
      (0 until numColumns).foreach(col => arr(col) = l)
      arr
    }
  }

  private def randomLong(numColumns: Int): Seq[Array[Any]] = {
    (1 to numRows).map { l =>
      val arr = new Array[Any](numColumns)
      (0 until numColumns).foreach(col => arr(col) = l.toLong)
      arr
    }
  }

  private def interleaveMultiByteArrayBenchmark(): Unit = {
    val benchmark = new Benchmark(
      s"$numRows rows zorder core benchmark", numRows, output = output)
    benchmark.addCase("2 int columns benchmark", 3) { _ =>
      randomInt(2).foreach(ZorderBytesUtils.interleaveBits)
    }

    benchmark.addCase("3 int columns benchmark", 3) { _ =>
      randomInt(3).foreach(ZorderBytesUtils.interleaveBits)
    }

    benchmark.addCase("4 int columns benchmark", 3) { _ =>
      randomInt(4).foreach(ZorderBytesUtils.interleaveBits)
    }

    benchmark.addCase("2 long columns benchmark", 3) { _ =>
      randomLong(2).foreach(ZorderBytesUtils.interleaveBits)
    }

    benchmark.addCase("3 long columns benchmark", 3) { _ =>
      randomLong(3).foreach(ZorderBytesUtils.interleaveBits)
    }

    benchmark.addCase("4 long columns benchmark", 3) { _ =>
      randomLong(4).foreach(ZorderBytesUtils.interleaveBits)
    }

    benchmark.run()
  }

  private def paddingTo8ByteBenchmark() {
    val iterations = 10 * 1000 * 1000

    val b2 = Array('a'.toByte, 'b'.toByte)
    val benchmark = new Benchmark(
      s"$iterations iterations paddingTo8Byte benchmark", iterations, output = output)
    benchmark.addCase("2 length benchmark", 3) { _ =>
      (1 to iterations).foreach(_ => ZorderBytesUtils.paddingTo8Byte(b2))
    }

    val b16 = Array.tabulate(16) { i => i.toByte }
    benchmark.addCase("16 length benchmark", 3) { _ =>
      (1 to iterations).foreach(_ => ZorderBytesUtils.paddingTo8Byte(b16))
    }

    benchmark.run()
  }

  test("zorder core benchmark") {
    assume(runBenchmark)

    withHeader {
      interleaveMultiByteArrayBenchmark()
      paddingTo8ByteBenchmark()
    }
  }

  override def sparkConf(): SparkConf = {
    super.sparkConf().remove(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key)
  }
}
