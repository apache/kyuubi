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

package org.apache.kyuubi.engine.spark.udf

import java.nio.file.Paths

import org.apache.kyuubi.{KyuubiFunSuite, MarkdownBuilder, MarkdownUtils, Utils}

// scalastyle:off line.size.limit
/**
 * End-to-end test cases for configuration doc file
 * The golden result file is "docs/sql/functions.md".
 *
 * To run the entire test suite:
 * {{{
 *   build/mvn clean test -pl externals/kyuubi-spark-sql-engine -am -Pflink-provided,spark-provided,hive-provided -DwildcardSuites=org.apache.kyuubi.engine.spark.udf.KyuubiDefinedFunctionSuite
 * }}}
 *
 * To re-generate golden files for entire suite, run:
 * {{{
 *   KYUUBI_UPDATE=1 build/mvn clean test -pl externals/kyuubi-spark-sql-engine -am -Pflink-provided,spark-provided,hive-provided -DwildcardSuites=org.apache.kyuubi.engine.spark.udf.KyuubiDefinedFunctionSuite
 * }}}
 */
// scalastyle:on line.size.limit
class KyuubiDefinedFunctionSuite extends KyuubiFunSuite {

  private val kyuubiHome: String = Utils.getCodeSourceLocation(getClass)
    .split("kyuubi-spark-sql-engine")(0)
  private val markdown =
    Paths.get(kyuubiHome, "..", "docs", "extensions", "engines", "spark", "functions.md")
      .toAbsolutePath

  test("verify or update kyuubi spark sql functions") {
    val builder = MarkdownBuilder(licenced = true, getClass.getName)

    builder
      .line("# Auxiliary SQL Functions")
      .line("""Kyuubi provides several auxiliary SQL functions as supplement to Spark's
        | [Built-in Functions](https://spark.apache.org/docs/latest/api/sql/index.html#
        |built-in-functions)""")
      .lines("""
        | Name | Description | Return Type | Since
        | --- | --- | --- | ---
        |
        |""")
    KDFRegistry.registeredFunctions.foreach { func =>
      builder.line(s"${func.name} | ${func.description} | ${func.returnType} | ${func.since}")
    }

    MarkdownUtils.verifyOutput(
      markdown,
      builder,
      getClass.getCanonicalName,
      "externals/kyuubi-spark-sql-engine")
  }
}
