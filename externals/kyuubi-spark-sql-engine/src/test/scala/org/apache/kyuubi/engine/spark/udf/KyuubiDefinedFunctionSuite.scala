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

import org.apache.kyuubi.{KyuubiFunSuite, MarkdownBuilder}
import org.apache.kyuubi.util.GoldenFileUtils._
import org.apache.kyuubi.util.JavaUtils

/**
 * End-to-end test cases for configuration doc file
 * The golden result file is "docs/extensions/engines/spark/functions.md".
 *
 * To run the entire test suite:
 * {{{
 *   KYUUBI_UPDATE=0 dev/gen/gen_spark_kdf_docs.sh
 * }}}
 *
 * To re-generate golden files for entire suite, run:
 * {{{
 *   dev/gen/gen_spark_kdf_docs.sh
 * }}}
 */
class KyuubiDefinedFunctionSuite extends KyuubiFunSuite {

  private val kyuubiHome: String = JavaUtils.getCodeSourceLocation(getClass)
    .split("kyuubi-spark-sql-engine")(0)
  private val markdown =
    Paths.get(kyuubiHome, "..", "docs", "extensions", "engines", "spark", "functions.md")
      .toAbsolutePath

  test("verify or update kyuubi spark sql functions") {
    val builder = MarkdownBuilder(licenced = true, getClass.getName)

    builder += "# Auxiliary SQL Functions" +=
      """Kyuubi provides several auxiliary SQL functions as supplement to Spark's
        | [Built-in Functions](https://spark.apache.org/docs/latest/api/sql/index.html#
        |built-in-functions)""" ++=
      """
        | Name | Description | Return Type | Since
        | --- | --- | --- | ---
        |"""
    new KyuubiDF().registeredFunctions.foreach { func =>
      builder += s"${func.name} | ${func.description} | ${func.returnType} | ${func.since}"
    }

    verifyOrRegenerateGoldenFile(markdown, builder.toMarkdown, "dev/gen/gen_spark_kdf_docs.sh")
  }
}
