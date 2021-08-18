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

import scala.collection.mutable.ArrayBuffer

import org.apache.kyuubi.{KyuubiFunSuite, TestUtils}

class KyuubiDefinedFunctionSuite extends KyuubiFunSuite {

  private val kyuubiHome: String = getClass.getProtectionDomain.getCodeSource
    .getLocation.getPath.split("kyuubi-spark-sql-engine")(0)
  private val markdown = Paths.get(kyuubiHome, "..", "docs", "sql", "functions.md")
    .toAbsolutePath

  test("verify or update kyuubi spark sql functions") {
    val newOutput = new ArrayBuffer[String]()
    newOutput += "<!--"
    newOutput += " - Licensed to the Apache Software Foundation (ASF) under one or more"
    newOutput += " - contributor license agreements.  See the NOTICE file distributed with"
    newOutput += " - this work for additional information regarding copyright ownership."
    newOutput += " - The ASF licenses this file to You under the Apache License, Version 2.0"
    newOutput += " - (the \"License\"); you may not use this file except in compliance with"
    newOutput += " - the License.  You may obtain a copy of the License at"
    newOutput += " -"
    newOutput += " -   http://www.apache.org/licenses/LICENSE-2.0"
    newOutput += " -"
    newOutput += " - Unless required by applicable law or agreed to in writing, software"
    newOutput += " - distributed under the License is distributed on an \"AS IS\" BASIS,"
    newOutput += " - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied."
    newOutput += " - See the License for the specific language governing permissions and"
    newOutput += " - limitations under the License."
    newOutput += " -->"
    newOutput += "<!-- DO NOT MODIFY THIS FILE DIRECTLY, IT IS AUTO GENERATED BY" +
      " [org.apache.kyuubi.engine.spark.udf.KyuubiDefinedFunctionSuite] -->"
    newOutput += ""
    newOutput += ""
    newOutput += "<div align=center>"
    newOutput += ""
    newOutput += "![](../imgs/kyuubi_logo.png)"
    newOutput += ""
    newOutput += "</div>"
    newOutput += ""
    newOutput += "# Auxiliary SQL Functions for Spark SQL"
    newOutput += ""
    newOutput += "Kyuubi provides several auxiliary SQL functions as supplement to Spark's " +
      "[Built-in Functions](https://spark.apache.org/docs/latest/api/sql/index.html#" +
      "built-in-functions)"
    newOutput += ""
    newOutput += "Name | Description | Return Type | Since"
    newOutput += "--- | --- | --- | ---"
    KDFRegistry
    KDFRegistry.registeredFunctions.foreach { func =>
      newOutput += s"${func.name} | ${func.description} | ${func.returnType} | ${func.since}"
    }

    newOutput += ""
    TestUtils.verifyOutput(markdown, newOutput, getClass.getCanonicalName)
  }
}
