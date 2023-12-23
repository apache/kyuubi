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

package org.apache.kyuubi.plugin.spark.authz.gen

import java.nio.file.Paths

// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.util.AssertionUtils._
import org.apache.kyuubi.util.GoldenFileUtils._

class CheckAuthzExtractorSPISuite extends AnyFunSuite {
  // scalastyle:on

  test("check authz extractor SPI service file sorted") {
    Seq(
      "org.apache.kyuubi.plugin.spark.authz.serde.ActionTypeExtractor",
      "org.apache.kyuubi.plugin.spark.authz.serde.CatalogExtractor",
      "org.apache.kyuubi.plugin.spark.authz.serde.ColumnExtractor",
      "org.apache.kyuubi.plugin.spark.authz.serde.DatabaseExtractor",
      "org.apache.kyuubi.plugin.spark.authz.serde.FunctionExtractor",
      "org.apache.kyuubi.plugin.spark.authz.serde.FunctionTypeExtractor",
      "org.apache.kyuubi.plugin.spark.authz.serde.QueryExtractor",
      "org.apache.kyuubi.plugin.spark.authz.serde.TableExtractor",
      "org.apache.kyuubi.plugin.spark.authz.serde.TableTypeExtractor",
      "org.apache.kyuubi.plugin.spark.authz.serde.URIExtractor")
      .foreach { fileName =>
        val filePath = Paths.get(
          s"${getCurrentModuleHome(this)}/src/main/resources/META-INF/services/$fileName")
        assertFileContentSorted(filePath)
      }
  }
}
