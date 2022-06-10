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

package org.apache.kyuubi.spark.connector.tpcds

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConverters._
import scala.io.{Codec, Source}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession
import org.apache.kyuubi.spark.connector.common.SparkUtils

// scalastyle:off line.size.limit
/**
 * To run this test suite:
 * {{{
 *   build/mvn clean install \
 *     -Dmaven.plugin.scalatest.exclude.tags="" \
 *     -Dtest=none -DwildcardSuites=org.apache.kyuubi.spark.connector.tpcds.TPCDSQuerySuite
 * }}}
 *
 * To re-generate golden files for this suite:
 * {{{
 *   KYUUBI_UPDATE=1 build/mvn clean install \
 *     -Dmaven.plugin.scalatest.exclude.tags="" \
 *     -Dtest=none -DwildcardSuites=org.apache.kyuubi.spark.connector.tpcds.TPCDSQuerySuite
 * }}}
 */
// scalastyle:on line.size.limit

class TPCDSQuerySuite extends KyuubiFunSuite {

  private val regenerateGoldenFiles = sys.env.get("KYUUBI_UPDATE").contains("1")

  private val licenseHeader =
    """/*
      | * Licensed to the Apache Software Foundation (ASF) under one or more
      | * contributor license agreements.  See the NOTICE file distributed with
      | * this work for additional information regarding copyright ownership.
      | * The ASF licenses this file to You under the Apache License, Version 2.0
      | * (the "License"); you may not use this file except in compliance with
      | * the License.  You may obtain a copy of the License at
      | *
      | *    http://www.apache.org/licenses/LICENSE-2.0
      | *
      | * Unless required by applicable law or agreed to in writing, software
      | * distributed under the License is distributed on an "AS IS" BASIS,
      | * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
      | * See the License for the specific language governing permissions and
      | * limitations under the License.
      | */""".stripMargin + "\n"

  val baseResourcePath: Path =
    Paths.get("src", "main", "resources")

  val queries: Set[String] = (1 to 99).map(i => s"q$i").toSet -
    ("q14", "q23", "q24", "q39") +
    ("q14a", "q14b", "q23a", "q23b", "q24a", "q24b", "q39a", "q39b")

  private def fileToString(file: Path): String = {
    new String(Files.readAllBytes(file), StandardCharsets.UTF_8)
  }

  test("run query on sf0") {
    assume(SparkUtils.isSparkVersionEqualTo("3.2"))
    val viewSuffix = "view";
    val sparkConf = new SparkConf().setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
      .set("spark.sql.catalog.tpcds.useTableSchema_2_6", "true")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      spark.sql("USE tpcds.tiny")
      queries.map { queryName =>
        val in = getClass.getClassLoader.getResourceAsStream(s"tpcds_3.2/sql/$queryName.sql")
        val queryContent: String = Source.fromInputStream(in)(Codec.UTF8).mkString
        in.close()
        queryName -> queryContent
      }.foreach { case (name, sql) =>
        try {
          val result = spark.sql(sql).collect()
          val schema = spark.sql(sql).schema
          val schemaDDL = licenseHeader + schema.toDDL + "\n"
          spark.createDataFrame(result.toList.asJava, schema).createTempView(s"$name$viewSuffix")
          val sumHashResult = licenseHeader + spark.sql(
            s"select sum(hash(*)) from $name$viewSuffix").collect().head.get(0) + "\n"

          val goldenSchemaFile = Paths.get(
            baseResourcePath.toFile.getAbsolutePath,
            "tpcds_3.2",
            "schema",
            s"${name.stripSuffix(".sql")}.output.schema")

          val goldenHashFile = Paths.get(
            baseResourcePath.toFile.getAbsolutePath,
            "tpcds_3.2",
            "schema",
            s"${name.stripSuffix(".sql")}.output.hash")
          if (regenerateGoldenFiles) {
            Files.write(goldenSchemaFile, schemaDDL.getBytes)
            Files.write(goldenHashFile, sumHashResult.getBytes)
          }
          val expectedSchema = fileToString(goldenSchemaFile)
          val expectedHash = fileToString(goldenHashFile)
          assert(schemaDDL == expectedSchema)
          assert(sumHashResult == expectedHash)
        } catch {
          case cause: Throwable =>
            fail(name, cause)
        }
      }
    }
  }
}
