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

package org.apache.kyuubi.spark.connector.tpch

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
 *     -Dtest=none -DwildcardSuites=org.apache.kyuubi.spark.connector.tpch.TPCHQuerySuite
 * }}}
 *
 * To re-generate golden files for this suite:
 * {{{
 *   KYUUBI_UPDATE=1 build/mvn clean install \
 *     -Dmaven.plugin.scalatest.exclude.tags="" \
 *     -Dtest=none -DwildcardSuites=org.apache.kyuubi.spark.connector.tpch.TPCHQuerySuite
 * }}}
 */
// scalastyle:on line.size.limit

class TPCHQuerySuite extends KyuubiFunSuite {

  private val regenerateGoldenFiles = sys.env.get("KYUUBI_UPDATE").contains("1")

  val baseResourcePath: Path =
    Paths.get("src", "main", "resources")

  val queries: Set[String] = (1 to 22).map(i => s"q$i").toSet

  val queryToSumHash: Map[String, Long] = Map(
    "q1" -> -2130215201L,
    "q2" -> 2296723790L,
    "q3" -> -4118618460L,
    "q4" -> 1856406098L,
    "q5" -> 3717321142L,
    "q6" -> 2062248569L,
    "q7" -> -1955579146L,
    "q8" -> 453239528L,
    "q9" -> 10861514367L,
    "q10" -> -4090660469L,
    "q11" -> -21773379672L,
    "q12" -> 2455990065L,
    "q13" -> -1379884230L,
    "q14" -> 47333415L,
    "q15" -> -2021679095L,
    "q16" -> 326972717L,
    "q17" -> 42L,
    "q18" -> 1596157524L,
    "q19" -> -1061725826L,
    "q20" -> -35851308L,
    "q21" -> 1407638530L,
    "q22" -> 2111900859L)

  private def fileToString(file: Path): String = {
    new String(Files.readAllBytes(file), StandardCharsets.UTF_8)
  }

  test("run query on tiny") {
    assume(SparkUtils.isSparkVersionEqualTo("3.2"))
    val viewSuffix = "view";
    val sparkConf = new SparkConf().setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpch", classOf[TPCHCatalog].getName)
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      spark.sql("USE tpch.tiny")
      queries.map { queryName =>
        val in = getClass.getClassLoader.getResourceAsStream(
          s"tpch/sql/$queryName.sql")
        val queryContent: String = Source.fromInputStream(in)(Codec.UTF8).mkString
        in.close()
        queryName -> queryContent
      }.foreach { case (name, sql) =>
        try {
          val result = spark.sql(sql).collect()
          val schema = spark.sql(sql).schema
          val schemaDDL = schema.toDDL + "\n"
          spark.createDataFrame(result.toList.asJava, schema).createTempView(s"$name$viewSuffix")
          val sumHashResult =
            spark.sql(s"select sum(hash(*)) from $name$viewSuffix").collect().head.get(0)

          val goldenFile = Paths.get(
            baseResourcePath.toFile.getAbsolutePath,
            "tpch",
            "schema",
            s"${name.stripSuffix(".sql")}.output.schema")

          if (regenerateGoldenFiles) {
            Files.write(goldenFile, schemaDDL.getBytes)
          }
          val expectedSchema = fileToString(goldenFile)

          assert(schemaDDL == expectedSchema)
          assert(sumHashResult == queryToSumHash(name))

        } catch {
          case cause: Throwable =>
            fail(name, cause)
        }
      }
    }
  }
}
