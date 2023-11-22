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

import scala.collection.JavaConverters._
import scala.io.{Codec, Source}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.tags.Slow

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.spark.connector.common.GoldenFileUtils._
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession

/**
 * To run this test suite:
 * {{{
 *   KYUUBI_UPDATE=0 dev/gen/gen_tpch_queries.sh
 * }}}
 *
 * To re-generate golden files for this suite:
 * {{{
 *   dev/gen/gen_tpch_queries.sh
 * }}}
 */
@Slow
class TPCHQuerySuite extends KyuubiFunSuite {

  val queries: List[String] = (1 to 22).map(i => s"q$i").toList

  test("run query on tiny") {
    val viewSuffix = "view"
    val sparkConf = new SparkConf().setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpch", classOf[TPCHCatalog].getName)
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      spark.sql("USE tpch.tiny")
      queries.map { queryName =>
        val in = Utils.getContextOrKyuubiClassLoader.getResourceAsStream(
          s"kyuubi/tpch/$queryName.sql")
        val queryContent: String = Source.fromInputStream(in)(Codec.UTF8).mkString
        in.close()
        queryName -> queryContent
      }.foreach { case (name, sql) =>
        val result = spark.sql(sql).collect()
        val schema = spark.sql(sql).schema
        val schemaDDL = LICENSE_HEADER + schema.toDDL + "\n"
        spark.createDataFrame(result.toList.asJava, schema).createTempView(s"$name$viewSuffix")
        val sumHashResult = LICENSE_HEADER + spark.sql(
          s"select sum(hash(*)) from $name$viewSuffix").collect().head.get(0) + "\n"
        val tuple = generateGoldenFiles("kyuubi/tpch", name, schemaDDL, sumHashResult)
        assert(schemaDDL == tuple._1, s"query $name schema not match")
        assert(sumHashResult == tuple._2, s"query $name result not match")
      }
    }
  }
}
