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

package org.apache.kyuubi.it.gluten.tpch

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.tags.Slow

import org.apache.kyuubi.{GlutenSuiteMixin, KyuubiFunSuite}
import org.apache.kyuubi.it.gluten.TPCUtils.loadTPCFile
import org.apache.kyuubi.spark.connector.common.GoldenFileUtils.LICENSE_HEADER
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession
import org.apache.kyuubi.spark.connector.tpch.TPCHCatalog
import org.apache.kyuubi.tags.GlutenTest

@Slow
@GlutenTest
class GlutenTPCHQuerySuite extends KyuubiFunSuite with GlutenSuiteMixin {
  // TODO: Fix the inconsistency in q3/q9 results.
  val queries: Set[String] = (1 to 22).map(i => s"q$i").toSet -- Seq("q3", "q9")

  lazy val sparkConf: SparkConf = {
    val glutenConf = new SparkConf().setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpch", classOf[TPCHCatalog].getName)
    extraConfigs.foreach { case (k, v) => glutenConf.set(k, v) }
    glutenConf
  }

  test("KYUUBI #5467:gluten tpc-h tiny query suite") {
    val viewSuffix = "view"
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      loadTPCHTINY(spark)
      queries.map { queryName =>
        queryName -> loadTPCFile(s"kyuubi/tpch/$queryName.sql")
      }.foreach { case (name, sql) =>
        val result = spark.sql(sql).collect()
        val schema = spark.sql(sql).schema
        val schemaDDL = LICENSE_HEADER + schema.toDDL + "\n"
        spark.createDataFrame(result.toList.asJava, schema).createTempView(s"$name$viewSuffix")
        val sumHashResult = LICENSE_HEADER + spark.sql(
          s"select sum(hash(*)) from $name$viewSuffix").collect().head.get(0) + "\n"
        val expectHash = loadTPCFile(s"kyuubi/tpch/$name.output.hash")
        val expectSchema = loadTPCFile(s"kyuubi/tpch/$name.output.schema")
        assert(schemaDDL == expectSchema, s"query $name schema not match")
        assert(sumHashResult == expectHash, s"query $name result not match")
      }
    }
  }

  def loadTPCHTINY(sc: SparkSession): Unit = {
    val queryContent: String = loadTPCFile("load-tpch-tiny.sql")
    queryContent.split(";\n").filterNot(_.trim.isEmpty).foreach { sql =>
      sc.sql(sql)
    }
  }
}
