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

package org.apache.kyuubi.it.gluten.tpcds

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.tags.Slow

import org.apache.kyuubi.{GlutenSuiteMixin, KyuubiFunSuite}
import org.apache.kyuubi.it.gluten.TPCUtils.loadTPCFile
import org.apache.kyuubi.spark.connector.common.GoldenFileUtils.LICENSE_HEADER
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession
import org.apache.kyuubi.spark.connector.tpcds.TPCDSCatalog
import org.apache.kyuubi.tags.GlutenTest

@Slow
@GlutenTest
class GlutenTPCDSQuerySuite extends KyuubiFunSuite with GlutenSuiteMixin {

  val queries: Set[String] = (1 to 99).map(i => s"q$i").toSet -
    ("q14", "q23", "q24", "q39") +
    ("q14a", "q14b", "q23a", "q23b", "q24a", "q24b", "q39a", "q39b") -
    // TODO:Fix gluten tpc-ds query test
    ("q1", "q4", "q7", "q11", "q12", "q17", "q20", "q21", "q25", "q26", "q29", "q30", "q34", "q37",
    "q39a", "q39b", "q40", "q43", "q46", "q49", "q56", "q58", "q59", "q60", "q68", "q73", "q74",
    "q78", "q79", "q81", "q82", "q83", "q84", "q91", "q98")
  lazy val sparkConf: SparkConf = {
    val glutenConf = new SparkConf().setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
      .set("spark.sql.catalog.tpcds.useTableSchema_2_6", "true")
    extraConfigs.foreach { case (k, v) => glutenConf.set(k, v) }
    glutenConf
  }

  test("KYUUBI #5467:gluten tpc-ds tiny query suite") {
    val viewSuffix = "view"
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      loadTPDSTINY(spark)
      queries.map { queryName =>
        queryName -> loadTPCFile(s"kyuubi/tpcds_3.2/$queryName.sql")
      }.foreach { case (name, sql) =>
        try {
          val result = spark.sql(sql).collect()
          val schema = spark.sql(sql).schema
          val schemaDDL = LICENSE_HEADER + schema.toDDL + "\n"
          spark.createDataFrame(result.toList.asJava, schema).createTempView(s"$name$viewSuffix")
          val sumHashResult = LICENSE_HEADER + spark.sql(
            s"select sum(hash(*)) from $name$viewSuffix").collect().head.get(0) + "\n"
          val expectHash = loadTPCFile(s"kyuubi/tpcds_3.2/$name.output.hash")
          val expectSchema = loadTPCFile(s"kyuubi/tpcds_3.2/$name.output.schema")
          assert(schemaDDL == expectSchema)
          assert(sumHashResult == expectHash)
        } catch {
          case cause: Throwable =>
            fail(name, cause)
        }
      }
    }
  }

  def loadTPDSTINY(sc: SparkSession): Unit = {
    val queryContent: String = loadTPCFile("load-tpcds-tiny.sql")
    queryContent.split(";\n").filterNot(_.trim.isEmpty).foreach { sql =>
      sc.sql(sql)
    }
  }
}
