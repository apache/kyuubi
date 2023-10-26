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

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.collection.JavaConverters._
import scala.io.{Codec, Source}

import io.trino.tpcds.`type`.Date
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DateType, StructField, StructType}
import org.scalatest.tags.Slow

import org.apache.kyuubi.{KyuubiFunSuite, Utils}
import org.apache.kyuubi.spark.connector.common.GoldenFileUtils._
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession

/**
 * To run this test suite:
 * {{{
 *   KYUUBI_UPDATE=0 dev/gen/gen_tpcds_queries.sh
 * }}}
 *
 * To re-generate golden files for this suite:
 * {{{
 *   dev/gen/gen_tpcds_queries.sh
 * }}}
 */
@Slow
class TPCDSQuerySuite extends KyuubiFunSuite {

  val queries: Set[String] = (1 to 99).map(i => s"q$i").toSet -
    ("q14", "q23", "q24", "q39") +
    ("q14a", "q14b", "q23a", "q23b", "q24a", "q24b", "q39a", "q39b")

  test("run query on tiny") {
    val viewSuffix = "view"
    val sparkConf = new SparkConf().setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
      .set("spark.sql.catalog.tpcds.useTableSchema_2_6", "true")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      spark.sql("USE tpcds.tiny")
      queries.map { queryName =>
        val in = Utils.getContextOrKyuubiClassLoader
          .getResourceAsStream(s"kyuubi/tpcds_3.2/$queryName.sql")
        val queryContent: String = Source.fromInputStream(in)(Codec.UTF8).mkString
        in.close()
        queryName -> queryContent
      }.foreach { case (name, sql) =>
        try {
          val result = spark.sql(sql).collect()
          val schema = spark.sql(sql).schema
          val schemaDDL = LICENSE_HEADER + schema.toDDL + "\n"
          spark.createDataFrame(result.toList.asJava, schema).createTempView(s"$name$viewSuffix")
          val sumHashResult = LICENSE_HEADER + spark.sql(
            s"select sum(hash(*)) from $name$viewSuffix").collect().head.get(0) + "\n"
          val tuple = generateGoldenFiles("kyuubi/tpcds_3.2", name, schemaDDL, sumHashResult)
          assert(schemaDDL == tuple._1)
          assert(sumHashResult == tuple._2)
        } catch {
          case cause: Throwable =>
            fail(name, cause)
        }
      }
    }
  }

  test("aa") {
//    val d = new Decimal(1234, 3)
//    println(d)
//    val d2 = org.apache.spark.sql.types.Decimal(1234L, 200, 3)
////    val d2 = org.apache.spark.sql.types.Decimal(100, 8, 2)
//    println(d2)
//
//    println(new Decimal(0, 7))
//    println(org.apache.spark.sql.types.Decimal(0, 2, 7))
//    println(org.apache.spark.sql.types.Decimal("0.0000000"))
//    println(org.apache.spark.sql.types.Decimal(388831L, 7, 2))
//    println(Long.MaxValue.toString.size)
//    println(org.apache.spark.sql.types.Decimal(123456L, 7, 2))
//    println(org.apache.spark.sql.types.Decimal(-5, 5, 2))
//    println(new Decimal(-5, 5))
//    println(new Decimal(-5, 2))
//    println(null.asInstanceOf[Long])
//    println(org.apache.spark.sql.types.Decimal(-500, 5, 2))
//    println(org.apache.spark.sql.types.Decimal("-5"))
//    val ddd = org.apache.spark.sql.types.Decimal(-5)
//    ddd.changePrecision(5, 2)
//    println(ddd)
//    println(org.apache.spark.sql.types.Decimal(""))
//    println(org.apache.spark.sql.types.Decimal(123456789L, 7, 2))

    val dateFmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val i = 2450815
    val d = Date.fromJulianDays(i)
    println(d.toString)
    val i2 = LocalDate.parse(d.toString, dateFmt).toEpochDay.toInt
    println(i2)
    val r = InternalRow(i2)
    val schema = StructType(Seq(StructField("d", DateType)))
//    val r = new GenericRowWithSchema(Array(i2), schema)
//    Date
    val sparkConf = new SparkConf().setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
      .set("spark.sql.catalog.tpcds.useTableSchema_2_6", "true")
//    val list = Seq(r).asJava
//    val list = new util.ArrayList[Row]()
//    list.add(r)
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      (spark.sparkContext.makeRDD(Seq(r)), schema)
    }
  }
}
