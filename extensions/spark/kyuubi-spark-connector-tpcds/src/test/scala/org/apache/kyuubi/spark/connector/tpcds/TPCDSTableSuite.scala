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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connector.tpcds.LocalSparkSession.withSparkSession

class TPCDSTableSuite extends KyuubiFunSuite {

  test("useAnsiStringType (true,false)") {
    Seq(true, false).foreach(key => {
      val sparkConf = new SparkConf().setMaster("local[*]")
        .set("spark.ui.enabled", "false")
        .set("spark.sql.catalogImplementation", "in-memory")
        .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
        .set("spark.sql.catalog.tpcds.useAnsiStringType", key.toString)
      withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
        val rows = spark.sql("desc tpcds.sf1.call_center").collect()
        rows.foreach(row => {
          val dataType = row.getString(1)
          row.getString(0) match {
            case "cc_call_center_id" =>
              if (key) {
                assert(dataType == "char(16)")
              } else {
                assert(dataType == "string")
              }
            case "cc_name" =>
              if (key) {
                assert(dataType == "varchar(50)")
              } else {
                assert(dataType == "string")
              }
            case _ =>
          }
        })
      }
    })
  }

  test("test rowCountPerTask") {
    val rowCountPerTask = 100
    val sparkConf = new SparkConf().setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
      .set("spark.sql.catalog.tpcds.rowCountPerTask", s"$rowCountPerTask")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      val rowCount = spark.table("tpcds.sf1.catalog_page").count
      val df = spark.sql("select * from tpcds.sf1.catalog_page")
      val tpcdsScan = df.queryExecution.executedPlan.collectFirst {
        case e: BatchScanExec if e.scan.isInstanceOf[TPCDSBatchScan] =>
          e.scan.asInstanceOf[TPCDSBatchScan]
      }
      assert(tpcdsScan.isDefined)
      val parallelism = (rowCount / rowCountPerTask.toDouble).ceil.toInt
      assert(tpcdsScan.get.planInputPartitions.length == parallelism)
    }
  }
}
