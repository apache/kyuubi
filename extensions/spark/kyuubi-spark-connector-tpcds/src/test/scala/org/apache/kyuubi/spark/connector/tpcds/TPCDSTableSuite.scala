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

import io.trino.tpcds.Table
import io.trino.tpcds.generator.CallCenterGeneratorColumn
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession
import org.apache.kyuubi.spark.connector.tpcds.TPCDSConf._

class TPCDSTableSuite extends KyuubiFunSuite {

  test("useAnsiStringType (true, false)") {
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

  test("test nullable column") {
    TPCDSSchemaUtils.BASE_TABLES.foreach { tpcdsTable =>
      val tableName = tpcdsTable.getName
      val sparkConf = new SparkConf().setMaster("local[*]")
        .set("spark.ui.enabled", "false")
        .set("spark.sql.catalogImplementation", "in-memory")
        .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
      withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
        val sparkTable = spark.table(s"tpcds.sf1.$tableName")
        var notNullBitMap = 0
        sparkTable.schema.fields.zipWithIndex.foreach { case (field, i) =>
          val index = TPCDSSchemaUtils.reviseNullColumnIndex(tpcdsTable, i)
          if (!field.nullable) {
            notNullBitMap |= 1 << index
          }
        }
        assert(tpcdsTable.getNotNullBitMap == notNullBitMap)
      }
    }
  }

  test("test reviseColumnIndex") {
    // io.trino.tpcds.row.CallCenterRow.getValues
    val getValuesColumns = Array(
      "CC_CALL_CENTER_SK",
      "CC_CALL_CENTER_ID",
      "CC_REC_START_DATE_ID",
      "CC_REC_END_DATE_ID",
      "CC_CLOSED_DATE_ID",
      "CC_OPEN_DATE_ID",
      "CC_NAME",
      "CC_CLASS",
      "CC_EMPLOYEES",
      "CC_SQ_FT",
      "CC_HOURS",
      "CC_MANAGER",
      "CC_MARKET_ID",
      "CC_MARKET_CLASS",
      "CC_MARKET_DESC",
      "CC_MARKET_MANAGER",
      "CC_DIVISION",
      "CC_DIVISION_NAME",
      "CC_COMPANY",
      "CC_COMPANY_NAME",
      "CC_STREET_NUMBER",
      "CC_STREET_NAME",
      "CC_STREET_TYPE",
      "CC_SUITE_NUMBER",
      "CC_CITY",
      "CC_ADDRESS",
      "CC_STATE",
      "CC_ZIP",
      "CC_COUNTRY",
      "CC_GMT_OFFSET",
      "CC_TAX_PERCENTAGE")
    Table.CALL_CENTER.getColumns.zipWithIndex.map {
      case (_, i) =>
        assert(TPCDSSchemaUtils.reviseNullColumnIndex(Table.CALL_CENTER, i) ==
          CallCenterGeneratorColumn.valueOf(getValuesColumns(i)).getGlobalColumnNumber -
          CallCenterGeneratorColumn.CC_CALL_CENTER_SK.getGlobalColumnNumber)
    }
  }

  test("test maxPartitionBytes") {
    val maxPartitionBytes: Long = 1 * 1024 * 1024L
    val sparkConf = new SparkConf().setMaster("local[*]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
      .set(
        s"$TPCDS_CONNECTOR_READ_CONF_PREFIX.$MAX_PARTITION_BYTES_CONF",
        String.valueOf(maxPartitionBytes))
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      val tableName = "catalog_returns"
      val table = Table.getTable(tableName)
      val scale = 100
      val df = spark.sql(s"select * from tpcds.sf$scale.$tableName")
      val scan = df.queryExecution.executedPlan.collectFirst {
        case scanExec: BatchScanExec if scanExec.scan.isInstanceOf[TPCDSBatchScan] =>
          scanExec.scan.asInstanceOf[TPCDSBatchScan]
      }
      assert(scan.isDefined)
      val expected =
        (TPCDSStatisticsUtils.sizeInBytes(table, scale) / maxPartitionBytes).ceil.toInt
      assert(scan.get.planInputPartitions.length == expected)
    }
  }
}
