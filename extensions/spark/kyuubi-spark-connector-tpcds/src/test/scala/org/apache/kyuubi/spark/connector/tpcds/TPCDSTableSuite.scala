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

import scala.collection.JavaConverters._

import io.trino.tpcds.{Options, Table}
import io.trino.tpcds.generator.CallCenterGeneratorColumn
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.spark.connector.common.LocalSparkSession.withSparkSession
import org.apache.kyuubi.spark.connector.tpcds.TPCDSConf._

class TPCDSTableSuite extends KyuubiFunSuite {

  Seq(Table.STORE_SALES, Table.STORE_RETURNS).foreach { table =>
    test(s"${table.getName} generation across the 32-bit row number boundary") {
      val sparkConf = new SparkConf().setMaster("local[1]")
        .set("spark.ui.enabled", "false")
        .set("spark.sql.catalogImplementation", "in-memory")
      withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
        val options = new Options
        options.table = table.getName
        options.scale = 10000
        val session = options.toSession
        val conf = TPCDSConf(spark, CaseInsensitiveStringMap.empty())
        val schema = new TPCDSTable(table.getName, options.scale, conf).schema
        val firstRow = Int.MaxValue.toLong - 2
        val lastRow = firstRow + 20

        def rows(first: Long, last: Long): Vector[InternalRow] = {
          new KyuubiResultsIterator(table, first, last, session, schema)
            .asScala.map(_.copy()).toVector
        }

        // Generate across the boundary without seeking past it, then compare with split reads.
        val expected = rows(firstRow, lastRow)
        assert(expected.nonEmpty)
        Seq(-1, 0, 1, 2, 10).foreach { offset =>
          val splitRow = Int.MaxValue.toLong + offset
          val actual = rows(firstRow, splitRow - 1) ++ rows(splitRow, lastRow)
          assertResult(expected.size, s"split at $splitRow")(actual.size)
          actual.zip(expected).foreach { case (actualRow, expectedRow) =>
            assert(actualRow == expectedRow, s"split at $splitRow")
          }
        }
      }
    }
  }

  test("large scale partition counts match native dsdgen") {
    val sparkConf = new SparkConf().setMaster("local[1]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      val conf = TPCDSConf(spark, CaseInsensitiveStringMap.empty())
      // Native dsdgen -scale 10000 -table store_sales -parallel 24000000 -child 23000001.
      // At SF30000 and SF100000, use 100 orders per chunk and select the penultimate chunk.
      // dsdgen writes both the sales and corresponding returns table for each sales command.
      val samples = Seq(
        (10000, Table.STORE_SALES, 24000000, 23000001, 1176),
        (10000, Table.STORE_RETURNS, 24000000, 23000001, 121),
        (30000, Table.STORE_SALES, 72000000, 71999999, 1160),
        (30000, Table.STORE_RETURNS, 72000000, 71999999, 125),
        (30000, Table.CATALOG_SALES, 48000000, 47999999, 863),
        (30000, Table.CATALOG_RETURNS, 48000000, 47999999, 80),
        (30000, Table.WEB_SALES, 18000000, 17999999, 1195),
        (30000, Table.WEB_RETURNS, 18000000, 17999999, 113),
        (100000, Table.STORE_SALES, 240000000, 239999999, 1154),
        (100000, Table.STORE_RETURNS, 240000000, 239999999, 120),
        (100000, Table.CATALOG_SALES, 160000000, 159999999, 893),
        (100000, Table.CATALOG_RETURNS, 160000000, 159999999, 98),
        (100000, Table.WEB_SALES, 60000000, 59999999, 1160),
        (100000, Table.WEB_RETURNS, 60000000, 59999999, 121))
      samples.foreach { case (scale, table, parallelism, chunk, expected) =>
        val schema = new TPCDSTable(table.getName, scale, conf).schema
        val reader = new TPCDSPartitionReader(table.getName, scale, parallelism, chunk, schema)
        var count = 0
        try {
          while (reader.next()) {
            count += 1
          }
        } finally {
          reader.close()
        }
        assert(count == expected, s"${table.getName} SF$scale chunk $chunk/$parallelism")
      }
    }
  }

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

  test("maxPartitionBytes rounds up and respects default parallelism") {
    val sparkConf = new SparkConf().setMaster("local[2]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
      .set("spark.sql.catalog.tpcds", classOf[TPCDSCatalog].getName)
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      def assertPartitions(tableName: String, maxBytes: Long, expected: Int): Unit = {
        spark.conf.set(s"$TPCDS_CONNECTOR_READ_CONF_PREFIX.$MAX_PARTITION_BYTES_CONF", maxBytes)
        val df = spark.sql(s"select * from $tableName")
        val scan = df.queryExecution.executedPlan.collectFirst {
          case scanExec: BatchScanExec if scanExec.scan.isInstanceOf[TPCDSBatchScan] =>
            scanExec.scan.asInstanceOf[TPCDSBatchScan]
        }
        assert(scan.isDefined)
        assert(scan.get.planInputPartitions.length == expected)
      }

      val size = TPCDSStatisticsUtils.sizeInBytes(Table.STORE_SALES, 1)
      assertPartitions("tpcds.sf1.store_sales", size / 4, 4)
      assertPartitions("tpcds.sf1.store_sales", size / 4 - 1, 5)
      assertPartitions("tpcds.sf1.store_sales", size / 4 + 1, 4)
      assertPartitions("tpcds.sf1.store_sales", size * 2, 2)
      assertPartitions("tpcds.sf0.store_sales", size, 2)
      assertPartitions("tpcds.sf1.call_center", 1, 1)

      spark.conf.set(s"$TPCDS_CONNECTOR_READ_CONF_PREFIX.$MAX_PARTITION_BYTES_CONF", 0L)
      intercept[IllegalArgumentException] {
        spark.sql("select * from tpcds.sf1.store_sales").queryExecution.executedPlan
      }
    }
  }

  test("all identifier columns use LongType") {
    val sparkConf = new SparkConf().setMaster("local[1]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      val conf = TPCDSConf(spark, CaseInsensitiveStringMap.empty())
      var identifierCount = 0
      TPCDSSchemaUtils.BASE_TABLES.foreach { table =>
        val schema = new TPCDSTable(table.getName, 1, conf).schema
        schema.fields.foreach { field =>
          val isIdentifier = field.name.endsWith("_sk") ||
            field.name.endsWith("_ticket_number") ||
            field.name.endsWith("_order_number")
          if (isIdentifier) {
            identifierCount += 1
            assert(field.dataType == LongType, s"${table.getName}.${field.name}")
          }
        }
      }
      assert(identifierCount == 127)
    }
  }
}
