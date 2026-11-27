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

  test("large scale partitions match native dsdgen counts and first-row keys") {
    val sparkConf = new SparkConf().setMaster("local[1]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.catalogImplementation", "in-memory")
    withSparkSession(SparkSession.builder.config(sparkConf).getOrCreate()) { spark =>
      val conf = TPCDSConf(spark, CaseInsensitiveStringMap.empty())
      val keyColumns = Map(
        Table.STORE_SALES -> ("ss_ticket_number", "ss_item_sk", "ss_sold_date_sk"),
        Table.STORE_RETURNS -> ("sr_ticket_number", "sr_item_sk", "sr_returned_date_sk"),
        Table.CATALOG_SALES -> ("cs_order_number", "cs_item_sk", "cs_sold_date_sk"),
        Table.CATALOG_RETURNS -> ("cr_order_number", "cr_item_sk", "cr_returned_date_sk"),
        Table.WEB_SALES -> ("ws_order_number", "ws_item_sk", "ws_sold_date_sk"),
        Table.WEB_RETURNS -> ("wr_order_number", "wr_item_sk", "wr_returned_date_sk"))
      // Native dsdgen -scale 10000 -table store_sales -parallel 24000000 -child 23000001.
      // At SF30000 and SF100000, use 100 orders per chunk and select the penultimate chunk.
      // dsdgen writes both the sales and corresponding returns table for each sales command.
      val samples = Seq(
        (10000, Table.STORE_SALES, 24000000, 23000001, 1176, (2300000001L, 205451L, 2452217L)),
        (10000, Table.STORE_RETURNS, 24000000, 23000001, 121, (2300000001L, 14211L, 2452355L)),
        (30000, Table.STORE_SALES, 72000000, 71999999, 1160, (7199999801L, 212533L, 2452640L)),
        (30000, Table.STORE_RETURNS, 72000000, 71999999, 125, (7199999801L, 63085L, 2452738L)),
        (30000, Table.CATALOG_SALES, 48000000, 47999999, 863, (4799999801L, 362637L, 2452654L)),
        (30000, Table.CATALOG_RETURNS, 48000000, 47999999, 80, (4799999802L, 352399L, 2452699L)),
        (30000, Table.WEB_SALES, 18000000, 17999999, 1195, (1799999801L, 200725L, 2451765L)),
        (30000, Table.WEB_RETURNS, 18000000, 17999999, 113, (1799999803L, 434260L, 2451821L)),
        (
          100000,
          Table.STORE_SALES,
          240000000,
          239999999,
          1154,
          (23999999801L, 388081L, 2452252L)),
        (
          100000,
          Table.STORE_RETURNS,
          240000000,
          239999999,
          120,
          (23999999801L, 186049L, 2452331L)),
        (
          100000,
          Table.CATALOG_SALES,
          160000000,
          159999999,
          893,
          (15999999801L, 270073L, 2452654L)),
        (
          100000,
          Table.CATALOG_RETURNS,
          160000000,
          159999999,
          98,
          (15999999801L, 270073L, 2452803L)),
        (100000, Table.WEB_SALES, 60000000, 59999999, 1160, (5999999801L, 498363L, 2452374L)),
        (100000, Table.WEB_RETURNS, 60000000, 59999999, 121, (5999999802L, 432290L, 2451818L)))
      samples.foreach { case (scale, table, parallelism, chunk, expectedCount, expectedFirstKeys) =>
        val schema = new TPCDSTable(table.getName, scale, conf).schema
        val (orderCol, itemCol, dateCol) = keyColumns(table)
        val orderIdx = schema.fieldIndex(orderCol)
        val itemIdx = schema.fieldIndex(itemCol)
        val dateIdx = schema.fieldIndex(dateCol)
        val reader = new TPCDSPartitionReader(table.getName, scale, parallelism, chunk, schema)
        var count = 0
        var firstKeys: Option[(Long, Long, Long)] = None
        try {
          while (reader.next()) {
            if (firstKeys.isEmpty) {
              val row = reader.get()
              firstKeys = Some((row.getLong(orderIdx), row.getLong(itemIdx), row.getLong(dateIdx)))
            }
            count += 1
          }
        } finally {
          reader.close()
        }
        val sampleLabel = s"${table.getName} SF$scale chunk $chunk/$parallelism"
        assert(count == expectedCount, sampleLabel)
        assert(firstKeys.contains(expectedFirstKeys), sampleLabel)
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
      def planScan(tableName: String, maxBytes: Long): TPCDSBatchScan = {
        spark.conf.set(s"$TPCDS_CONNECTOR_READ_CONF_PREFIX.$MAX_PARTITION_BYTES_CONF", maxBytes)
        val df = spark.sql(s"select * from $tableName")
        val scan = df.queryExecution.executedPlan.collectFirst {
          case scanExec: BatchScanExec if scanExec.scan.isInstanceOf[TPCDSBatchScan] =>
            scanExec.scan.asInstanceOf[TPCDSBatchScan]
        }
        assert(scan.isDefined)
        scan.get
      }

      def assertPartitions(
          tableName: String,
          maxBytes: Long,
          expected: Int,
          checkNonEmpty: Boolean = false): Unit = {
        val scan = planScan(tableName, maxBytes)
        val partitions = scan.planInputPartitions
        assert(partitions.length == expected)
        if (checkNonEmpty) {
          val readerFactory = scan.createReaderFactory
          partitions.foreach { partition =>
            val reader = readerFactory.createReader(partition)
            try {
              assert(reader.next(), s"Expected non-empty partition: $partition")
            } finally {
              reader.close()
            }
          }
        }
      }

      val sf10Size = TPCDSStatisticsUtils.sizeInBytes(Table.STORE_SALES, 10)
      assertPartitions("tpcds.sf10.store_sales", sf10Size / 4, 4)
      assertPartitions("tpcds.sf10.store_sales", sf10Size / 4 - 1, 5, checkNonEmpty = true)
      assertPartitions("tpcds.sf10.store_sales", sf10Size / 4 + 1, 4)
      assertPartitions("tpcds.sf10.store_sales", sf10Size * 2, 2)
      assert(planScan("tpcds.sf10.store_sales", 1024).parallelism == 2400000)
      assert(planScan("tpcds.sf10.store_sales", 1).parallelism == 2400000)
      assertPartitions("tpcds.sf1.store_sales", 1, 1, checkNonEmpty = true)
      assertPartitions("tpcds.sf0.store_sales", sf10Size, 1)
      assertPartitions("tpcds.sf1.call_center", 1, 1, checkNonEmpty = true)

      spark.conf.set(s"$TPCDS_CONNECTOR_READ_CONF_PREFIX.$MAX_PARTITION_BYTES_CONF", 0L)
      Seq("tpcds.sf10.store_sales", "tpcds.sf1.store_sales", "tpcds.sf1.call_center")
        .foreach { tableName =>
          intercept[IllegalArgumentException] {
            spark.sql(s"select * from $tableName").queryExecution.executedPlan
          }
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
