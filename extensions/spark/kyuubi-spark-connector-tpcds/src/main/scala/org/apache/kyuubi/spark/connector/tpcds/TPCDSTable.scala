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

import java.util
import java.util.Optional

import scala.collection.JavaConverters._

import io.trino.tpcds.Table
import io.trino.tpcds.column._
import io.trino.tpcds.column.ColumnType.Base._
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table => SparkTable, TableCapability}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class TPCDSTable(tbl: String, scale: Int, options: CaseInsensitiveStringMap)
  extends SparkTable with SupportsRead {

  // When true, use CHAR VARCHAR; otherwise use STRING
  val useAnsiStringType: Boolean = options.getBoolean("useAnsiStringType", false)

  // 09-26-2017 v2.6.0
  // Replaced two occurrences of "c_last_review_date" with "c_last_review_date_sk" to be consistent
  // with Table 2-14 (Customer Table Column Definitions) in section 2.4.7 of the specification
  // (fogbugz 2046).
  //
  // https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v3.2.0.pdf
  val useTableSchema_2_6: Boolean = options.getBoolean("useTableSchema_2_6", true)

  // #2543, one split represents the number of rows generated per ten seconds
  // in TPCDSTableGenerateBenchmark
  val splitPerTask: Int = options.getInt("splitPerTask", 1)

  val tpcdsTable: Table = Table.getTable(tbl)

  override def name: String = s"sf$scale.$tbl"

  override def toString: String = s"TPCDSTable($name)"

  override def schema: StructType = {
    def nullable(index: Int): Boolean = {
      val bitMask = 1L << index
      (bitMask & ~tpcdsTable.getNotNullBitMap) != 0
    }
    StructType(
      tpcdsTable.getColumns.zipWithIndex.map { case (c, i) =>
        // Because the order of `GeneratorColumn` and `Column` of some tables is inconsistent,
        // we need to revise the index of null column, in order to be consistent
        // with the calculation of null column in the getValues method of Row.
        // Like: io.trino.tpcds.row.CallCenterRow.getValues
        val index = TPCDSSchemaUtils.reviseNullColumnIndex(tpcdsTable, i)
        StructField(
          TPCDSSchemaUtils.reviseColumnName(c, useTableSchema_2_6),
          toSparkDataType(c.getType),
          nullable(index))
      })
  }

  override def partitioning: Array[Transform] = TPCDSSchemaUtils
    .tablePartitionColumnNames(tpcdsTable, useTableSchema_2_6)
    .map { Expressions.identity }

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new TPCDSBatchScan(tpcdsTable, scale, splitPerTask, schema)

  def toSparkDataType(tpcdsType: ColumnType): DataType = {
    (tpcdsType.getBase, tpcdsType.getPrecision.asScala, tpcdsType.getScale.asScala) match {
      case (INTEGER, None, None) => IntegerType
      case (IDENTIFIER, None, None) => LongType
      case (DATE, None, None) => DateType
      case (DECIMAL, Some(precision), Some(scale)) => DecimalType(precision, scale)
      case (VARCHAR, Some(precision), None) =>
        if (useAnsiStringType) VarcharType(precision) else StringType
      case (CHAR, Some(precision), None) =>
        if (useAnsiStringType) CharType(precision) else StringType
      case (t, po, so) =>
        throw new IllegalArgumentException(s"Unsupported TPC-DS type: ($t, $po, $so)")
    }
  }

  implicit final class RichOptional[T](val optional: Optional[T]) {
    def asScala: Option[T] = optional match {
      case null => null
      case _ => if (optional.isPresent) Option(optional.get) else None
    }
  }
}
