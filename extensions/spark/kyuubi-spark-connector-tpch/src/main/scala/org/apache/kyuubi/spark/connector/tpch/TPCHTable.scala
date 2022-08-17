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

import java.util
import java.util.Optional

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`

import io.trino.tpch.{TpchColumnType, TpchEntity, TpchTable}
import io.trino.tpch.TpchColumnType.Base._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table => SparkTable, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class TPCHTable(tbl: String, scale: Double, tpchConf: TPCHConf)
  extends SparkTable with SupportsRead {

  val tpchTable: TpchTable[_] = TpchTable.getTable(tbl)

  override def name: String = s"${TPCHSchemaUtils.dbName(scale)}.$tbl"

  override def toString: String = s"TPCHTable($name)"

  override def schema: StructType = {
    StructType(
      tpchTable.asInstanceOf[TpchTable[TpchEntity]].getColumns.zipWithIndex.map { case (c, _) =>
        StructField(c.getColumnName, toSparkDataType(c.getType))
      })
  }

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new TPCHBatchScan(tpchTable, scale, schema, TPCHReadConf(SparkSession.active, this, options))
  }

  def toSparkDataType(tpchType: TpchColumnType): DataType = {
    (tpchType.getBase, tpchType.getPrecision.asScala, tpchType.getScale.asScala) match {
      case (INTEGER, None, None) => IntegerType
      case (IDENTIFIER, None, None) => LongType
      case (DOUBLE, None, None) => DoubleType
      case (DATE, None, None) => DateType
      case (VARCHAR, Some(precision), None) =>
        if (tpchConf.useAnsiStringType) VarcharType(precision.toInt) else StringType
      case (t, po, so) =>
        throw new IllegalArgumentException(s"Unsupported TPC-H type: ($t, $po, $so)")
    }
  }

  implicit final class RichOptional[T](val optional: Optional[T]) {
    def asScala: Option[T] = optional match {
      case null => null
      case _ => if (optional.isPresent) Option(optional.get) else None
    }
  }
}
