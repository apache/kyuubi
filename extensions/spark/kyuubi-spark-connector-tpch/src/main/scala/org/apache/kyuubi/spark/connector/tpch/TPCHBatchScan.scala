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

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util
import java.util.OptionalLong

import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

import io.trino.tpch._
import io.trino.tpch.GenerateUtils.formatDate
import io.trino.tpch.TpchColumnType.Base._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.kyuubi.spark.connector.tpch.TPCHSchemaUtils.normalize

case class TPCHTableChuck(table: String, scale: Double, parallelism: Int, index: Int)
  extends InputPartition

class TPCHBatchScan(
    @transient table: TpchTable[_],
    scale: Double,
    schema: StructType,
    readConf: TPCHReadConf) extends ScanBuilder
  with SupportsReportStatistics with Batch with Serializable {

  private val _sizeInBytes: Long = TPCHStatisticsUtils.sizeInBytes(table, scale)

  private val _numRows: Long = TPCHStatisticsUtils.numRows(table, scale)

  private val parallelism: Int =
    if (table.equals(TpchTable.NATION) || table.equals(TpchTable.REGION)) 1
    else math.max(
      SparkSession.active.sparkContext.defaultParallelism,
      (_numRows / readConf.maxPartitionBytes).ceil.toInt)

  override def build: Scan = this

  override def toBatch: Batch = this

  override def description: String =
    s"Scan TPC-H ${TPCHSchemaUtils.dbName(scale)}.${table.getTableName}, " +
      s"count: ${_numRows}, parallelism: $parallelism"

  override def readSchema: StructType = schema

  override def planInputPartitions: Array[InputPartition] =
    (1 to parallelism).map { i =>
      TPCHTableChuck(table.getTableName, scale, parallelism, i)
    }.toArray

  def createReaderFactory: PartitionReaderFactory = (partition: InputPartition) => {
    val chuck = partition.asInstanceOf[TPCHTableChuck]
    new TPCHPartitionReader(chuck.table, chuck.scale, chuck.parallelism, chuck.index, schema)
  }

  override def estimateStatistics: Statistics = new Statistics {
    override def sizeInBytes: OptionalLong = OptionalLong.of(_sizeInBytes)
    override def numRows: OptionalLong = OptionalLong.of(_numRows)
  }

}

class TPCHPartitionReader(
    table: String,
    scale: Double,
    parallelism: Int,
    index: Int,
    schema: StructType) extends PartitionReader[InternalRow] {

  private val tpchTable = TpchTable.getTable(table)

  private val columns = tpchTable.getColumns
    .asInstanceOf[java.util.List[TpchColumn[TpchEntity]]]

  private lazy val dateFmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  private val iterator = {
    if (normalize(scale).equals("0")) {
      new util.Iterator[TpchEntity] {

        override def hasNext: Boolean = false

        override def next(): TpchEntity = throw new NoSuchElementException("next on empty iterator")
      }
    } else tpchTable.createGenerator(scale, index, parallelism).iterator
  }

  private var currentRow: InternalRow = _

  override def next(): Boolean = {
    val hasNext = iterator.hasNext
    if (hasNext) currentRow = {
      val row = iterator.next().asInstanceOf[TpchEntity]
      val rowValue = new ArrayBuffer[String]()
      columns.stream().forEach(column => {
        val baseType = column.getType.getBase
        var value: String = ""
        baseType match {
          case IDENTIFIER => value += column.getIdentifier(row)
          case INTEGER => value += column.getInteger(row)
          case DATE => value += column.getDate(row)
          case DOUBLE => value += column.getDouble(row)
          case VARCHAR => value += column.getString(row)
        }
        rowValue += value
      })
      val rowAny = new ArrayBuffer[Any]()
      rowValue.zipWithIndex.map { case (value, i) =>
        (value, schema(i).dataType) match {
          case (null, _) => null
          case ("", _) => null
          case (value, IntegerType) => rowAny += value.toInt
          case (value, LongType) => rowAny += value.toLong
          case (value, DoubleType) => rowAny += value.toDouble
          case (value, DateType) => rowAny += LocalDate.parse(formatDate(value.toInt), dateFmt)
              .toEpochDay.toInt
          case (value, StringType) => rowAny += UTF8String.fromString(value)
          case (value, CharType(_)) => rowAny += UTF8String.fromString(value)
          case (value, VarcharType(_)) => rowAny += UTF8String.fromString(value)
          case (value, DecimalType()) => rowAny += Decimal(value)
          case (value, dt) => throw new IllegalArgumentException(s"value: $value, type: $dt")
        }
      }
      InternalRow.fromSeq(rowAny)
    }
    hasNext
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {}

}
