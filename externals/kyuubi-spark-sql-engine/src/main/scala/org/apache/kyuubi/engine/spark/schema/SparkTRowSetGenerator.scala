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

package org.apache.kyuubi.engine.spark.schema

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.HiveResult
import org.apache.spark.sql.types._

import org.apache.kyuubi.engine.result.TRowSetGenerator
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

class SparkTRowSetGenerator
  extends TRowSetGenerator[StructType, Row, DataType] {

  // reused time formatters in single RowSet generation, see KYUUBI #5811
  private val tf = HiveResult.getTimeFormatters
  private val bf = RowSet.getBinaryFormatter

  override def getColumnSizeFromSchemaType(schema: StructType): Int = schema.length

  override def getColumnType(schema: StructType, ordinal: Int): DataType = schema(ordinal).dataType

  override def isColumnNullAt(row: Row, ordinal: Int): Boolean = row.isNullAt(ordinal)

  override def getColumnAs[T](row: Row, ordinal: Int): T = row.getAs[T](ordinal)

  override def toTColumn(rows: Seq[Row], ordinal: Int, typ: DataType): TColumn = {
    typ match {
      case BooleanType => asBooleanTColumn(rows, ordinal)
      case ByteType => asByteTColumn(rows, ordinal)
      case ShortType => asShortTColumn(rows, ordinal)
      case IntegerType => asIntegerTColumn(rows, ordinal)
      case LongType => asLongTColumn(rows, ordinal)
      case FloatType => asFloatTColumn(rows, ordinal)
      case DoubleType => asDoubleTColumn(rows, ordinal)
      case StringType => asStringTColumn(rows, ordinal)
      case BinaryType => asByteArrayTColumn(rows, ordinal)
      case _ =>
        val timeFormatters = tf
        val binaryFormatter = bf
        asStringTColumn(
          rows,
          ordinal,
          "NULL",
          (row, ordinal) =>
            RowSet.toHiveString(
              getColumnAs[Any](row, ordinal) -> typ,
              timeFormatters = timeFormatters,
              binaryFormatter = binaryFormatter))
    }
  }

  override def toTColumnValue(row: Row, ordinal: Int, types: StructType): TColumnValue = {
    getColumnType(types, ordinal) match {
      case BooleanType => asBooleanTColumnValue(row, ordinal)
      case ByteType => asByteTColumnValue(row, ordinal)
      case ShortType => asShortTColumnValue(row, ordinal)
      case IntegerType => asIntegerTColumnValue(row, ordinal)
      case LongType => asLongTColumnValue(row, ordinal)
      case FloatType => asFloatTColumnValue(row, ordinal)
      case DoubleType => asDoubleTColumnValue(row, ordinal)
      case StringType => asStringTColumnValue(row, ordinal)
      case _ => asStringTColumnValue(
          row,
          ordinal,
          rawValue =>
            RowSet.toHiveString(
              rawValue -> types(ordinal).dataType,
              timeFormatters = tf,
              binaryFormatter = bf))
    }
  }

}
