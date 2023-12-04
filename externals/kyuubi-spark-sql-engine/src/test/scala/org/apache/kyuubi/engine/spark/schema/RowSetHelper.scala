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

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

trait RowSetHelper {
  protected def genRow(value: Int): Row = {
    val boolVal = value % 3 match {
      case 0 => true
      case 1 => false
      case _ => null
    }
    val byteVal = value.toByte
    val shortVal = value.toShort
    val longVal = value.toLong
    val floatVal = java.lang.Float.valueOf(s"$value.$value")
    val doubleVal = java.lang.Double.valueOf(s"$value.$value")
    val stringVal = value.toString * value
    val decimalVal = new java.math.BigDecimal(s"$value.$value")
    val day = java.lang.String.format("%02d", java.lang.Integer.valueOf(value % 30 + 1))
    val dateVal = Date.valueOf(s"2018-11-$day")
    val timestampVal = Timestamp.valueOf(s"2018-11-17 13:33:33.$value")
    val binaryVal = Array.fill[Byte](value)(value.toByte)
    val arrVal = Array.fill(10)(doubleVal).toSeq
    val mapVal = Map(value -> doubleVal)
    val interval = new CalendarInterval(value, value, value)
    val localDate = LocalDate.of(2018, 11, 17)
    val instant = Instant.now()

    Row(
      boolVal,
      byteVal,
      shortVal,
      value,
      longVal,
      floatVal,
      doubleVal,
      stringVal,
      decimalVal,
      dateVal,
      timestampVal,
      binaryVal,
      arrVal,
      mapVal,
      interval,
      localDate,
      instant)
  }

  protected val schemaStructFields: Seq[StructField] = Seq(
    ("a", "boolean", "boolVal"),
    ("b", "tinyint", "byteVal"),
    ("c", "smallint", "shortVal"),
    ("d", "int", "value"),
    ("e", "bigint", "longVal"),
    ("f", "float", "floatVal"),
    ("g", "double", "doubleVal"),
    ("h", "string", "stringVal"),
    ("i", "decimal", "decimalVal"),
    ("j", "date", "dateVal"),
    ("k", "timestamp", "timestampVal"),
    ("l", "binary", "binaryVal"),
    ("m", "array<double>", "arrVal"),
    ("n", "map<int, double>", "mapVal"),
    ("o", "interval", "interval"),
    ("p", "date", "localDate"),
    ("q", "timestamp", "instant"))
    .map { case (colName, typeName, comment) =>
      StructField(colName, CatalystSqlParser.parseDataType(typeName)).withComment(comment)
    }

  protected val schema: StructType = StructType(schemaStructFields)
}
