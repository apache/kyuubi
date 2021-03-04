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

package org.apache.kyuubi.schema

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.schema.RowSet.toHiveString

class RowSetSuite extends KyuubiFunSuite {

  def genRow(value: Int): Row = {
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
    val day = java.lang.String.format("%02d", java.lang.Integer.valueOf(value + 1))
    val dateVal = Date.valueOf(s"2018-11-$day")
    val timestampVal = Timestamp.valueOf(s"2018-11-17 13:33:33.$value")
    val binaryVal = Array.fill[Byte](value)(value.toByte)
    val arrVal = Array.fill(value)(doubleVal).toSeq
    val mapVal = Map(value -> doubleVal)
    val interval = new CalendarInterval(value, value, value)

    Row(boolVal,
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
      interval)
  }

  val schema: StructType = new StructType()
    .add("a", "boolean")
    .add("b", "tinyint")
    .add("c", "smallint")
    .add("d", "int")
    .add("e", "bigint")
    .add("f", "float")
    .add("g", "double")
    .add("h", "string")
    .add("i", "decimal")
    .add("j", "date")
    .add("k", "timestamp")
    .add("l", "binary")
    .add("m", "array<double>")
    .add("n", "map<int, double>")
    .add("o", "interval")


  val rows: Seq[Row] = (0 to 10).map(genRow) ++ Seq(
    Row(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null))

  test("column based set") {
    val tRowSet = RowSet.toColumnBasedSet(rows, schema)
    assert(tRowSet.getColumns.size() === schema.size)
    assert(tRowSet.getRowsSize === 0)

    val cols = tRowSet.getColumns.iterator()
    val boolCol = cols.next().getBoolVal
    assert(boolCol.getValuesSize === rows.size)
    boolCol.getValues.asScala.zipWithIndex.foreach { case (b, i) =>
      i % 3 match {
        case 0 => assert(b)
        case 1 => assert(!b)
        case _ => assert(b)
      }
    }

    val byteCol = cols.next().getByteVal
    byteCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === 0)
      case (b, i) => assert(b === i)
    }

    val shortCol = cols.next().getI16Val
    shortCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === 0)
      case (b, i) => assert(b === i)
    }

    val intCol = cols.next().getI32Val
    intCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === 0)
      case (b, i) => assert(b === i)
    }

    val longCol = cols.next().getI64Val
    longCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === 0)
      case (b, i) => assert(b === i)
    }

    val floatCol = cols.next().getDoubleVal
    floatCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === 0)
      case (b, i) => assert(b === java.lang.Float.valueOf(s"$i.$i"))
    }

    val doubleCol = cols.next().getDoubleVal
    doubleCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === 0)
      case (b, i) => assert(b === java.lang.Double.valueOf(s"$i.$i"))
    }

    val strCol = cols.next().getStringVal
    strCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) => assert(b === i.toString * i)
    }

    val decCol = cols.next().getStringVal
    decCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) => assert(b === s"$i.$i")
    }

    val dateCol = cols.next().getStringVal
    dateCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) =>
        assert(b === toHiveString((Date.valueOf(s"2018-11-${i + 1}"), DateType)))
    }

    val tsCol = cols.next().getStringVal
    tsCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) => assert(b ===
        toHiveString((Timestamp.valueOf(s"2018-11-17 13:33:33.$i"), TimestampType)))
    }

    val binCol = cols.next().getBinaryVal
    binCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === ByteBuffer.allocate(0))
      case (b, i) => assert(b === ByteBuffer.wrap(Array.fill[Byte](i)(i.toByte)))
    }

    val arrCol = cols.next().getStringVal
    arrCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === "")
      case (b, i) => assert(b === toHiveString(
        (Array.fill(i)(java.lang.Double.valueOf(s"$i.$i")).toSeq, ArrayType(DoubleType))))
    }

    val mapCol = cols.next().getStringVal
    mapCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === "")
      case (b, i) => assert(b === toHiveString(
        (Map(i -> java.lang.Double.valueOf(s"$i.$i")), MapType(IntegerType, DoubleType))))
    }

    val intervalCol = cols.next().getStringVal
    intervalCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === "")
      case (b, i) => assert(b === new CalendarInterval(i, i, i).toString)
    }
  }

  test("row based set") {
    val tRowSet = RowSet.toRowBasedSet(rows, schema)
    assert(tRowSet.getColumnCount === 0)
    assert(tRowSet.getRowsSize === rows.size)
    val iter = tRowSet.getRowsIterator

    val r1 = iter.next().getColVals
    assert(r1.get(0).getBoolVal.isValue)
    assert(r1.get(1).getByteVal.getValue === 0)

    val r2 = iter.next().getColVals
    assert(!r2.get(0).getBoolVal.isValue)
    assert(r2.get(2).getI16Val.getValue === 1)

    val r3 = iter.next().getColVals
    assert(!r3.get(0).getBoolVal.isValue)
    assert(r3.get(3).getI32Val.getValue === 2)

    val r4 = iter.next().getColVals
    assert(r4.get(4).getI64Val.getValue === 3)
    assert(r4.get(5).getDoubleVal.getValue === 3.3f)

    val r5 = iter.next().getColVals
    assert(r5.get(6).getDoubleVal.getValue === 4.4d)
    assert(r5.get(7).getStringVal.getValue === "4" * 4)

    val r6 = iter.next().getColVals
    assert(r6.get(8).getStringVal.getValue === "5.5")
    assert(r6.get(9).getStringVal.getValue === "2018-11-06")

    val r7 = iter.next().getColVals
    assert(r7.get(10).getStringVal.getValue === "2018-11-17 13:33:33.600")
    assert(r7.get(11).getStringVal.getValue === new String(
      Array.fill[Byte](6)(6.toByte), StandardCharsets.UTF_8))

    val r8 = iter.next().getColVals
    assert(r8.get(12).getStringVal.getValue === Array.fill(7)(7.7d).mkString("[", ",", "]"))
    assert(r8.get(13).getStringVal.getValue ===
      toHiveString((Map(7 -> 7.7d), MapType(IntegerType, DoubleType))))

    val r9 = iter.next().getColVals
    assert(r9.get(14).getStringVal.getValue === new CalendarInterval(8, 8, 8).toString)
  }

  test("to row set") {
    TProtocolVersion.values().foreach { proto =>
      val set = RowSet.toTRowSet(rows, schema, proto)
      if (proto.getValue < TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
        assert(!set.isSetColumns, proto.toString)
        assert(set.isSetRows, proto.toString)
      } else {
        assert(set.isSetColumns, proto.toString)
        assert(set.isSetRows, proto.toString)
      }
    }
  }
}
