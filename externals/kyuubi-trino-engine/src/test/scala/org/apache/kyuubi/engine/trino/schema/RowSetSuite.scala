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

package org.apache.kyuubi.engine.trino.schema

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.Date
import java.sql.Time

import scala.collection.JavaConverters._

import io.trino.client.ClientStandardTypes._
import io.trino.client.ClientTypeSignature
import io.trino.client.Column
import io.trino.client.Row

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.engine.trino.schema.RowSet.toHiveString
import org.apache.kyuubi.engine.trino.util.TestUtils._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TProtocolVersion

class RowSetSuite extends KyuubiFunSuite {

  final private val UUID_PREFIX = "486bb66f-1206-49e3-993f-0db68f3cd8"

  def genRow(value: Int): List[_] = {
    val boolVal = value % 3 match {
      case 0 => true
      case 1 => false
      case _ => null
    }
    val byteVal = value.toByte
    val shortVal = value.toShort
    val longVal = value.toLong
    val charVal = String.format("%10s", value.toString)
    val floatVal = java.lang.Float.valueOf(s"$value.$value")
    val doubleVal = java.lang.Double.valueOf(s"$value.$value")
    val stringVal = value.toString * value
    val decimalVal = new java.math.BigDecimal(s"$value.$value").toPlainString
    val dayOrTime = java.lang.String.format("%02d", java.lang.Integer.valueOf(value + 1))
    val dateVal = s"2018-11-$dayOrTime"
    val timeVal = s"13:33:$dayOrTime"
    val timestampVal = s"2018-11-17 13:33:33.$value"
    val timestampWithZoneVal = s"2018-11-17 13:33:33.$value Asia/Shanghai"
    val binaryVal = Array.fill[Byte](value)(value.toByte)
    val arrVal = Array.fill(value)(doubleVal).toList.asJava
    val mapVal = Map(value -> doubleVal).asJava
    val jsonVal = s"""{"$value": $value}"""
    val rowVal = Row.builder()
      .addField("", value.toString)
      .addField("", mapVal).build()
    val ipVal = s"${value}.${value}.${value}.${value}"
    val uuidVal = java.util.UUID.fromString(
      s"$UUID_PREFIX${uuidSuffix(value)}")

    List(
      longVal,
      value,
      shortVal,
      byteVal,
      boolVal,
      dateVal,
      decimalVal,
      floatVal,
      doubleVal,
      timestampVal,
      timestampWithZoneVal,
      timeVal,
      binaryVal,
      stringVal,
      charVal,
      rowVal,
      arrVal,
      mapVal,
      jsonVal,
      ipVal,
      uuidVal)
  }

  val schema: List[Column] = List(
    column("a", BIGINT),
    column("b", INTEGER),
    column("c", SMALLINT),
    column("d", TINYINT),
    column("e", BOOLEAN),
    column("f", DATE),
    column("g", DECIMAL),
    column("h", REAL),
    column("i", DOUBLE),
    column("j", TIMESTAMP),
    column("k", TIMESTAMP_WITH_TIME_ZONE),
    column("l", TIME),
    column("m", VARBINARY),
    column("n", VARCHAR),
    column("o", CHAR),
    column("p", ROW, rowTypeSignature),
    column("q", ARRAY, arrayTypeSignature),
    column("r", MAP, mapTypeSignature),
    column("s", JSON),
    column("t", IPADDRESS),
    column("u", UUID))

  private val rows: Seq[List[_]] = (0 to 10).map(genRow) ++ Seq(List.fill(21)(null))

  def column(name: String, tp: String): Column = column(name, tp, new ClientTypeSignature(tp))

  def column(name: String, tp: String, signature: ClientTypeSignature): Column = {
    new Column(name, tp, signature)
  }

  def uuidSuffix(value: Int): String = if (value > 9) value.toString else s"f$value"

  test("column based set") {
    val tRowSet = new TrinoTRowSetGenerator().toColumnBasedSet(rows, schema)
    assert(tRowSet.getColumns.size() === schema.size)
    assert(tRowSet.getRowsSize === 0)

    val cols = tRowSet.getColumns.iterator()

    val longCol = cols.next().getI64Val
    longCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === 0)
      case (b, i) => assert(b === i)
    }

    val intCol = cols.next().getI32Val
    intCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === 0)
      case (b, i) => assert(b === i)
    }

    val shortCol = cols.next().getI16Val
    shortCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === 0)
      case (b, i) => assert(b === i)
    }

    val byteCol = cols.next().getByteVal
    byteCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === 0)
      case (b, i) => assert(b === i)
    }

    val boolCol = cols.next().getBoolVal
    assert(boolCol.getValuesSize === rows.size)
    boolCol.getValues.asScala.zipWithIndex.foreach { case (b, i) =>
      i % 3 match {
        case 0 => assert(b)
        case 1 => assert(!b)
        case _ => assert(b)
      }
    }

    val dateCol = cols.next().getStringVal
    dateCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) =>
        assert(b === toHiveString(Date.valueOf(s"2018-11-${i + 1}"), new ClientTypeSignature(DATE)))
    }

    val decCol = cols.next().getStringVal
    decCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) => assert(b === s"$i.$i")
    }

    val floatCol = cols.next().getDoubleVal
    floatCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === 0)
      case (b, i) => assert(b.toFloat === java.lang.Float.valueOf(s"$i.$i"))
    }

    val doubleCol = cols.next().getDoubleVal
    doubleCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === 0)
      case (b, i) => assert(b === java.lang.Double.valueOf(s"$i.$i"))
    }

    val timestampCol = cols.next().getStringVal
    timestampCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) => assert(b === s"2018-11-17 13:33:33.$i")
    }

    val timestampWithZoneCol = cols.next().getStringVal
    timestampWithZoneCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) => assert(b === s"2018-11-17 13:33:33.$i Asia/Shanghai")
    }

    val timeCol = cols.next().getStringVal
    timeCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) => assert(b ===
          toHiveString(Time.valueOf(s"13:33:${i + 1}"), new ClientTypeSignature(TIME)))
    }

    val binCol = cols.next().getBinaryVal
    binCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === ByteBuffer.allocate(0))
      case (b, i) => assert(b === ByteBuffer.wrap(Array.fill[Byte](i)(i.toByte)))
    }

    val strCol = cols.next().getStringVal
    strCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) => assert(b === i.toString * i)
    }

    val charCol = cols.next().getStringVal
    charCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) => assert(b === String.format("%10s", i.toString))
    }

    val rowCol = cols.next().getStringVal
    rowCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b.isEmpty)
      case (b, i) => assert(b === toHiveString(
          Row.builder().addField("", i.toString).addField(
            "",
            Map(i -> java.lang.Double.valueOf(s"$i.$i")).asJava).build(),
          rowTypeSignature))
    }

    val arrCol = cols.next().getStringVal
    arrCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === "")
      case (b, i) => assert(b === toHiveString(
          Array.fill(i)(java.lang.Double.valueOf(s"$i.$i")).toList.asJava,
          arrayTypeSignature))
    }

    val mapCol = cols.next().getStringVal
    mapCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === "")
      case (b, i) => assert(b === toHiveString(
          Map(i -> java.lang.Double.valueOf(s"$i.$i")).asJava,
          mapTypeSignature))
    }

    val jsonCol = cols.next().getStringVal
    jsonCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === "")
      case (b, i) => assert(b ===
          toHiveString(s"""{"$i": $i}""", new ClientTypeSignature(JSON)))
    }

    val ipCol = cols.next().getStringVal
    ipCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === "")
      case (b, i) => assert(b ===
          toHiveString(s"${i}.${i}.${i}.${i}", new ClientTypeSignature(IPADDRESS)))
    }

    val uuidCol = cols.next().getStringVal
    uuidCol.getValues.asScala.zipWithIndex.foreach {
      case (b, 11) => assert(b === "")
      case (b, i) => assert(b ===
          toHiveString(s"$UUID_PREFIX${uuidSuffix(i)}", new ClientTypeSignature(UUID)))
    }
  }

  test("row based set") {
    val tRowSet = new TrinoTRowSetGenerator().toRowBasedSet(rows, schema)
    assert(tRowSet.getColumnCount === 0)
    assert(tRowSet.getRowsSize === rows.size)
    val iter = tRowSet.getRowsIterator

    val r1 = iter.next().getColVals
    assert(r1.get(0).getI64Val.getValue === 0)
    assert(r1.get(4).getBoolVal.isValue)

    val r2 = iter.next().getColVals
    assert(r2.get(1).getI32Val.getValue === 1)
    assert(!r2.get(4).getBoolVal.isValue)

    val r3 = iter.next().getColVals
    assert(r3.get(2).getI16Val.getValue == 2)
    assert(!r3.get(4).getBoolVal.isValue)

    val r4 = iter.next().getColVals
    assert(r4.get(3).getByteVal.getValue == 3)

    val r5 = iter.next().getColVals
    assert(r5.get(5).getStringVal.getValue === "2018-11-05")
    assert(r5.get(6).getStringVal.getValue === "4.4")

    val r6 = iter.next().getColVals
    assert(r6.get(7).getDoubleVal.getValue === 5.5)
    assert(r6.get(8).getDoubleVal.getValue === 5.5)

    val r7 = iter.next().getColVals
    assert(r7.get(9).getStringVal.getValue === "2018-11-17 13:33:33.6")
    assert(r7.get(10).getStringVal.getValue === "2018-11-17 13:33:33.6 Asia/Shanghai")

    val r8 = iter.next().getColVals
    assert(r8.get(11).getStringVal.getValue === "13:33:08")

    val r9 = iter.next().getColVals
    assert(r9.get(12).getStringVal.getValue === new String(
      Array.fill[Byte](8)(8.toByte),
      StandardCharsets.UTF_8))
    assert(r9.get(13).getStringVal.getValue === "8" * 8)
    assert(r9.get(14).getStringVal.getValue === String.format(s"%10s", 8.toString))

    val r10 = iter.next().getColVals
    val mapStr =
      Map(9 -> 9.9d).map { case (key, value) => s"$key:$value" }.toSeq.mkString("{", ",", "}")
    assert(r10.get(15).getStringVal.getValue ===
      String.format("{foo=\"%s\",bar=%s}", "9", mapStr))
    assert(r10.get(16).getStringVal.getValue === Array.fill(9)(9.9d).mkString("[", ",", "]"))
    assert(r10.get(17).getStringVal.getValue === mapStr)
    assert(r10.get(18).getStringVal.getValue === "{\"9\": 9}")
    assert(r10.get(19).getStringVal.getValue === "9.9.9.9")
    assert(r10.get(20).getStringVal.getValue === s"$UUID_PREFIX${uuidSuffix(9)}")
  }

  test("to row set") {
    TProtocolVersion.values().foreach { proto =>
      val set = new TrinoTRowSetGenerator().toTRowSet(rows, schema, proto)
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
