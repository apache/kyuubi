/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.schema

import java.nio.ByteBuffer
import java.sql.Timestamp
import java.util.BitSet

import scala.collection.mutable

import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Row, SparkSQLUtils}
import org.apache.spark.sql.types._

class ColumnBasedSetSuite extends SparkFunSuite {
  val maxRows: Int = 5
  val schema = new StructType().add("a", "int").add("b", "string")
  val rows = Seq(
    Row(1, "11"),
    Row(2, "22"),
    Row(3, "33"),
    Row(4, "44"),
    Row(5, "55"),
    Row(6, "66"),
    Row(7, "77"),
    Row(8, "88"),
    Row(9, "99"),
    Row(10, "000"),
    Row(11, "111"),
    Row(12, "222"),
    Row(13, "333"),
    Row(14, "444"),
    Row(15, "555"),
    Row(16, "666"))

  val listIter = rows.iterator

  var arrayIter = rows.toArray.iterator


  test("row set basic suites with list to iterator") {
    // fetch next

    var taken = listIter.take(maxRows).toSeq
    var tRowSet = ColumnBasedSet(schema, taken).toTRowSet
    assert(tRowSet.getColumns.size() === 2)
    assert(tRowSet.getRowsSize === 0)
    assert(tRowSet.getColumns.get(0).getI32Val.getValuesSize === 5)
    assert(tRowSet.getColumns.get(0).getI32Val.getValues.get(0).intValue() === 1)
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(1) === "22")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(2) === "33")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(3) === "44")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(4) === "55")

    taken = listIter.take(maxRows).toSeq
    tRowSet = ColumnBasedSet(schema, taken).toTRowSet
    assert(tRowSet.getColumns.get(0).getI32Val.getValues.get(0).intValue() === 6)
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(1) === "77")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(2) === "88")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(3) === "99")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(4) === "000")

    taken = listIter.take(maxRows).toSeq
    tRowSet = ColumnBasedSet(schema, taken).toTRowSet
    assert(tRowSet.getColumns.get(0).getI32Val.getValues.get(0).intValue() === 11)
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(1) === "222")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(2) === "333")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(3) === "444")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(4) === "555")

    taken = listIter.take(maxRows).toSeq
    tRowSet = ColumnBasedSet(schema, taken).toTRowSet
    assert(tRowSet.getColumns.get(0).getI32Val.getValues.get(0).intValue() === 16)
    intercept[IndexOutOfBoundsException](tRowSet.getColumns.get(1).getStringVal.getValues.get(1))

    assert(listIter.isEmpty)
  }

  test("row set null value test with iterator") {
    val schema = new StructType()
      .add("a", "int")
      .add("b", "string")
      .add("c", "boolean")
      .add("d", "long")
      .add("e", "short")
      .add("f", "double")
    val rows = Seq(
      Row(1, "11", true, 1L, 1.toShort, 1.0),
      Row(2, "22", null, 2L, 2.toShort, 2.0),
      Row(3, null, false, null, 3.toShort, 3.0),
      Row(4, "44", false, 4L, null, 4.0),
      Row(5, null, true, null, null, null))

    var iter = rows.toIterator.toSeq
    var tRowSet = ColumnBasedSet(schema, iter).toTRowSet

    val columnA = tRowSet.getColumns.get(0).getI32Val.getValues
    assert(columnA.get(0) === 1)
    assert(columnA.get(1) === 2)
    assert(columnA.get(2) === 3)
    assert(columnA.get(3) === 4)
    assert(columnA.get(4) === 5)

    val columnB = tRowSet.getColumns.get(1).getStringVal.getValues
    assert(columnB.get(0) === "11")
    assert(columnB.get(1) === "22")
    assert(columnB.get(2) === "")
    assert(columnB.get(3) === "44")
    assert(columnB.get(4) === "")

    val columnC = tRowSet.getColumns.get(2).getBoolVal.getValues
    assert(columnC.get(0) === true)
    assert(columnC.get(1) === true)
    assert(columnC.get(2) === false)
    assert(columnC.get(3) === false)
    assert(columnC.get(4) === true)

    val columnD = tRowSet.getColumns.get(3).getI64Val.getValues
    assert(columnD.get(0) === 1L)
    assert(columnD.get(1) === 2L)
    assert(columnD.get(2) === 0L)
    assert(columnD.get(3) === 4L)
    assert(columnD.get(4) === 0L)

    val columnE = tRowSet.getColumns.get(4).getI16Val.getValues
    assert(columnE.get(0) === 1.toShort)
    assert(columnE.get(1) === 2.toShort)
    assert(columnE.get(2) === 3.toShort)
    assert(columnE.get(3) === 0.toShort)
    assert(columnE.get(4) === 0.toShort)

    val columnF = tRowSet.getColumns.get(5).getDoubleVal.getValues
    assert(columnE.get(0) === 1.0)
    assert(columnE.get(1) === 2.0)
    assert(columnE.get(2) === 3.0)
    assert(columnE.get(3) === 0.toDouble)
    assert(columnE.get(4) === 0.toDouble)

    // byteBuffer 00000
    assert(tRowSet.getColumns.get(0).getI32Val.getNulls === Array[Byte]())
    // byteBuffer 10100
    assert(tRowSet.getColumns.get(1).getStringVal.getNulls === Array[Byte](20))
    // byteBuffer 00010
    assert(tRowSet.getColumns.get(2).getBoolVal.getNulls === Array[Byte](2))
    // byteBuffer 10100
    assert(tRowSet.getColumns.get(3).getI64Val.getNulls === Array[Byte](20))
    // byteBuffer 11000
    assert(tRowSet.getColumns.get(4).getI16Val.getNulls === Array[Byte](24))
    // byteBuffer 10000
    assert(tRowSet.getColumns.get(5).getDoubleVal.getNulls === Array[Byte](16))
  }

  test("kyuubi set to TRowSet then to Hive Row Set") {
    val rowIterator = rows.iterator
    val taken = rowIterator.take(maxRows).toSeq
    val tRowSet = ColumnBasedSet(schema, taken).toTRowSet

    val hiveRowSet = new org.apache.hive.service.cli.ColumnBasedSet(tRowSet)
    assert(hiveRowSet.getColumns.get(0).get(0).asInstanceOf[Int] === 1)
    assert(hiveRowSet.getColumns.get(1).get(0).equals("11"))
    assert(hiveRowSet.getColumns.get(1).get(4).equals("55"))
  }

  test("get global row iterator with array to iterator") {

    def getNextRowSet(maxRowsL: Long, ifDrop: Boolean = false): RowSet = {
      val taken = arrayIter.take(maxRowsL.toInt)
      if (ifDrop) arrayIter = arrayIter.drop(maxRowsL.toInt)
      RowSetBuilder.create(schema, taken.toList, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)
    }

    var tRowSet = getNextRowSet(5).toTRowSet
    assert(tRowSet.getColumns.size() === 2)
    assert(tRowSet.getRowsSize === 0)
    assert(tRowSet.getColumns.get(0).getI32Val.getValuesSize === 5)
    assert(tRowSet.getColumns.get(0).getI32Val.getValues.get(0).intValue() === 1)
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(1) === "22")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(2) === "33")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(3) === "44")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(4) === "55")

    tRowSet = getNextRowSet(5).toTRowSet
    assert(tRowSet.getColumns.get(0).getI32Val.getValues.get(0).intValue() === 1)

    tRowSet = getNextRowSet(5, ifDrop = true).toTRowSet
    assert(tRowSet.getColumns.get(0).getI32Val.getValues.get(0).intValue() === 1)

    tRowSet = getNextRowSet(5, ifDrop = true).toTRowSet
    assert(tRowSet.getColumns.get(0).getI32Val.getValues.get(0).intValue() === 6)
  }

  test("bool type null value tests") {
    val schema1 = new StructType().add("c1", "boolean")
    val rows = Seq(Row(null), Row(true), Row(false), Row(null))
    val tRowSet = ColumnBasedSet(schema1, rows).toTRowSet
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getBoolVal.getNulls).get(0))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getBoolVal.getNulls).get(1))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getBoolVal.getNulls).get(2))
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getBoolVal.getNulls).get(3))

    assert(tRowSet.getColumns.get(0).getBoolVal.getValues.get(0).booleanValue())
    assert(tRowSet.getColumns.get(0).getBoolVal.getValues.get(1).booleanValue())
    assert(!tRowSet.getColumns.get(0).getBoolVal.getValues.get(2).booleanValue())
    assert(tRowSet.getColumns.get(0).getBoolVal.getValues.get(3).booleanValue())
    val tRowSet2 = ColumnBasedSet(schema1, Seq.empty[Row]).toTRowSet
    assert(tRowSet2.getColumns.get(0).getBoolVal.getValues.size() === 0)
  }

  test("byte type null value tests") {
    val schema1 = new StructType().add("c1", "byte")
    val rows = Seq(Row(null), Row(1.toByte), Row(2.toByte), Row(null))
    val tRowSet = ColumnBasedSet(schema1, rows).toTRowSet
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getByteVal.getNulls).get(0))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getByteVal.getNulls).get(1))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getByteVal.getNulls).get(2))
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getByteVal.getNulls).get(3))

    assert(tRowSet.getColumns.get(0).getByteVal.getValues.get(0).byteValue() === 0)
    assert(tRowSet.getColumns.get(0).getByteVal.getValues.get(1).byteValue() === 1)
    assert(tRowSet.getColumns.get(0).getByteVal.getValues.get(2).byteValue() === 2)
    assert(tRowSet.getColumns.get(0).getByteVal.getValues.get(3).byteValue() === 0)
    val tRowSet2 = ColumnBasedSet(schema1, Seq.empty[Row]).toTRowSet
    assert(tRowSet2.getColumns.get(0).getByteVal.getValues.size() === 0)
  }

  test("short type null value tests") {
    val schema1 = new StructType().add("c1", "short")
    val rows = Seq(Row(null), Row(1.toShort), Row(2.toShort), Row(null))
    val tRowSet = ColumnBasedSet(schema1, rows).toTRowSet
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getI16Val.getNulls).get(0))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getI16Val.getNulls).get(1))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getI16Val.getNulls).get(2))
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getI16Val.getNulls).get(3))

    assert(tRowSet.getColumns.get(0).getI16Val.getValues.get(0).byteValue() === 0)
    assert(tRowSet.getColumns.get(0).getI16Val.getValues.get(1).byteValue() === 1)
    assert(tRowSet.getColumns.get(0).getI16Val.getValues.get(2).byteValue() === 2)
    assert(tRowSet.getColumns.get(0).getI16Val.getValues.get(3).byteValue() === 0)

    val tRowSet2 = ColumnBasedSet(schema1, Seq.empty[Row]).toTRowSet
    assert(tRowSet2.getColumns.get(0).getI16Val.getValues.size() === 0)
  }

  test("int type null value tests") {
    val schema1 = new StructType().add("c1", "int")
    val rows = Seq(Row(null), Row(1), Row(2), Row(null))
    val tRowSet = ColumnBasedSet(schema1, rows).toTRowSet
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getI32Val.getNulls).get(0))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getI32Val.getNulls).get(1))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getI32Val.getNulls).get(2))
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getI32Val.getNulls).get(3))

    assert(tRowSet.getColumns.get(0).getI32Val.getValues.get(0).byteValue() === 0)
    assert(tRowSet.getColumns.get(0).getI32Val.getValues.get(1).byteValue() === 1)
    assert(tRowSet.getColumns.get(0).getI32Val.getValues.get(2).byteValue() === 2)
    assert(tRowSet.getColumns.get(0).getI32Val.getValues.get(3).byteValue() === 0)
    val tRowSet2 = ColumnBasedSet(schema1, Seq.empty[Row]).toTRowSet
    assert(tRowSet2.getColumns.get(0).getI32Val.getValues.size() === 0)
  }

  test("long type null value tests") {
    val schema1 = new StructType().add("c1", "long")
    val rows = Seq(Row(null), Row(1L), Row(2L), Row(null))
    val tRowSet = ColumnBasedSet(schema1, rows).toTRowSet
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getI64Val.getNulls).get(0))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getI64Val.getNulls).get(1))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getI64Val.getNulls).get(2))
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getI64Val.getNulls).get(3))

    assert(tRowSet.getColumns.get(0).getI64Val.getValues.get(0).byteValue() === 0)
    assert(tRowSet.getColumns.get(0).getI64Val.getValues.get(1).byteValue() === 1)
    assert(tRowSet.getColumns.get(0).getI64Val.getValues.get(2).byteValue() === 2)
    assert(tRowSet.getColumns.get(0).getI64Val.getValues.get(3).byteValue() === 0)
    val tRowSet2 = ColumnBasedSet(schema1, Seq.empty[Row]).toTRowSet
    assert(tRowSet2.getColumns.get(0).getI64Val.getValues.size() === 0)
  }

  test("float type null value tests") {
    val schema1 = new StructType().add("c1", "float")
    val rows = Seq(Row(null), Row(1.0f), Row(2.0f), Row(null))
    val tRowSet = ColumnBasedSet(schema1, rows).toTRowSet
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getDoubleVal.getNulls).get(0))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getDoubleVal.getNulls).get(1))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getDoubleVal.getNulls).get(2))
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getDoubleVal.getNulls).get(3))

    assert(tRowSet.getColumns.get(0).getDoubleVal.getValues.get(0).byteValue() === 0)
    assert(tRowSet.getColumns.get(0).getDoubleVal.getValues.get(1).byteValue() === 1)
    assert(tRowSet.getColumns.get(0).getDoubleVal.getValues.get(2).byteValue() === 2)
    assert(tRowSet.getColumns.get(0).getDoubleVal.getValues.get(3).byteValue() === 0)
    val tRowSet2 = ColumnBasedSet(schema1, Seq.empty[Row]).toTRowSet
    assert(tRowSet2.getColumns.get(0).getDoubleVal.getValues.size() === 0)
  }

  test("double type null value tests") {
    val schema1 = new StructType().add("c1", "double")
    val rows = Seq(Row(null), Row(1.0d), Row(2.0d), Row(null))
    val tRowSet = ColumnBasedSet(schema1, rows).toTRowSet
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getDoubleVal.getNulls).get(0))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getDoubleVal.getNulls).get(1))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getDoubleVal.getNulls).get(2))
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getDoubleVal.getNulls).get(3))

    assert(tRowSet.getColumns.get(0).getDoubleVal.getValues.get(0).byteValue() === 0)
    assert(tRowSet.getColumns.get(0).getDoubleVal.getValues.get(1).byteValue() === 1)
    assert(tRowSet.getColumns.get(0).getDoubleVal.getValues.get(2).byteValue() === 2)
    assert(tRowSet.getColumns.get(0).getDoubleVal.getValues.get(3).byteValue() === 0)
    val tRowSet2 = ColumnBasedSet(schema1, Seq.empty[Row]).toTRowSet
    assert(tRowSet2.getColumns.get(0).getDoubleVal.getValues.size() === 0)
  }

  test("string type null value tests") {
    val schema1 = new StructType().add("c1", "string")
    val rows = Seq(Row(null), Row("a"), Row(""), Row(null))
    val tRowSet = ColumnBasedSet(schema1, rows).toTRowSet
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(0))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(1))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(2))
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(3))

    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(0) === "")
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(1) === "a")
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(2) === "")
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(3) === "")
    val tRowSet2 = ColumnBasedSet(schema1, Seq.empty[Row]).toTRowSet
    assert(tRowSet2.getColumns.get(0).getStringVal.getValues.size() === 0)
  }

  test("binary type null value tests") {
    val schema1 = new StructType().add("c1", BinaryType)
    val v1 = Array(1.toByte)
    val v2 = Array(2.toByte)
    val rows = Seq(Row(null), Row(v1), Row(v2), Row(null))
    val tRowSet = ColumnBasedSet(schema1, rows).toTRowSet
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getBinaryVal.getNulls).get(0))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getBinaryVal.getNulls).get(1))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getBinaryVal.getNulls).get(2))
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getBinaryVal.getNulls).get(3))

    assert(tRowSet.getColumns.get(0).getBinaryVal.getValues.get(0) === ByteBuffer.allocate(0))
    assert(tRowSet.getColumns.get(0).getBinaryVal.getValues.get(1) === ByteBuffer.wrap(v1))
    assert(tRowSet.getColumns.get(0).getBinaryVal.getValues.get(2) === ByteBuffer.wrap(v2))
    assert(tRowSet.getColumns.get(0).getBinaryVal.getValues.get(3) === ByteBuffer.allocate(0))
    val tRowSet2 = ColumnBasedSet(schema1, Seq.empty[Row]).toTRowSet
    assert(tRowSet2.getColumns.get(0).getBinaryVal.getValues.size() === 0)
  }

  test("date type null value tests") {
    val schema1 = new StructType().add("c1", DateType)
    val v1 = "2018-03-12"
    val v2 = "2010-03-13"
    val rows = Seq(Row(null), Row(v1), Row(v2), Row(null))
    val tRowSet = ColumnBasedSet(schema1, rows).toTRowSet
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(0))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(1))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(2))
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(3))

    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(0) === "")
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(1) === v1)
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(2) === v2)
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(3) === "")
    val tRowSet2 = ColumnBasedSet(schema1, Seq.empty[Row]).toTRowSet
    assert(tRowSet2.getColumns.get(0).getStringVal.getValues.size() === 0)
  }

  test("timestamp type null value tests") {
    val schema1 = new StructType().add("c1", TimestampType)
    val v1 = new Timestamp(System.currentTimeMillis())
    val v2 = "2018-03-30 17:59:59"
    val rows = Seq(Row(null), Row(v1), Row(v2), Row(null))
    val tRowSet = ColumnBasedSet(schema1, rows).toTRowSet
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(0))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(1))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(2))
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(3))

    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(0) === "")
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(1) ===
      SparkSQLUtils.toHiveString((v1, schema1.head.dataType)))
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(2) === v2)
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(3) === "")
    val tRowSet2 = ColumnBasedSet(schema1, Seq.empty[Row]).toTRowSet
    assert(tRowSet2.getColumns.get(0).getStringVal.getValues.size() === 0)
  }

  test("decimal type null value tests") {
    val schema1 = new StructType().add("c1", "decimal(5, 1)")
    val v1 = new java.math.BigDecimal("100.12")
    val v2 = new java.math.BigDecimal("200.34")
    val rows = Seq(Row(null), Row(v1), Row(v2), Row(null))
    val tRowSet = ColumnBasedSet(schema1, rows).toTRowSet
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(0))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(1))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(2))
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(3))

    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(0) === "")
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(1) ===
      SparkSQLUtils.toHiveString((v1, schema1.head.dataType)))
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(2) ===
      SparkSQLUtils.toHiveString((v2, schema1.head.dataType)))
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(3) === "")
    val tRowSet2 = ColumnBasedSet(schema1, Seq.empty[Row]).toTRowSet
    assert(tRowSet2.getColumns.get(0).getStringVal.getValues.size() === 0)
  }

  test("array type null value tests") {
    val schema1 = new StructType().add("c1", "array<int>")
    val v1 = mutable.WrappedArray.make(Array(1, 2))
    val v2 = mutable.WrappedArray.make(Array(3, 4))
    val rows = Seq(Row(null), Row(v1), Row(v2), Row(null))
    val tRowSet = ColumnBasedSet(schema1, rows).toTRowSet
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(0))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(1))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(2))
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(3))

    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(0) === "")
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(1) ===
      SparkSQLUtils.toHiveString((v1, schema1.head.dataType)))
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(2) ===
      SparkSQLUtils.toHiveString((v2, schema1.head.dataType)))
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(3) === "")
    val tRowSet2 = ColumnBasedSet(schema1, Seq.empty[Row]).toTRowSet
    assert(tRowSet2.getColumns.get(0).getStringVal.getValues.size() === 0)
  }

  test("map type null value tests") {
    val schema1 = new StructType().add("c1", "map<int, array<string>>")
    val v1 = Map(1 -> mutable.WrappedArray.make(Array(1, 2)))
    val v2 = Map(2 -> mutable.WrappedArray.make(Array(3, 4)))
    val rows = Seq(Row(null), Row(v1), Row(v2), Row(null))
    val tRowSet = ColumnBasedSet(schema1, rows).toTRowSet
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(0))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(1))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(2))
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(3))

    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(0) === "")
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(1) ===
      SparkSQLUtils.toHiveString((v1, schema1.head.dataType)))
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(2) ===
      SparkSQLUtils.toHiveString((v2, schema1.head.dataType)))
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(3) === "")
    val tRowSet2 = ColumnBasedSet(schema1, Seq.empty[Row]).toTRowSet
    assert(tRowSet2.getColumns.get(0).getStringVal.getValues.size() === 0)
  }

  test("ss type null value tests") {
    val schema1 = new StructType().add("c1", "struct<a: int, b:string>")
    val v1 = Row(1, "12")
    val v2 = Row(2, "22")
    val rows = Seq(Row(null), Row(v1), Row(v2), Row(null))
    val tRowSet = ColumnBasedSet(schema1, rows).toTRowSet
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(0))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(1))
    assert(!BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(2))
    assert(BitSet.valueOf(tRowSet.getColumns.get(0).getStringVal.getNulls).get(3))

    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(0) === "")
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(1) ===
      SparkSQLUtils.toHiveString((v1, schema1.head.dataType)))
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(2) ===
      SparkSQLUtils.toHiveString((v2, schema1.head.dataType)))
    assert(tRowSet.getColumns.get(0).getStringVal.getValues.get(3) === "")
    val tRowSet2 = ColumnBasedSet(schema1, Seq.empty[Row]).toTRowSet
    assert(tRowSet2.getColumns.get(0).getStringVal.getValues.size() === 0)
  }

  test("row is null") {
    assert(!ColumnBasedSet(new StructType(), null).toTRowSet.isSetColumns)
  }
}
