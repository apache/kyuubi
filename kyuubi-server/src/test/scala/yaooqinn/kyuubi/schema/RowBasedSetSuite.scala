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

import java.sql.Timestamp

import scala.collection.mutable

import org.apache.hive.service.cli
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Row, SparkSQLUtils}
import org.apache.spark.sql.types.{BinaryType, DateType, StructType, TimestampType}

class RowBasedSetSuite extends SparkFunSuite {
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

  test("row set basic suites") {
    // fetch next
    val rowIterator = rows.iterator
    var taken = rowIterator.take(maxRows).toSeq
    var tRowSet = RowBasedSet(schema, taken).toTRowSet
    assert(tRowSet.getRowsSize === 5)
    assert(tRowSet.getRows.get(0).getColVals.get(1).getStringVal.getValue === "11")
    assert(tRowSet.getRows.get(1).getColVals.get(1).getStringVal.getValue === "22")
    assert(tRowSet.getRows.get(2).getColVals.get(1).getStringVal.getValue === "33")
    assert(tRowSet.getRows.get(3).getColVals.get(1).getStringVal.getValue === "44")
    assert(tRowSet.getRows.get(4).getColVals.get(1).getStringVal.getValue === "55")

    taken = rowIterator.take(maxRows).toSeq
    tRowSet = RowBasedSet(schema, taken).toTRowSet
    assert(tRowSet.getRowsSize === 5)
    assert(tRowSet.getRows.get(0).getColVals.get(1).getStringVal.getValue === "66")
    assert(tRowSet.getRows.get(1).getColVals.get(1).getStringVal.getValue === "77")
    assert(tRowSet.getRows.get(2).getColVals.get(1).getStringVal.getValue === "88")
    assert(tRowSet.getRows.get(3).getColVals.get(1).getStringVal.getValue === "99")
    assert(tRowSet.getRows.get(4).getColVals.get(1).getStringVal.getValue === "000")

    taken = rowIterator.take(maxRows).toSeq
    tRowSet = RowBasedSet(schema, taken).toTRowSet
    assert(tRowSet.getRowsSize === 5)
    assert(tRowSet.getRows.get(0).getColVals.get(1).getStringVal.getValue === "111")
    assert(tRowSet.getRows.get(1).getColVals.get(1).getStringVal.getValue === "222")
    assert(tRowSet.getRows.get(2).getColVals.get(1).getStringVal.getValue === "333")
    assert(tRowSet.getRows.get(3).getColVals.get(1).getStringVal.getValue === "444")
    assert(tRowSet.getRows.get(4).getColVals.get(1).getStringVal.getValue === "555")

    taken = rowIterator.take(maxRows).toSeq
    tRowSet = RowBasedSet(schema, taken).toTRowSet
    assert(tRowSet.getRowsSize === 1)
    assert(tRowSet.getRows.get(0).getColVals.get(1).getStringVal.getValue === "666")
    intercept[IndexOutOfBoundsException](tRowSet.getRows.get(1))

    assert(rowIterator.isEmpty)

    // fetch first
    val rowIterator2 = rows.iterator

    val (itr1, itr2) = rowIterator2.take(maxRows).duplicate
    val resultList = itr2.toList

    taken = itr1.toSeq
    tRowSet = RowBasedSet(schema, taken).toTRowSet
    assert(tRowSet.getRowsSize === 5)
    assert(tRowSet.getRows.get(0).getColVals.get(1).getStringVal.getValue === "11")
    assert(tRowSet.getRows.get(1).getColVals.get(1).getStringVal.getValue === "22")
    assert(tRowSet.getRows.get(2).getColVals.get(1).getStringVal.getValue === "33")
    assert(tRowSet.getRows.get(3).getColVals.get(1).getStringVal.getValue === "44")
    assert(tRowSet.getRows.get(4).getColVals.get(1).getStringVal.getValue === "55")

    taken = resultList
    tRowSet = RowBasedSet(schema, taken).toTRowSet
    assert(tRowSet.getRowsSize === 5)
    assert(tRowSet.getRows.get(0).getColVals.get(1).getStringVal.getValue === "11")
    assert(tRowSet.getRows.get(1).getColVals.get(1).getStringVal.getValue === "22")
    assert(tRowSet.getRows.get(2).getColVals.get(1).getStringVal.getValue === "33")
    assert(tRowSet.getRows.get(3).getColVals.get(1).getStringVal.getValue === "44")
    assert(tRowSet.getRows.get(4).getColVals.get(1).getStringVal.getValue === "55")

    taken = resultList
    tRowSet = RowBasedSet(schema, taken).toTRowSet
    assert(tRowSet.getRowsSize === 5)
    assert(tRowSet.getRows.get(0).getColVals.get(1).getStringVal.getValue === "11")
    assert(tRowSet.getRows.get(1).getColVals.get(1).getStringVal.getValue === "22")
    assert(tRowSet.getRows.get(2).getColVals.get(1).getStringVal.getValue === "33")
    assert(tRowSet.getRows.get(3).getColVals.get(1).getStringVal.getValue === "44")
    assert(tRowSet.getRows.get(4).getColVals.get(1).getStringVal.getValue === "55")
  }

  test("kyuubi row set to TRowSet then to hive row set") {
    val rowIterator = rows.iterator
    val taken = rowIterator.take(maxRows).toSeq
    val tRowSet = RowBasedSet(schema, taken).toTRowSet

    val hiveSet = new cli.RowBasedSet(tRowSet)
    assert(hiveSet.numRows() === 5)
    assert(hiveSet.numColumns() === 2)
    val hiveRowIter = hiveSet.iterator()
    val row1 = hiveRowIter.next().iterator
    assert(row1.next().asInstanceOf[Int] === 1)
    assert(row1.next().equals("11"))
    val row2 = hiveRowIter.next().iterator
    assert(row2.next().asInstanceOf[Int] === 2)
    assert(row2.next().equals("22"))
    val row3 = hiveRowIter.next().iterator
    assert(row3.next().asInstanceOf[Int] === 3)
    assert(row3.next().equals("33"))
    val row4 = hiveRowIter.next().iterator
    assert(row4.next().asInstanceOf[Int] === 4)
    assert(row4.next().equals("44"))
    val row5 = hiveRowIter.next().iterator
    assert(row5.next().asInstanceOf[Int] === 5)
    assert(row5.next().equals("55"))
  }

  test("bool type null value tests") {
    val schema1 = new StructType().add("c1", "boolean")
    val rows = Seq(Row(null), Row(true), Row(false), Row(null))
    val tRowSet = RowBasedSet(schema1, rows).toTRowSet
    assert(!tRowSet.getRows.get(0).getColVals.get(0).getBoolVal.isSetValue)
    assert(tRowSet.getRows.get(1).getColVals.get(0).getBoolVal.isSetValue)
    assert(tRowSet.getRows.get(2).getColVals.get(0).getBoolVal.isSetValue)
    assert(!tRowSet.getRows.get(3).getColVals.get(0).getBoolVal.isSetValue)
  }

  test("byte type null value tests") {
    val schema1 = new StructType().add("c1", "byte")
    val rows = Seq(Row(null), Row(1.toByte), Row(2.toByte), Row(null))
    val tRowSet = RowBasedSet(schema1, rows).toTRowSet
    assert(tRowSet.getRows.get(0).getColVals.get(0).getByteVal.getValue === 0)
    assert(tRowSet.getRows.get(1).getColVals.get(0).getByteVal.getValue === 1)
    assert(tRowSet.getRows.get(2).getColVals.get(0).getByteVal.getValue === 2)
    assert(tRowSet.getRows.get(3).getColVals.get(0).getByteVal.getValue === 0)
  }

  test("short type null value tests") {
    val schema1 = new StructType().add("c1", "short")
    val rows = Seq(Row(null), Row(1.toShort), Row(2.toShort), Row(null))
    val tRowSet = RowBasedSet(schema1, rows).toTRowSet
    assert(tRowSet.getRows.get(0).getColVals.get(0).getI16Val.getValue === 0)
    assert(tRowSet.getRows.get(1).getColVals.get(0).getI16Val.getValue === 1)
    assert(tRowSet.getRows.get(2).getColVals.get(0).getI16Val.getValue === 2)
    assert(tRowSet.getRows.get(3).getColVals.get(0).getI16Val.getValue === 0)
  }

  test("int type null value tests") {
    val schema1 = new StructType().add("c1", "int")
    val rows = Seq(Row(null), Row(1), Row(2), Row(null))
    val tRowSet = RowBasedSet(schema1, rows).toTRowSet
    assert(tRowSet.getRows.get(0).getColVals.get(0).getI32Val.getValue === 0)
    assert(tRowSet.getRows.get(1).getColVals.get(0).getI32Val.getValue === 1)
    assert(tRowSet.getRows.get(2).getColVals.get(0).getI32Val.getValue === 2)
    assert(tRowSet.getRows.get(3).getColVals.get(0).getI32Val.getValue === 0)
  }

  test("long type null value tests") {
    val schema1 = new StructType().add("c1", "long")
    val rows = Seq(Row(null), Row(1L), Row(2L), Row(null))
    val tRowSet = RowBasedSet(schema1, rows).toTRowSet
    assert(tRowSet.getRows.get(0).getColVals.get(0).getI64Val.getValue === 0)
    assert(tRowSet.getRows.get(1).getColVals.get(0).getI64Val.getValue === 1)
    assert(tRowSet.getRows.get(2).getColVals.get(0).getI64Val.getValue === 2)
    assert(tRowSet.getRows.get(3).getColVals.get(0).getI64Val.getValue === 0)
  }

  test("float type null value tests") {
    val schema1 = new StructType().add("c1", "float")
    val rows = Seq(Row(null), Row(1.0f), Row(2.0f), Row(null))
    val tRowSet = RowBasedSet(schema1, rows).toTRowSet
    assert(tRowSet.getRows.get(0).getColVals.get(0).getDoubleVal.getValue === 0)
    assert(tRowSet.getRows.get(1).getColVals.get(0).getDoubleVal.getValue === 1)
    assert(tRowSet.getRows.get(2).getColVals.get(0).getDoubleVal.getValue === 2)
    assert(tRowSet.getRows.get(3).getColVals.get(0).getDoubleVal.getValue === 0)
  }

  test("double type null value tests") {
    val schema1 = new StructType().add("c1", "double")
    val rows = Seq(Row(null), Row(1.0d), Row(2.0d), Row(null))
    val tRowSet = RowBasedSet(schema1, rows).toTRowSet
    assert(tRowSet.getRows.get(0).getColVals.get(0).getDoubleVal.getValue === 0)
    assert(tRowSet.getRows.get(1).getColVals.get(0).getDoubleVal.getValue === 1)
    assert(tRowSet.getRows.get(2).getColVals.get(0).getDoubleVal.getValue === 2)
    assert(tRowSet.getRows.get(3).getColVals.get(0).getDoubleVal.getValue === 0)
  }

  test("string type null value tests") {
    val schema1 = new StructType().add("c1", "string")
    val rows = Seq(Row(null), Row("a"), Row(""), Row(null))
    val tRowSet = RowBasedSet(schema1, rows).toTRowSet
    assert(tRowSet.getRows.get(0).getColVals.get(0).getStringVal.getValue === null)
    assert(tRowSet.getRows.get(1).getColVals.get(0).getStringVal.getValue === "a")
    assert(tRowSet.getRows.get(2).getColVals.get(0).getStringVal.getValue === "")
    assert(tRowSet.getRows.get(3).getColVals.get(0).getStringVal.getValue === null)
  }

  test("binary type null value tests") {
    val schema1 = new StructType().add("c1", BinaryType)
    val v1 = Array(1.toByte)
    val v2 = Array(2.toByte)
    val rows = Seq(Row(null), Row(v1), Row(v2), Row(null))
    val tRowSet = RowBasedSet(schema1, rows).toTRowSet
    assert(tRowSet.getRows.get(1).getColVals.get(0).getStringVal.getValue ===
      SparkSQLUtils.toHiveString((v1, BinaryType)))
    assert(tRowSet.getRows.get(2).getColVals.get(0).getStringVal.getValue ===
      SparkSQLUtils.toHiveString((v2, BinaryType)))
  }

  test("date type null value tests") {
    val schema1 = new StructType().add("c1", DateType)
    val v1 = "2018-03-12"
    val v2 = "2010-03-13"
    val rows = Seq(Row(null), Row(v1), Row(v2), Row(null))
    val tRowSet = RowBasedSet(schema1, rows).toTRowSet
    assert(tRowSet.getRows.get(0).getColVals.get(0).getStringVal.getValue === null)
    assert(tRowSet.getRows.get(1).getColVals.get(0).getStringVal.getValue === v1)
    assert(tRowSet.getRows.get(2).getColVals.get(0).getStringVal.getValue === v2)
  }

  test("timestamp type null value tests") {
    val schema1 = new StructType().add("c1", TimestampType)
    val v1 = new Timestamp(System.currentTimeMillis())
    val v2 = "2018-03-30 17:59:59"
    val rows = Seq(Row(null), Row(v1), Row(v2), Row(null))
    val tRowSet = RowBasedSet(schema1, rows).toTRowSet
    assert(tRowSet.getRows.get(0).getColVals.get(0).getStringVal.getValue === null)
    assert(tRowSet.getRows.get(1).getColVals.get(0).getStringVal.getValue === v1.toString)
    assert(tRowSet.getRows.get(2).getColVals.get(0).getStringVal.getValue === v2)
  }

  test("decimal type null value tests") {
    val schema1 = new StructType().add("c1", "decimal(5, 1)")
    val v1 = new java.math.BigDecimal("100.12")
    val v2 = new java.math.BigDecimal("200.34")
    val rows = Seq(Row(null), Row(v1), Row(v2), Row(null))
    val tRowSet = RowBasedSet(schema1, rows).toTRowSet
    assert(tRowSet.getRows.get(0).getColVals.get(0).getStringVal.getValue === null)
    assert(tRowSet.getRows.get(1).getColVals.get(0).getStringVal.getValue === v1.toString)
    assert(tRowSet.getRows.get(2).getColVals.get(0).getStringVal.getValue === v2.toString)
  }

  test("array type null value tests") {
    val schema1 = new StructType().add("c1", "array<int>")
    val v1 = mutable.WrappedArray.make(Array(1, 2))
    val v2 = mutable.WrappedArray.make(Array(3, 4))
    val rows = Seq(Row(null), Row(v1), Row(v2), Row(null))
    val tRowSet = RowBasedSet(schema1, rows).toTRowSet
    assert(tRowSet.getRows.get(0).getColVals.get(0).getStringVal.getValue === null)
    assert(tRowSet.getRows.get(1).getColVals.get(0).getStringVal.getValue ===
      SparkSQLUtils.toHiveString((v1, schema1.head.dataType)))
    assert(tRowSet.getRows.get(2).getColVals.get(0).getStringVal.getValue ===
      SparkSQLUtils.toHiveString((v2, schema1.head.dataType)))
  }

  test("map type null value tests") {
    val schema1 = new StructType().add("c1", "map<int, array<string>>")
    val v1 = Map(1 -> mutable.WrappedArray.make(Array(1, 2)))
    val v2 = Map(2 -> mutable.WrappedArray.make(Array(3, 4)))
    val rows = Seq(Row(null), Row(v1), Row(v2), Row(null))
    val tRowSet = RowBasedSet(schema1, rows).toTRowSet
    assert(tRowSet.getRows.get(0).getColVals.get(0).getStringVal.getValue === null)
    assert(tRowSet.getRows.get(1).getColVals.get(0).getStringVal.getValue ===
      SparkSQLUtils.toHiveString((v1, schema1.head.dataType)))
    assert(tRowSet.getRows.get(2).getColVals.get(0).getStringVal.getValue ===
      SparkSQLUtils.toHiveString((v2, schema1.head.dataType)))
  }

  test("ss type null value tests") {
    val schema1 = new StructType().add("c1", "struct<a: int, b:string>")
    val v1 = Row(1, "12")
    val v2 = Row(2, "22")
    val rows = Seq(Row(null), Row(v1), Row(v2), Row(null))
    val tRowSet = RowBasedSet(schema1, rows).toTRowSet
    assert(tRowSet.getRows.get(0).getColVals.get(0).getStringVal.getValue === null)
    assert(tRowSet.getRows.get(1).getColVals.get(0).getStringVal.getValue ===
      SparkSQLUtils.toHiveString((v1, schema1.head.dataType)))
    assert(tRowSet.getRows.get(2).getColVals.get(0).getStringVal.getValue ===
      SparkSQLUtils.toHiveString((v2, schema1.head.dataType)))
  }

  test("the given row set is null") {
    assert(RowBasedSet(schema, null).toTRowSet.getRows.size() === 0)
    assert(RowBasedSet(null, null).toTRowSet.getRows.size() === 0)

  }

  test("the given schema is null") {
    intercept[IllegalArgumentException](RowBasedSet(null, rows).toTRowSet)
  }

  test("the give schema does match row schema") {
    intercept[IllegalArgumentException](RowBasedSet(new StructType(), rows).toTRowSet)

  }

  test("empty row") {
    val tRowSet = RowBasedSet(new StructType(), Seq(Row())).toTRowSet
    assert(tRowSet.getRows.size() === 1)
    assert(!tRowSet.getRows.get(0).isSetColVals)

  }
}
