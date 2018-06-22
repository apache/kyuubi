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

import org.apache.hive.service.cli
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

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
}
