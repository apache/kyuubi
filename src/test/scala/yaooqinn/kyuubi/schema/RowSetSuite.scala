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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

class RowSetSuite extends SparkFunSuite {

  test("1") {
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

    // fetch next
    val rowIterator = rows.iterator
    var taken = rowIterator.take(maxRows)
    var tRowSet = RowSet(schema, taken).toTRowSet
    assert(tRowSet.getRowsSize === 5)
    assert(tRowSet.getRows.get(0).getColVals.get(1).getStringVal.getValue === "11")
    assert(tRowSet.getRows.get(1).getColVals.get(1).getStringVal.getValue === "22")
    assert(tRowSet.getRows.get(2).getColVals.get(1).getStringVal.getValue === "33")
    assert(tRowSet.getRows.get(3).getColVals.get(1).getStringVal.getValue === "44")
    assert(tRowSet.getRows.get(4).getColVals.get(1).getStringVal.getValue === "55")

    taken = rowIterator.take(maxRows)
    tRowSet = RowSet(schema, taken).toTRowSet
    assert(tRowSet.getRowsSize === 5)
    assert(tRowSet.getRows.get(0).getColVals.get(1).getStringVal.getValue === "66")
    assert(tRowSet.getRows.get(1).getColVals.get(1).getStringVal.getValue === "77")
    assert(tRowSet.getRows.get(2).getColVals.get(1).getStringVal.getValue === "88")
    assert(tRowSet.getRows.get(3).getColVals.get(1).getStringVal.getValue === "99")
    assert(tRowSet.getRows.get(4).getColVals.get(1).getStringVal.getValue === "000")

    taken = rowIterator.take(maxRows)
    tRowSet = RowSet(schema, taken).toTRowSet
    assert(tRowSet.getRowsSize === 5)
    assert(tRowSet.getRows.get(0).getColVals.get(1).getStringVal.getValue === "111")
    assert(tRowSet.getRows.get(1).getColVals.get(1).getStringVal.getValue === "222")
    assert(tRowSet.getRows.get(2).getColVals.get(1).getStringVal.getValue === "333")
    assert(tRowSet.getRows.get(3).getColVals.get(1).getStringVal.getValue === "444")
    assert(tRowSet.getRows.get(4).getColVals.get(1).getStringVal.getValue === "555")

    taken = rowIterator.take(maxRows)
    tRowSet = RowSet(schema, taken).toTRowSet
    assert(tRowSet.getRowsSize === 1)
    assert(tRowSet.getRows.get(0).getColVals.get(1).getStringVal.getValue === "666")
    intercept[IndexOutOfBoundsException](tRowSet.getRows.get(1))

    assert(rowIterator.isEmpty)

    // fetch first
    val rowIterator2 = rows.iterator

    val (itr1, itr2) = rowIterator2.take(maxRows).duplicate
    val resultList = itr2.toList

    taken = itr1
    tRowSet = RowSet(schema, taken).toTRowSet
    assert(tRowSet.getRowsSize === 5)
    assert(tRowSet.getRows.get(0).getColVals.get(1).getStringVal.getValue === "11")
    assert(tRowSet.getRows.get(1).getColVals.get(1).getStringVal.getValue === "22")
    assert(tRowSet.getRows.get(2).getColVals.get(1).getStringVal.getValue === "33")
    assert(tRowSet.getRows.get(3).getColVals.get(1).getStringVal.getValue === "44")
    assert(tRowSet.getRows.get(4).getColVals.get(1).getStringVal.getValue === "55")

    taken = resultList.iterator
    tRowSet = RowSet(schema, taken).toTRowSet
    assert(tRowSet.getRowsSize === 5)
    assert(tRowSet.getRows.get(0).getColVals.get(1).getStringVal.getValue === "11")
    assert(tRowSet.getRows.get(1).getColVals.get(1).getStringVal.getValue === "22")
    assert(tRowSet.getRows.get(2).getColVals.get(1).getStringVal.getValue === "33")
    assert(tRowSet.getRows.get(3).getColVals.get(1).getStringVal.getValue === "44")
    assert(tRowSet.getRows.get(4).getColVals.get(1).getStringVal.getValue === "55")

    taken = resultList.iterator
    tRowSet = RowSet(schema, taken).toTRowSet
    assert(tRowSet.getRowsSize === 5)
    assert(tRowSet.getRows.get(0).getColVals.get(1).getStringVal.getValue === "11")
    assert(tRowSet.getRows.get(1).getColVals.get(1).getStringVal.getValue === "22")
    assert(tRowSet.getRows.get(2).getColVals.get(1).getStringVal.getValue === "33")
    assert(tRowSet.getRows.get(3).getColVals.get(1).getStringVal.getValue === "44")
    assert(tRowSet.getRows.get(4).getColVals.get(1).getStringVal.getValue === "55")
  }

}
