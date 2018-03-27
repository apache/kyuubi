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

  test("row set basic suites") {
    // fetch next
    val rowIterator = rows.iterator
    var taken = rowIterator.take(maxRows).toSeq
    var tRowSet = ColumnBasedSet(schema, taken).toTRowSet
    assert(tRowSet.getColumns.size() === 2)
    assert(tRowSet.getRowsSize === 0)
    assert(tRowSet.getColumns.get(0).getI32Val.getValuesSize === 5)
    assert(tRowSet.getColumns.get(0).getI32Val.getValues.get(0).intValue() === 1)
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(1) === "22")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(2) === "33")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(3) === "44")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(4) === "55")

    taken = rowIterator.take(maxRows).toSeq
    tRowSet = ColumnBasedSet(schema, taken).toTRowSet
    assert(tRowSet.getColumns.get(0).getI32Val.getValues.get(0).intValue() === 6)
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(1) === "77")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(2) === "88")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(3) === "99")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(4) === "000")

    taken = rowIterator.take(maxRows).toSeq
    tRowSet = ColumnBasedSet(schema, taken).toTRowSet
    assert(tRowSet.getColumns.get(0).getI32Val.getValues.get(0).intValue() === 11)
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(1) === "222")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(2) === "333")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(3) === "444")
    assert(tRowSet.getColumns.get(1).getStringVal.getValues.get(4) === "555")

    taken = rowIterator.take(maxRows).toSeq
    tRowSet = ColumnBasedSet(schema, taken).toTRowSet
    assert(tRowSet.getColumns.get(0).getI32Val.getValues.get(0).intValue() === 16)
    intercept[IndexOutOfBoundsException](tRowSet.getColumns.get(1).getStringVal.getValues.get(1))

    assert(rowIterator.isEmpty)
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
}
