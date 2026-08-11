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

package org.apache.kyuubi.server.flight

import java.nio.ByteBuffer
import java.util

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

class KyuubiFlightArrowUtilsSuite extends KyuubiFunSuite {

  test("populateRootFromRowSet writes columnar thrift without Seq materialization") {
    val emptyFields = util.Collections.emptyList[Field]()
    val schema = new Schema(util.Arrays.asList(
      new Field("flag", new FieldType(true, ArrowType.Bool.INSTANCE, null), emptyFields),
      new Field(
        "id",
        new FieldType(true, new ArrowType.Int(32, true), null),
        emptyFields),
      new Field(
        "score",
        new FieldType(
          true,
          new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
          null),
        emptyFields),
      new Field(
        "name",
        new FieldType(true, ArrowType.Utf8.INSTANCE, null),
        emptyFields)))

    val boolNulls = ByteBuffer.wrap(Array[Byte](0))
    val i32Nulls = ByteBuffer.wrap(Array[Byte](0))
    val doubleNulls = ByteBuffer.wrap(Array[Byte](0))
    val stringNulls = ByteBuffer.wrap(Array[Byte](0))

    val rowSet = new TRowSet()
    rowSet.addToColumns(TColumn.boolVal(
      new TBoolColumn(util.Arrays.asList(true, false), boolNulls)))
    rowSet.addToColumns(TColumn.i32Val(new TI32Column(util.Arrays.asList(1, 2), i32Nulls)))
    rowSet.addToColumns(TColumn.doubleVal(new TDoubleColumn(
      util.Arrays.asList(1.5d, 2.5d),
      doubleNulls)))
    rowSet.addToColumns(TColumn.stringVal(new TStringColumn(
      util.Arrays.asList("a", "b"),
      stringNulls)))

    val allocator = new RootAllocator()
    val root = VectorSchemaRoot.create(schema, allocator)
    try {
      KyuubiFlightArrowUtils.populateRootFromRowSet(root, rowSet)
      assert(root.getRowCount === 2)
      assert(root.getVector(0).getObject(0) === true)
      assert(root.getVector(1).getObject(1).toString === "2")
      assert(root.getVector(2).getObject(0).toString === "1.5")
      assert(root.getVector(3).getObject(1).toString === "b")
    } finally {
      root.close()
      allocator.close()
    }
  }

  test("isEmpty and isArrowRowSet helpers") {
    assert(KyuubiFlightArrowUtils.isEmpty(null))
    assert(KyuubiFlightArrowUtils.isEmpty(new TRowSet()))
    val arrow = new TRowSet()
    val binary = new TBinaryColumn()
    binary.addToValues(ByteBuffer.wrap(Array[Byte](1, 2, 3)))
    arrow.addToColumns(TColumn.binaryVal(binary))
    assert(KyuubiFlightArrowUtils.isArrowRowSet(arrow))
    assert(KyuubiFlightArrowUtils.arrowBatchBytes(arrow) === 3L)
    assert(!KyuubiFlightArrowUtils.isEmpty(arrow))
  }

  test("isEmpty is true for exhausted columnar thrift pages with schema columns") {
    // Matches TRowSetGenerator.toColumnBasedSet(Nil, schema): one empty TColumn per field.
    val emptyNulls = ByteBuffer.wrap(Array[Byte](0))
    val rowSet = new TRowSet()
    rowSet.setRows(new util.ArrayList[TRow]())
    rowSet.addToColumns(TColumn.i32Val(new TI32Column(new util.ArrayList[Integer](), emptyNulls)))
    rowSet.addToColumns(TColumn.stringVal(
      new TStringColumn(new util.ArrayList[String](), emptyNulls)))

    assert(rowSet.getColumnsSize === 2)
    assert(KyuubiFlightArrowUtils.rowCount(rowSet) === 0)
    assert(KyuubiFlightArrowUtils.isEmpty(rowSet))
    assert(!KyuubiFlightArrowUtils.isArrowRowSet(rowSet))
  }

  test("isEmpty is false when columnar thrift page has values") {
    val nulls = ByteBuffer.wrap(Array[Byte](0))
    val rowSet = new TRowSet()
    rowSet.addToColumns(TColumn.i32Val(new TI32Column(util.Arrays.asList(7), nulls)))
    assert(KyuubiFlightArrowUtils.rowCount(rowSet) === 1)
    assert(!KyuubiFlightArrowUtils.isEmpty(rowSet))
  }

  test("populateRootFromRowSet yields zero rows for exhausted columnar page") {
    val emptyFields = util.Collections.emptyList[Field]()
    val schema = new Schema(util.Arrays.asList(
      new Field("id", new FieldType(true, new ArrowType.Int(32, true), null), emptyFields),
      new Field("name", new FieldType(true, ArrowType.Utf8.INSTANCE, null), emptyFields)))

    val emptyNulls = ByteBuffer.wrap(Array[Byte](0))
    val rowSet = new TRowSet()
    rowSet.addToColumns(TColumn.i32Val(new TI32Column(new util.ArrayList[Integer](), emptyNulls)))
    rowSet.addToColumns(TColumn.stringVal(
      new TStringColumn(new util.ArrayList[String](), emptyNulls)))

    val allocator = new RootAllocator()
    val root = VectorSchemaRoot.create(schema, allocator)
    try {
      KyuubiFlightArrowUtils.populateRootFromRowSet(root, rowSet)
      assert(root.getRowCount === 0)
    } finally {
      root.close()
      allocator.close()
    }
  }
}
