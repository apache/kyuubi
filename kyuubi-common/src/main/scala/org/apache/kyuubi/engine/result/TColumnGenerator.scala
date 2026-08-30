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

package org.apache.kyuubi.engine.result
import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Long => JLong, Short => JShort}
import java.nio.ByteBuffer
import java.util.{ArrayList => JArrayList, BitSet => JBitSet, List => JList}

import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

trait TColumnGenerator[RowT] extends TRowSetColumnGetter[RowT] {
  protected def getColumnToList[T](
      rows: Seq[RowT],
      ordinal: Int,
      defaultVal: T,
      convertFunc: (RowT, Int) => T = null): (JList[T], ByteBuffer) = {
    val rowSize = rows.length
    val ret = new JArrayList[T](rowSize)
    val nulls = new JBitSet(rowSize)
    val valueOf: (RowT, Int) => T =
      if (convertFunc == null) (row, ord) => getColumnAs[T](row, ord) else convertFunc
    val iter = rows.iterator
    var idx = 0
    while (iter.hasNext) {
      val row = iter.next()
      if (isColumnNullAt(row, ordinal)) {
        nulls.set(idx)
        ret.add(defaultVal)
      } else {
        ret.add(valueOf(row, ordinal))
      }
      idx += 1
    }
    (ret, ByteBuffer.wrap(nulls.toByteArray))
  }

  def asBooleanTColumn(rows: Seq[RowT], ordinal: Int): TColumn = {
    val (values, nulls) = getColumnToList[JBoolean](rows, ordinal, true)
    TColumn.boolVal(new TBoolColumn(values, nulls))
  }

  def asByteTColumn(rows: Seq[RowT], ordinal: Int): TColumn = {
    val (values, nulls) = getColumnToList[JByte](rows, ordinal, 0.toByte)
    TColumn.byteVal(new TByteColumn(values, nulls))
  }

  def asShortTColumn(
      rows: Seq[RowT],
      ordinal: Int,
      convertFunc: (RowT, Int) => JShort = null): TColumn = {
    val (values, nulls) = getColumnToList[JShort](rows, ordinal, 0.toShort, convertFunc)
    TColumn.i16Val(new TI16Column(values, nulls))
  }

  def asIntegerTColumn(
      rows: Seq[RowT],
      ordinal: Int,
      convertFunc: (RowT, Int) => Integer = null): TColumn = {
    val (values, nulls) = getColumnToList[Integer](rows, ordinal, 0, convertFunc)
    TColumn.i32Val(new TI32Column(values, nulls))
  }

  def asLongTColumn(
      rows: Seq[RowT],
      ordinal: Int,
      convertFunc: (RowT, Int) => JLong = null): TColumn = {
    val (values, nulls) = getColumnToList[JLong](rows, ordinal, 0.toLong, convertFunc)
    TColumn.i64Val(new TI64Column(values, nulls))
  }

  def asFloatTColumn(rows: Seq[RowT], ordinal: Int): TColumn = {
    val (values, nulls) = getColumnToList[JDouble](
      rows,
      ordinal,
      0.toDouble,
      (row, ord) => JDouble.valueOf(getColumnAs[JFloat](row, ord).toString))
    TColumn.doubleVal(new TDoubleColumn(values, nulls))
  }

  def asDoubleTColumn(rows: Seq[RowT], ordinal: Int): TColumn = {
    val (values, nulls) = getColumnToList[JDouble](rows, ordinal, 0.toDouble)
    TColumn.doubleVal(new TDoubleColumn(values, nulls))
  }

  def asStringTColumn(
      rows: Seq[RowT],
      ordinal: Int,
      defaultVal: String = "",
      convertFunc: (RowT, Int) => String = null): TColumn = {
    val (values, nulls) = getColumnToList[String](rows, ordinal, defaultVal, convertFunc)
    TColumn.stringVal(new TStringColumn(values, nulls))
  }

  def asByteArrayTColumn(rows: Seq[RowT], ordinal: Int): TColumn = {
    val (values, nulls) = getColumnToList[ByteBuffer](
      rows,
      ordinal,
      defaultVal = ByteBuffer.wrap(Array[Byte]()),
      (row, ord) => ByteBuffer.wrap(getColumnAs[Array[Byte]](row, ord)))
    TColumn.binaryVal(new TBinaryColumn(values, nulls))
  }
}
