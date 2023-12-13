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

package org.apache.kyuubi.engine.schema
import java.nio.ByteBuffer
import java.util.{ArrayList => JArrayList, BitSet => JBitSet, List => JList}

import scala.collection.JavaConverters._

import org.apache.kyuubi.shaded.hive.service.rpc.thrift._
import org.apache.kyuubi.shaded.hive.service.rpc.thrift.TTypeId._
import org.apache.kyuubi.util.RowSetUtils.bitSetToBuffer

trait AbstractTRowSetGenerator[SchemaT, RowT, ColumnT] {

  protected def getColumnSizeFromSchemaType(schema: SchemaT): Int

  protected def getColumnType(schema: SchemaT, ordinal: Int): ColumnT

  protected def isColumnNullAt(row: RowT, ordinal: Int): Boolean

  protected def getColumnAs[T](row: RowT, ordinal: Int): T

  protected def toTColumn(rows: Seq[RowT], ordinal: Int, typ: ColumnT): TColumn

  protected def toTColumnValue(ordinal: Int, row: RowT, types: SchemaT): TColumnValue

  def toTRowSet(
      rows: Seq[RowT],
      schema: SchemaT,
      protocolVersion: TProtocolVersion): TRowSet = {
    if (protocolVersion.getValue < TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      toRowBasedSet(rows, schema)
    } else {
      toColumnBasedSet(rows, schema)
    }
  }

  def toRowBasedSet(rows: Seq[RowT], schema: SchemaT): TRowSet = {
    val rowSize = rows.length
    val tRows = new JArrayList[TRow](rowSize)
    var i = 0
    while (i < rowSize) {
      val row = rows(i)
      var j = 0
      val columnSize = getColumnSizeFromSchemaType(schema)
      val tColumnValues = new JArrayList[TColumnValue](columnSize)
      while (j < columnSize) {
        val columnValue = toTColumnValue(j, row, schema)
        tColumnValues.add(columnValue)
        j += 1
      }
      i += 1
      val tRow = new TRow(tColumnValues)
      tRows.add(tRow)
    }
    new TRowSet(0, tRows)
  }

  def toColumnBasedSet(rows: Seq[RowT], schema: SchemaT): TRowSet = {
    val rowSize = rows.length
    val tRowSet = new TRowSet(0, new JArrayList[TRow](rowSize))
    var i = 0
    val columnSize = getColumnSizeFromSchemaType(schema)
    val tColumns = new JArrayList[TColumn](columnSize)
    while (i < columnSize) {
      val tColumn = toTColumn(rows, i, getColumnType(schema, i))
      tColumns.add(tColumn)
      i += 1
    }
    tRowSet.setColumns(tColumns)
    tRowSet
  }

  protected def getOrSetAsNull[T](
      rows: Seq[RowT],
      ordinal: Int,
      nulls: JBitSet,
      defaultVal: T): JList[T] = {
    val size = rows.length
    val ret = new JArrayList[T](size)
    var idx = 0
    while (idx < size) {
      val row = rows(idx)
      val isNull = isColumnNullAt(row, ordinal)
      if (isNull) {
        nulls.set(idx, true)
        ret.add(defaultVal)
      } else {
        ret.add(getColumnAs[T](row, ordinal))
      }
      idx += 1
    }
    ret
  }

  protected def toTTypeColumnVal(typeId: TTypeId, row: RowT, ordinal: Int): TColumnValue = {
    def isNull = isColumnNullAt(row, ordinal)
    typeId match {
      case BOOLEAN_TYPE =>
        val boolValue = new TBoolValue
        if (!isNull) boolValue.setValue(getColumnAs[java.lang.Boolean](row, ordinal))
        TColumnValue.boolVal(boolValue)

      case BINARY_TYPE =>
        val byteValue = new TByteValue
        if (!isNull) byteValue.setValue(getColumnAs[java.lang.Byte](row, ordinal))
        TColumnValue.byteVal(byteValue)

      case TINYINT_TYPE =>
        val tI16Value = new TI16Value
        if (!isNull) tI16Value.setValue(getColumnAs[java.lang.Short](row, ordinal))
        TColumnValue.i16Val(tI16Value)

      case INT_TYPE =>
        val tI32Value = new TI32Value
        if (!isNull) tI32Value.setValue(getColumnAs[java.lang.Integer](row, ordinal))
        TColumnValue.i32Val(tI32Value)

      case BIGINT_TYPE =>
        val tI64Value = new TI64Value
        if (!isNull) tI64Value.setValue(getColumnAs[java.lang.Long](row, ordinal))
        TColumnValue.i64Val(tI64Value)

      case FLOAT_TYPE =>
        val tDoubleValue = new TDoubleValue
        if (!isNull) tDoubleValue.setValue(getColumnAs[java.lang.Float](row, ordinal).toDouble)
        TColumnValue.doubleVal(tDoubleValue)

      case DOUBLE_TYPE =>
        val tDoubleValue = new TDoubleValue
        if (!isNull) tDoubleValue.setValue(getColumnAs[java.lang.Double](row, ordinal))
        TColumnValue.doubleVal(tDoubleValue)

      case STRING_TYPE =>
        val tStringValue = new TStringValue
        if (!isNull) tStringValue.setValue(getColumnAs[String](row, ordinal))
        TColumnValue.stringVal(tStringValue)

      case otherType =>
        throw new UnsupportedOperationException(s"unsupported type $otherType for toTTypeColumnVal")
    }
  }

  protected def toTTypeColumn(typeId: TTypeId, rows: Seq[RowT], ordinal: Int): TColumn = {
    val nulls = new JBitSet()
    typeId match {
      case BOOLEAN_TYPE =>
        val values = getOrSetAsNull[java.lang.Boolean](rows, ordinal, nulls, true)
        TColumn.boolVal(new TBoolColumn(values, nulls))

      case BINARY_TYPE =>
        val values = getOrSetAsNull[java.lang.Byte](rows, ordinal, nulls, 0.toByte)
        TColumn.byteVal(new TByteColumn(values, nulls))

      case SMALLINT_TYPE =>
        val values = getOrSetAsNull[java.lang.Short](rows, ordinal, nulls, 0.toShort)
        TColumn.i16Val(new TI16Column(values, nulls))

      case TINYINT_TYPE =>
        val values = getOrSetAsNull[java.lang.Short](rows, ordinal, nulls, 0.toShort)
        TColumn.i16Val(new TI16Column(values, nulls))

      case INT_TYPE =>
        val values = getOrSetAsNull[java.lang.Integer](rows, ordinal, nulls, 0)
        TColumn.i32Val(new TI32Column(values, nulls))

      case BIGINT_TYPE =>
        val values = getOrSetAsNull[java.lang.Long](rows, ordinal, nulls, 0L)
        TColumn.i64Val(new TI64Column(values, nulls))

      case FLOAT_TYPE =>
        val values = getOrSetAsNull[java.lang.Float](rows, ordinal, nulls, 0.toFloat)
          .asScala.map(n => java.lang.Double.valueOf(n.toString)).asJava
        TColumn.doubleVal(new TDoubleColumn(values, nulls))

      case DOUBLE_TYPE =>
        val values = getOrSetAsNull[java.lang.Double](rows, ordinal, nulls, 0.toDouble)
        TColumn.doubleVal(new TDoubleColumn(values, nulls))

      case STRING_TYPE =>
        val values = getOrSetAsNull[java.lang.String](rows, ordinal, nulls, "")
        TColumn.stringVal(new TStringColumn(values, nulls))

      case ARRAY_TYPE =>
        val values = getOrSetAsNull[Array[Byte]](rows, ordinal, nulls, Array())
          .asScala
          .map(ByteBuffer.wrap)
          .asJava
        TColumn.binaryVal(new TBinaryColumn(values, nulls))

      case otherType =>
        throw new UnsupportedOperationException(s"unsupported type $otherType for toTTypeColumnVal")
    }
  }
}
