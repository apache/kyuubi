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

package org.apache.kyuubi.engine.flink.schema

import java.nio.ByteBuffer
import java.util
import java.util.Collections

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsJavaMapConverter, seqAsJavaListConverter}
import scala.language.implicitConversions

import org.apache.flink.table.types.logical._
import org.apache.flink.types.Row
import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.engine.flink.result.{ColumnInfo, ResultSet}

object RowSet {

  def resultSetToTRowSet(
      rows: Seq[Row],
      resultSet: ResultSet,
      protocolVersion: TProtocolVersion): TRowSet = {
    if (protocolVersion.getValue < TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      toRowBaseSet(rows, resultSet)
    } else {
      toColumnBasedSet(rows, resultSet)
    }
  }

  def toRowBaseSet(rows: Seq[Row], resultSet: ResultSet): TRowSet = {
    val tRows = rows.map { row =>
      val tRow = new TRow()
      (0 until row.getArity).map(i => toTColumnValue(i, row, resultSet))
        .foreach(tRow.addToColVals)
      tRow
    }.asJava

    new TRowSet(0, tRows)
  }

  def toColumnBasedSet(rows: Seq[Row], resultSet: ResultSet): TRowSet = {
    val size = rows.length
    val tRowSet = new TRowSet(0, new util.ArrayList[TRow](size))
    resultSet.getColumns.asScala.zipWithIndex.foreach { case (filed, i) =>
      val tColumn = toTColumn(rows, i, filed.getLogicalType)
      tRowSet.addToColumns(tColumn)
    }
    tRowSet
  }

  private def toTColumnValue(
      ordinal: Int,
      row: Row,
      resultSet: ResultSet): TColumnValue = {

    val logicalType = resultSet.getColumns.get(ordinal).getLogicalType

    if (logicalType.isInstanceOf[BooleanType]) {
      val boolValue = new TBoolValue
      if (row.getField(ordinal) != null) {
        boolValue.setValue(row.getField(ordinal).asInstanceOf[Boolean])
      }
      TColumnValue.boolVal(boolValue)
    } else if (logicalType.isInstanceOf[TinyIntType]) {
      val tI16Value = new TI16Value
      if (row.getField(ordinal) != null) {
        tI16Value.setValue(row.getField(ordinal).asInstanceOf[Short])
      }
      TColumnValue.i16Val(tI16Value)
    } else if (logicalType.isInstanceOf[IntType]) {
      val tI32Value = new TI32Value
      if (row.getField(ordinal) != null) {
        tI32Value.setValue(row.getField(ordinal).asInstanceOf[Short])
      }
      TColumnValue.i32Val(tI32Value)
    } else if (logicalType.isInstanceOf[BigIntType]) {
      val tI64Value = new TI64Value
      if (row.getField(ordinal) != null) {
        tI64Value.setValue(row.getField(ordinal).asInstanceOf[Long])
      }
      TColumnValue.i64Val(tI64Value)
    } else if (logicalType.isInstanceOf[FloatType]) {
      val tDoubleValue = new TDoubleValue
      if (row.getField(ordinal) != null) {
        tDoubleValue.setValue(row.getField(ordinal).asInstanceOf[Float])
      }
      TColumnValue.doubleVal(tDoubleValue)
    } else if (logicalType.isInstanceOf[DoubleType]) {
      val tDoubleValue = new TDoubleValue
      if (row.getField(ordinal) != null) {
        tDoubleValue.setValue(row.getField(ordinal).asInstanceOf[Double])
      }
      TColumnValue.doubleVal(tDoubleValue)
    } else if (logicalType.isInstanceOf[VarCharType]) {
      val tStringValue = new TStringValue
      if (row.getField(ordinal) != null) {
        tStringValue.setValue(row.getField(ordinal).asInstanceOf[String])
      }
      TColumnValue.stringVal(tStringValue)
    } else {
      val tStrValue = new TStringValue
      if (row.getField(ordinal) != null) {
        // TODO to be done
      }
      TColumnValue.stringVal(tStrValue)
    }
  }

  implicit private def bitSetToBuffer(bitSet: java.util.BitSet): ByteBuffer = {
    ByteBuffer.wrap(bitSet.toByteArray)
  }

  private def toTColumn(
      rows: Seq[Row],
      ordinal: Int,
      logicalType: LogicalType): TColumn = {
    val nulls = new java.util.BitSet()
    if (logicalType.isInstanceOf[BooleanType]) {
      val values = getOrSetAsNull[java.lang.Boolean](
        rows,
        ordinal,
        nulls,
        true)
      TColumn.boolVal(new TBoolColumn(values, nulls))
    } else if (logicalType.isInstanceOf[TinyIntType]) {
      val values = getOrSetAsNull[java.lang.Short](
        rows,
        ordinal,
        nulls,
        0.toShort)
      TColumn.i16Val(new TI16Column(values, nulls))
    } else if (logicalType.isInstanceOf[VarCharType]) {
      val values = getOrSetAsNull[java.lang.String](
        rows,
        ordinal,
        nulls,
        "")
      TColumn.stringVal(new TStringColumn(values, nulls))
    } else if (logicalType.isInstanceOf[CharType]) {
      val values = getOrSetAsNull[java.lang.String](
        rows,
        ordinal,
        nulls,
        "")
      TColumn.stringVal(new TStringColumn(values, nulls))
    } else {
      null
    }
  }

  private def getOrSetAsNull[T](
      rows: Seq[Row],
      ordinal: Int,
      nulls: java.util.BitSet,
      defaultVal: T): java.util.List[T] = {
    val size = rows.length
    val ret = new java.util.ArrayList[T](size)
    var idx = 0
    while (idx < size) {
      val row = rows(idx)
      val isNull = row.getField(ordinal) == null
      if (isNull) {
        nulls.set(idx, true)
        ret.add(idx, defaultVal)
      } else {
        ret.add(idx, row.getField(ordinal).asInstanceOf[T])
      }
      idx += 1
    }
    ret
  }

  def toTColumnDesc(field: ColumnInfo, pos: Int): TColumnDesc = {
    val tColumnDesc = new TColumnDesc()
    tColumnDesc.setColumnName(field.getName)
    tColumnDesc.setTypeDesc(toTTypeDesc(field.getLogicalType))
    tColumnDesc.setComment("")
    tColumnDesc.setPosition(pos)
    tColumnDesc
  }

  def toTTypeDesc(typ: LogicalType): TTypeDesc = {
    val typeEntry = new TPrimitiveTypeEntry(toTTypeId(typ))
    typeEntry.setTypeQualifiers(toTTypeQualifiers(typ))
    val tTypeDesc = new TTypeDesc()
    tTypeDesc.addToTypes(TTypeEntry.primitiveEntry(typeEntry))
    tTypeDesc
  }

  def toTTypeQualifiers(typ: LogicalType): TTypeQualifiers = {
    val ret = new TTypeQualifiers()
    val qualifiers = typ match {
      case d: DecimalType =>
        Map(
          TCLIServiceConstants.PRECISION -> TTypeQualifierValue.i32Value(d.getPrecision),
          TCLIServiceConstants.SCALE -> TTypeQualifierValue.i32Value(d.getScale)).asJava
      case _ => Collections.emptyMap[String, TTypeQualifierValue]()
    }
    ret.setQualifiers(qualifiers)
    ret
  }

  def toTTypeId(typ: LogicalType): TTypeId =
    if (typ.isInstanceOf[NullType]) {
      TTypeId.NULL_TYPE
    } else if (typ.isInstanceOf[BooleanType]) {
      TTypeId.BOOLEAN_TYPE
    } else if (typ.isInstanceOf[FloatType]) {
      TTypeId.FLOAT_TYPE
    } else if (typ.isInstanceOf[DoubleType]) {
      TTypeId.DOUBLE_TYPE
    } else if (typ.isInstanceOf[VarCharType]) {
      TTypeId.STRING_TYPE
    } else if (typ.isInstanceOf[CharType]) {
      TTypeId.STRING_TYPE
    } else {
      null
    }

}
