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

import java.{lang, util}
import java.nio.ByteBuffer
import java.util.Collections

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.flink.table.catalog.Column
import org.apache.flink.table.types.logical.{DecimalType, _}
import org.apache.flink.types.Row
import org.apache.hive.service.rpc.thrift._

import org.apache.kyuubi.engine.flink.result.ResultSet

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
      val tColumn = toTColumn(rows, i, filed.getDataType.getLogicalType)
      tRowSet.addToColumns(tColumn)
    }
    tRowSet
  }

  private def toTColumnValue(
      ordinal: Int,
      row: Row,
      resultSet: ResultSet): TColumnValue = {

    val logicalType = resultSet.getColumns.get(ordinal).getDataType.getLogicalType

    logicalType match {
      case _: BooleanType =>
        val boolValue = new TBoolValue
        if (row.getField(ordinal) != null) {
          boolValue.setValue(row.getField(ordinal).asInstanceOf[Boolean])
        }
        TColumnValue.boolVal(boolValue)
      case _: TinyIntType =>
        val tI16Value = new TI16Value
        if (row.getField(ordinal) != null) {
          tI16Value.setValue(row.getField(ordinal).asInstanceOf[Short])
        }
        TColumnValue.i16Val(tI16Value)
      case _: IntType =>
        val tI32Value = new TI32Value
        if (row.getField(ordinal) != null) {
          tI32Value.setValue(row.getField(ordinal).asInstanceOf[Short])
        }
        TColumnValue.i32Val(tI32Value)
      case _: BigIntType =>
        val tI64Value = new TI64Value
        if (row.getField(ordinal) != null) {
          tI64Value.setValue(row.getField(ordinal).asInstanceOf[Long])
        }
        TColumnValue.i64Val(tI64Value)
      case _: FloatType =>
        val tDoubleValue = new TDoubleValue
        if (row.getField(ordinal) != null) {
          tDoubleValue.setValue(row.getField(ordinal).asInstanceOf[Float])
        }
        TColumnValue.doubleVal(tDoubleValue)
      case _: DoubleType =>
        val tDoubleValue = new TDoubleValue
        if (row.getField(ordinal) != null) {
          tDoubleValue.setValue(row.getField(ordinal).asInstanceOf[Double])
        }
        TColumnValue.doubleVal(tDoubleValue)
      case _: VarCharType =>
        val tStringValue = new TStringValue
        if (row.getField(ordinal) != null) {
          tStringValue.setValue(row.getField(ordinal).asInstanceOf[String])
        }
        TColumnValue.stringVal(tStringValue)
      case _: CharType =>
        val tStringValue = new TStringValue
        if (row.getField(ordinal) != null) {
          tStringValue.setValue(row.getField(ordinal).asInstanceOf[String])
        }
        TColumnValue.stringVal(tStringValue)
      case _ =>
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

  private def toTColumn(rows: Seq[Row], ordinal: Int, logicalType: LogicalType): TColumn = {
    val nulls = new java.util.BitSet()
    logicalType match {
      case _: BooleanType =>
        val values = getOrSetAsNull[lang.Boolean](rows, ordinal, nulls, true)
        TColumn.boolVal(new TBoolColumn(values, nulls))
      case _: TinyIntType =>
        val values = getOrSetAsNull[lang.Short](rows, ordinal, nulls, 0.toShort)
        TColumn.i16Val(new TI16Column(values, nulls))
      case _: VarCharType =>
        val values = getOrSetAsNull[String](rows, ordinal, nulls, "")
        TColumn.stringVal(new TStringColumn(values, nulls))
      case _: CharType =>
        val values = getOrSetAsNull[String](rows, ordinal, nulls, "")
        TColumn.stringVal(new TStringColumn(values, nulls))

      case _ =>
        val values = rows.zipWithIndex.toList.map { case (row, i) =>
          nulls.set(i, row.getField(ordinal) == null)
          if (row.getField(ordinal) == null) {
            ""
          } else {
            toHiveString((row.getField(ordinal), logicalType))
          }
        }.asJava
        TColumn.stringVal(new TStringColumn(values, nulls))
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
        ret.add(idx, row.getFieldAs[T](ordinal))
      }
      idx += 1
    }
    ret
  }

  def toTColumnDesc(field: Column, pos: Int): TColumnDesc = {
    val tColumnDesc = new TColumnDesc()
    tColumnDesc.setColumnName(field.getName)
    tColumnDesc.setTypeDesc(toTTypeDesc(field.getDataType.getLogicalType))
    tColumnDesc.setComment(field.getComment.orElse(""))
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

  def toTTypeId(typ: LogicalType): TTypeId = typ match {
    case _: NullType => TTypeId.NULL_TYPE
    case _: BooleanType => TTypeId.BOOLEAN_TYPE
    case _: FloatType => TTypeId.FLOAT_TYPE
    case _: DoubleType => TTypeId.DOUBLE_TYPE
    case _: VarCharType => TTypeId.STRING_TYPE
    case _: CharType => TTypeId.STRING_TYPE
    case _: DecimalType => TTypeId.DECIMAL_TYPE
    case other =>
      throw new IllegalArgumentException(s"Unrecognized type name: ${other.asSummaryString()}")
  }

  /**
   * A simpler impl of Flink's toHiveString
   */
  def toHiveString(dataWithType: (Any, LogicalType)): String = {
    dataWithType match {
      case (null, _) =>
        // Only match nulls in nested type values
        "null"

      case (decimal: java.math.BigDecimal, _: DecimalType) =>
        decimal.toPlainString

      case (other, _) =>
        other.toString
    }
  }
}
