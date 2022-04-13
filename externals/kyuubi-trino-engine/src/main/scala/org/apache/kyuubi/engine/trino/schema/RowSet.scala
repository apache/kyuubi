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

package org.apache.kyuubi.engine.trino.schema

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util

import scala.collection.JavaConverters._

import io.trino.client.ClientStandardTypes._
import io.trino.client.ClientTypeSignature
import io.trino.client.Column
import io.trino.client.Row
import org.apache.hive.service.rpc.thrift.TBinaryColumn
import org.apache.hive.service.rpc.thrift.TBoolColumn
import org.apache.hive.service.rpc.thrift.TBoolValue
import org.apache.hive.service.rpc.thrift.TByteColumn
import org.apache.hive.service.rpc.thrift.TByteValue
import org.apache.hive.service.rpc.thrift.TColumn
import org.apache.hive.service.rpc.thrift.TColumnValue
import org.apache.hive.service.rpc.thrift.TDoubleColumn
import org.apache.hive.service.rpc.thrift.TDoubleValue
import org.apache.hive.service.rpc.thrift.TI16Column
import org.apache.hive.service.rpc.thrift.TI16Value
import org.apache.hive.service.rpc.thrift.TI32Column
import org.apache.hive.service.rpc.thrift.TI32Value
import org.apache.hive.service.rpc.thrift.TI64Column
import org.apache.hive.service.rpc.thrift.TI64Value
import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.hive.service.rpc.thrift.TRow
import org.apache.hive.service.rpc.thrift.TRowSet
import org.apache.hive.service.rpc.thrift.TStringColumn
import org.apache.hive.service.rpc.thrift.TStringValue

import org.apache.kyuubi.util.RowSetUtils.bitSetToBuffer

object RowSet {

  def toTRowSet(
      rows: Seq[List[_]],
      schema: List[Column],
      protocolVersion: TProtocolVersion): TRowSet = {
    if (protocolVersion.getValue < TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      toRowBasedSet(rows, schema)
    } else {
      toColumnBasedSet(rows, schema)
    }
  }

  def toRowBasedSet(rows: Seq[List[_]], schema: List[Column]): TRowSet = {
    val rowSize = rows.length
    val tRows = new util.ArrayList[TRow](rowSize)
    var i = 0
    while (i < rowSize) {
      val row = rows(i)
      val tRow = new TRow()
      val columnSize = row.size
      var j = 0
      while (j < columnSize) {
        val columnValue = toTColumnValue(j, row, schema)
        tRow.addToColVals(columnValue)
        j += 1
      }
      tRows.add(tRow)
      i += 1
    }
    new TRowSet(0, tRows)
  }

  def toColumnBasedSet(rows: Seq[List[_]], schema: List[Column]): TRowSet = {
    val size = rows.size
    val tRowSet = new TRowSet(0, new java.util.ArrayList[TRow](size))
    val columnSize = schema.length
    var i = 0
    while (i < columnSize) {
      val field = schema(i)
      val tColumn = toTColumn(rows, i, field.getTypeSignature)
      tRowSet.addToColumns(tColumn)
      i += 1
    }
    tRowSet
  }

  private def toTColumn(
      rows: Seq[Seq[Any]],
      ordinal: Int,
      typ: ClientTypeSignature): TColumn = {
    val nulls = new java.util.BitSet()
    typ.getRawType match {
      case BOOLEAN =>
        val values = getOrSetAsNull[java.lang.Boolean](rows, ordinal, nulls, true)
        TColumn.boolVal(new TBoolColumn(values, nulls))

      case TINYINT =>
        val values = getOrSetAsNull[java.lang.Byte](rows, ordinal, nulls, 0.toByte)
        TColumn.byteVal(new TByteColumn(values, nulls))

      case SMALLINT =>
        val values = getOrSetAsNull[java.lang.Short](rows, ordinal, nulls, 0.toShort)
        TColumn.i16Val(new TI16Column(values, nulls))

      case INTEGER =>
        val values = getOrSetAsNull[java.lang.Integer](rows, ordinal, nulls, 0)
        TColumn.i32Val(new TI32Column(values, nulls))

      case BIGINT =>
        val values = getOrSetAsNull[java.lang.Long](rows, ordinal, nulls, 0L)
        TColumn.i64Val(new TI64Column(values, nulls))

      case REAL =>
        val values = getOrSetAsNull[java.lang.Float](rows, ordinal, nulls, 0.toFloat)
          .asScala.map(n => java.lang.Double.valueOf(n.toString)).asJava
        TColumn.doubleVal(new TDoubleColumn(values, nulls))

      case DOUBLE =>
        val values = getOrSetAsNull[java.lang.Double](rows, ordinal, nulls, 0.toDouble)
        TColumn.doubleVal(new TDoubleColumn(values, nulls))

      case VARCHAR =>
        val values = getOrSetAsNull[String](rows, ordinal, nulls, "")
        TColumn.stringVal(new TStringColumn(values, nulls))

      case VARBINARY =>
        val values = getOrSetAsNull[Array[Byte]](rows, ordinal, nulls, Array())
          .asScala
          .map(ByteBuffer.wrap)
          .asJava
        TColumn.binaryVal(new TBinaryColumn(values, nulls))

      case _ =>
        val rowSize = rows.length
        val values = new util.ArrayList[String](rowSize)
        var i = 0
        while (i < rowSize) {
          val row = rows(i)
          nulls.set(i, row(ordinal) == null)
          val value =
            if (row(ordinal) == null) {
              ""
            } else {
              toHiveString(row(ordinal), typ)
            }
          values.add(value)
          i += 1
        }
        TColumn.stringVal(new TStringColumn(values, nulls))
    }
  }

  private def getOrSetAsNull[T](
      rows: Seq[Seq[Any]],
      ordinal: Int,
      nulls: java.util.BitSet,
      defaultVal: T): java.util.List[T] = {
    val size = rows.length
    val ret = new java.util.ArrayList[T](size)
    var idx = 0
    while (idx < size) {
      val row = rows(idx)
      val isNull = row(ordinal) == null
      if (isNull) {
        nulls.set(idx, true)
        ret.add(idx, defaultVal)
      } else {
        ret.add(idx, row(ordinal).asInstanceOf[T])
      }
      idx += 1
    }
    ret
  }

  private def toTColumnValue(
      ordinal: Int,
      row: List[Any],
      types: List[Column]): TColumnValue = {

    types(ordinal).getTypeSignature.getRawType match {
      case BOOLEAN =>
        val boolValue = new TBoolValue
        if (row(ordinal) != null) boolValue.setValue(row(ordinal).asInstanceOf[Boolean])
        TColumnValue.boolVal(boolValue)

      case TINYINT =>
        val byteValue = new TByteValue
        if (row(ordinal) != null) byteValue.setValue(row(ordinal).asInstanceOf[Byte])
        TColumnValue.byteVal(byteValue)

      case SMALLINT =>
        val tI16Value = new TI16Value
        if (row(ordinal) != null) tI16Value.setValue(row(ordinal).asInstanceOf[Short])
        TColumnValue.i16Val(tI16Value)

      case INTEGER =>
        val tI32Value = new TI32Value
        if (row(ordinal) != null) tI32Value.setValue(row(ordinal).asInstanceOf[Int])
        TColumnValue.i32Val(tI32Value)

      case BIGINT =>
        val tI64Value = new TI64Value
        if (row(ordinal) != null) tI64Value.setValue(row(ordinal).asInstanceOf[Long])
        TColumnValue.i64Val(tI64Value)

      case REAL =>
        val tDoubleValue = new TDoubleValue
        if (row(ordinal) != null) {
          val doubleValue = java.lang.Double.valueOf(row(ordinal).asInstanceOf[Float].toString)
          tDoubleValue.setValue(doubleValue)
        }
        TColumnValue.doubleVal(tDoubleValue)

      case DOUBLE =>
        val tDoubleValue = new TDoubleValue
        if (row(ordinal) != null) tDoubleValue.setValue(row(ordinal).asInstanceOf[Double])
        TColumnValue.doubleVal(tDoubleValue)

      case VARCHAR =>
        val tStringValue = new TStringValue
        if (row(ordinal) != null) tStringValue.setValue(row(ordinal).asInstanceOf[String])
        TColumnValue.stringVal(tStringValue)

      case _ =>
        val tStrValue = new TStringValue
        if (row(ordinal) != null) {
          tStrValue.setValue(
            toHiveString(row(ordinal), types(ordinal).getTypeSignature))
        }
        TColumnValue.stringVal(tStrValue)
    }
  }

  /**
   * A simpler impl of Trino's toHiveString
   */
  def toHiveString(data: Any, typ: ClientTypeSignature): String = {
    (data, typ.getRawType) match {
      case (null, _) =>
        // Only match nulls in nested type values
        "null"

      case (bin: Array[Byte], VARBINARY) =>
        new String(bin, StandardCharsets.UTF_8)

      case (s: String, VARCHAR) =>
        // Only match string in nested type values
        "\"" + s + "\""

      case (list: java.util.List[_], ARRAY) =>
        require(
          typ.getArgumentsAsTypeSignatures.asScala.nonEmpty,
          "Missing ARRAY argument type")
        val listType = typ.getArgumentsAsTypeSignatures.get(0)
        list.asScala
          .map(toHiveString(_, listType))
          .mkString("[", ",", "]")

      case (m: java.util.Map[_, _], MAP) =>
        require(
          typ.getArgumentsAsTypeSignatures.size() == 2,
          "Mismatched number of MAP argument types")
        val keyType = typ.getArgumentsAsTypeSignatures.get(0)
        val valueType = typ.getArgumentsAsTypeSignatures.get(1)
        m.asScala.map { case (key, value) =>
          toHiveString(key, keyType) + ":" + toHiveString(value, valueType)
        }.toSeq.sorted.mkString("{", ",", "}")

      case (row: Row, ROW) =>
        require(
          row.getFields.size() == typ.getArguments.size(),
          "Mismatched data values and ROW type")
        row.getFields.asScala.zipWithIndex.map { case (r, index) =>
          val namedRowType = typ.getArguments.get(index).getNamedTypeSignature
          if (namedRowType.getName.isPresent) {
            namedRowType.getName.get() + "=" +
              toHiveString(r.getValue, namedRowType.getTypeSignature)
          } else {
            toHiveString(r.getValue, namedRowType.getTypeSignature)
          }
        }.mkString("{", ",", "}")

      case (other, _) =>
        other.toString
    }
  }
}
