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
import java.time.ZoneId

import scala.collection.JavaConverters._

import io.trino.client.ClientStandardTypes._
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
      protocolVersion: TProtocolVersion,
      timeZone: ZoneId): TRowSet = {
    if (protocolVersion.getValue < TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6.getValue) {
      toRowBasedSet(rows, schema, timeZone)
    } else {
      toColumnBasedSet(rows, schema, timeZone)
    }
  }

  def toRowBasedSet(rows: Seq[List[_]], schema: List[Column], timeZone: ZoneId): TRowSet = {
    val tRows = rows.map { row =>
      val tRow = new TRow()
      (0 until row.size).map(i => toTColumnValue(i, row, schema, timeZone))
        .foreach(tRow.addToColVals)
      tRow
    }.asJava
    new TRowSet(0, tRows)
  }

  def toColumnBasedSet(rows: Seq[List[_]], schema: List[Column], timeZone: ZoneId): TRowSet = {
    val size = rows.size
    val tRowSet = new TRowSet(0, new java.util.ArrayList[TRow](size))
    schema.zipWithIndex.foreach { case (filed, i) =>
      val tColumn = toTColumn(
        rows,
        i,
        filed.getType,
        timeZone)
      tRowSet.addToColumns(tColumn)
    }
    tRowSet
  }

  private def toTColumn(
      rows: Seq[Seq[Any]],
      ordinal: Int,
      typ: String,
      timeZone: ZoneId): TColumn = {
    val nulls = new java.util.BitSet()
    typ match {
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
          .asScala.map(n => java.lang.Double.valueOf(n.toDouble)).asJava
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
        val values = rows.zipWithIndex.map { case (row, i) =>
          nulls.set(i, row(ordinal) == null)
          if (row(ordinal) == null) {
            ""
          } else {
            toHiveString((row(ordinal), typ), timeZone)
          }
        }.asJava
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
      types: List[Column],
      timeZone: ZoneId): TColumnValue = {

    types(ordinal).getType match {
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
        if (row(ordinal) != null) tDoubleValue.setValue(row(ordinal).asInstanceOf[Float])
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
            toHiveString((row(ordinal), types(ordinal).getType), timeZone))
        }
        TColumnValue.stringVal(tStrValue)
    }
  }

  /**
   * A simpler impl of Trino's toHiveString
   */
  def toHiveString(dataWithType: (Any, String), timeZone: ZoneId): String = {
    dataWithType match {
      case (null, _) =>
        // Only match nulls in nested type values
        "null"

      case (bin: Array[Byte], VARBINARY) =>
        new String(bin, StandardCharsets.UTF_8)

      case (s: String, VARCHAR) =>
        // Only match string in nested type values
        "\"" + s + "\""

      // for Array Map and Row, temporarily convert to string
      // TODO further analysis of type
      case (list: java.util.List[_], _) =>
        formatValue(list)

      case (m: java.util.Map[_, _], _) =>
        formatValue(m)

      case (row: Row, _) =>
        formatValue(row)

      case (other, _) =>
        other.toString
    }
  }

  def formatValue(o: Any): String = {
    o match {
      case null =>
        "null"

      case m: java.util.Map[_, _] =>
        m.asScala.map { case (key, value) =>
          formatValue(key) + ":" + formatValue(value)
        }.toSeq.sorted.mkString("{", ",", "}")

      case l: java.util.List[_] =>
        l.asScala.map(formatValue).mkString("[", ",", "]")

      case row: Row =>
        row.getFields.asScala.map { r =>
          val formattedValue = formatValue(r.getValue())
          if (r.getName.isPresent) {
            r.getName.get() + "=" + formattedValue
          } else {
            formattedValue
          }
        }.mkString("{", ",", "}")

      case _ => o.toString
    }
  }
}
