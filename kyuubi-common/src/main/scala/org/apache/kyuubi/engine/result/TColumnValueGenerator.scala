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

import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

trait TColumnValueGenerator[RowT] extends TRowSetColumnGetter[RowT] {

  def asBooleanTColumnValue(row: RowT, ordinal: Int): TColumnValue = {
    val tValue = new TBoolValue
    if (!isColumnNullAt(row, ordinal)) {
      tValue.setValue(getColumnAs[JBoolean](row, ordinal))
    }
    TColumnValue.boolVal(tValue)
  }

  def asByteTColumnValue(row: RowT, ordinal: Int): TColumnValue = {
    val tValue = new TByteValue
    if (!isColumnNullAt(row, ordinal)) {
      tValue.setValue(getColumnAs[JByte](row, ordinal))
    }
    TColumnValue.byteVal(tValue)
  }

  def asShortTColumnValue(
      row: RowT,
      ordinal: Int,
      convertFunc: Any => JShort = null): TColumnValue = {
    val tValue = new TI16Value
    if (!isColumnNullAt(row, ordinal)) {
      val short = getColumnAs[JShort](row, ordinal) match {
        case sObj: JShort => sObj
        case obj if convertFunc != null => convertFunc(obj)
      }
      tValue.setValue(short)
    }
    TColumnValue.i16Val(tValue)
  }

  def asIntegerTColumnValue(
      row: RowT,
      ordinal: Int,
      convertFunc: Any => Integer = null): TColumnValue = {
    val tValue = new TI32Value
    if (!isColumnNullAt(row, ordinal)) {
      val integer = getColumnAs[Integer](row, ordinal) match {
        case iObj: Integer => iObj
        case obj if convertFunc != null => convertFunc(obj)
      }
      tValue.setValue(integer)
    }
    TColumnValue.i32Val(tValue)
  }

  def asLongTColumnValue(
      row: RowT,
      ordinal: Int,
      convertFunc: Any => JLong = null): TColumnValue = {
    val tValue = new TI64Value
    if (!isColumnNullAt(row, ordinal)) {
      val long = getColumnAs[JLong](row, ordinal) match {
        case lObj: JLong => lObj
        case obj if convertFunc != null => convertFunc(obj)
      }
      tValue.setValue(long)
    }
    TColumnValue.i64Val(tValue)
  }

  def asFloatTColumnValue(row: RowT, ordinal: Int): TColumnValue = {
    val tValue = new TDoubleValue
    if (!isColumnNullAt(row, ordinal)) {
      tValue.setValue(getColumnAs[JFloat](row, ordinal).toDouble)
    }
    TColumnValue.doubleVal(tValue)
  }

  def asDoubleTColumnValue(row: RowT, ordinal: Int): TColumnValue = {
    val tValue = new TDoubleValue
    if (!isColumnNullAt(row, ordinal)) {
      tValue.setValue(getColumnAs[JDouble](row, ordinal))
    }
    TColumnValue.doubleVal(tValue)
  }

  def asStringTColumnValue(
      row: RowT,
      ordinal: Int,
      convertFunc: Any => String = null): TColumnValue = {
    val tValue = new TStringValue
    if (!isColumnNullAt(row, ordinal)) {
      val str = getColumnAs[Any](row, ordinal) match {
        case strObj: String => strObj
        case obj if convertFunc != null => convertFunc(obj)
        case anyObj => String.valueOf(anyObj)
      }
      tValue.setValue(str)
    }
    TColumnValue.stringVal(tValue)
  }
}
