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
package org.apache.kyuubi.engine.jdbc.schema

import java.sql.Types
import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.kyuubi.shaded.hive.service.rpc.thrift._

abstract class SchemaHelper {

  def toTTTableSchema(schema: List[Column]): TTableSchema = {
    val tTableSchema = new TTableSchema()
    schema.zipWithIndex.foreach { case (f, i) =>
      tTableSchema.addToColumns(toTColumnDesc(f, i))
    }
    tTableSchema
  }

  private def toTColumnDesc(column: Column, pos: Int): TColumnDesc = {
    val tColumnDesc = new TColumnDesc()
    tColumnDesc.setColumnName(column.label)
    tColumnDesc.setTypeDesc(toTTypeDesc(column.sqlType, column.precision, column.scale))
    tColumnDesc.setPosition(pos)
    tColumnDesc
  }

  private def toTTypeDesc(sqlType: Int, precision: Int, scale: Int): TTypeDesc = {
    val typeEntry = new TPrimitiveTypeEntry(toTTypeId(sqlType))
    typeEntry.setTypeQualifiers(toTTypeQualifiers(sqlType, precision, scale))
    val tTypeDesc = new TTypeDesc()
    tTypeDesc.addToTypes(TTypeEntry.primitiveEntry(typeEntry))
    tTypeDesc
  }

  private def toTTypeQualifiers(sqlType: Int, precision: Int, scale: Int): TTypeQualifiers = {
    val ret = new TTypeQualifiers()
    val qualifiers = sqlType match {
      case java.sql.Types.DECIMAL =>
        Map(
          TCLIServiceConstants.PRECISION -> TTypeQualifierValue.i32Value(precision),
          TCLIServiceConstants.SCALE -> TTypeQualifierValue.i32Value(scale)).asJava
      case _ =>
        Collections.emptyMap[String, TTypeQualifierValue]()
    }
    ret.setQualifiers(qualifiers)
    ret
  }

  protected def toTTypeId(sqlType: Int): TTypeId = sqlType match {
    case Types.BIT =>
      bitToTTypeId

    case Types.TINYINT =>
      tinyIntToTTypeId

    case Types.SMALLINT =>
      smallIntToTTypeId

    case Types.INTEGER =>
      integerToTTypeId

    case Types.BIGINT =>
      bigintToTTypeId

    case Types.REAL =>
      realToTTypeId

    case Types.DOUBLE =>
      doubleToTTypeId

    case Types.CHAR =>
      charToTTypeId

    case Types.VARCHAR =>
      varcharToTTypeId

    case Types.DATE =>
      dateToTTypeId

    case Types.TIMESTAMP =>
      timestampToTTypeId

    case Types.DECIMAL =>
      decimalToTTypeId

    case Types.FLOAT =>
      floatToTTypeId

    case Types.BOOLEAN =>
      booleanToTTypeId

    // TODO add more type support
    case _ =>
      defaultToTTypeId
  }

  protected def bitToTTypeId: TTypeId = {
    TTypeId.BOOLEAN_TYPE
  }

  protected def booleanToTTypeId: TTypeId = {
    TTypeId.BOOLEAN_TYPE
  }

  protected def tinyIntToTTypeId: TTypeId = {
    TTypeId.TINYINT_TYPE
  }

  protected def smallIntToTTypeId: TTypeId = {
    TTypeId.SMALLINT_TYPE
  }

  protected def integerToTTypeId: TTypeId = {
    TTypeId.INT_TYPE
  }

  protected def bigintToTTypeId: TTypeId = {
    TTypeId.BIGINT_TYPE
  }

  protected def realToTTypeId: TTypeId = {
    TTypeId.FLOAT_TYPE
  }

  protected def floatToTTypeId: TTypeId = {
    TTypeId.FLOAT_TYPE
  }

  protected def doubleToTTypeId: TTypeId = {
    TTypeId.DOUBLE_TYPE
  }

  protected def charToTTypeId: TTypeId = {
    TTypeId.STRING_TYPE
  }

  protected def varcharToTTypeId: TTypeId = {
    TTypeId.STRING_TYPE
  }

  protected def dateToTTypeId: TTypeId = {
    TTypeId.DATE_TYPE
  }

  protected def timestampToTTypeId: TTypeId = {
    TTypeId.TIMESTAMP_TYPE
  }

  protected def decimalToTTypeId: TTypeId = {
    TTypeId.DECIMAL_TYPE
  }

  protected def defaultToTTypeId: TTypeId = {
    TTypeId.STRING_TYPE
  }
}
