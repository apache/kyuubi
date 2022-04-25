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

import org.apache.hive.service.rpc.thrift._

object SchemaHelper {

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

  private def toTTypeId(sqlType: Int): TTypeId = sqlType match {
    case Types.TINYINT =>
      TTypeId.TINYINT_TYPE
    case Types.SMALLINT =>
      TTypeId.SMALLINT_TYPE
    case Types.INTEGER =>
      TTypeId.INT_TYPE
    case Types.BIGINT =>
      TTypeId.BIGINT_TYPE
    case Types.FLOAT =>
      TTypeId.FLOAT_TYPE
    case Types.DOUBLE =>
      TTypeId.DOUBLE_TYPE
    case Types.NUMERIC =>
      TTypeId.DECIMAL_TYPE
    case Types.DECIMAL =>
      TTypeId.DECIMAL_TYPE
    case Types.CHAR =>
      TTypeId.CHAR_TYPE
    case Types.VARCHAR =>
      TTypeId.VARCHAR_TYPE
    case Types.LONGVARCHAR =>
      TTypeId.STRING_TYPE
    case Types.DATE =>
      TTypeId.DATE_TYPE
    case Types.TIMESTAMP =>
      TTypeId.TIMESTAMP_TYPE
    case Types.BINARY =>
      TTypeId.BINARY_TYPE
    case Types.NULL =>
      TTypeId.NULL_TYPE
    case Types.JAVA_OBJECT =>
      TTypeId.MAP_TYPE
    case Types.STRUCT =>
      TTypeId.STRUCT_TYPE
    case Types.ARRAY =>
      TTypeId.ARRAY_TYPE
    case Types.BOOLEAN =>
      TTypeId.BOOLEAN_TYPE
    case Types.NVARCHAR =>
      TTypeId.STRING_TYPE
    case Types.LONGNVARCHAR =>
      TTypeId.STRING_TYPE
    case Types.BIT =>
      TTypeId.BINARY_TYPE
    case Types.REAL =>
      TTypeId.DOUBLE_TYPE
    // TODO add more type support
    case _ =>
      TTypeId.STRING_TYPE
  }

}
