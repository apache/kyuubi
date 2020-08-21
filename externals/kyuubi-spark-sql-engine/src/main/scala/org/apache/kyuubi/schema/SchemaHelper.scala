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

package org.apache.kyuubi.schema

import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.hive.service.rpc.thrift._
import org.apache.spark.sql.types._

object SchemaHelper {

  def toTTypeId(typ: DataType): TTypeId = typ match {
    case NullType => TTypeId.NULL_TYPE
    case BooleanType => TTypeId.BOOLEAN_TYPE
    case ByteType => TTypeId.TINYINT_TYPE
    case ShortType => TTypeId.SMALLINT_TYPE
    case IntegerType => TTypeId.INT_TYPE
    case LongType => TTypeId.BIGINT_TYPE
    case FloatType => TTypeId.FLOAT_TYPE
    case DoubleType => TTypeId.DOUBLE_TYPE
    case StringType => TTypeId.STRING_TYPE
    case DecimalType() => TTypeId.DECIMAL_TYPE
    case DateType => TTypeId.DATE_TYPE
    case TimestampType => TTypeId.TIMESTAMP_TYPE
    case BinaryType => TTypeId.BINARY_TYPE
    case CalendarIntervalType => TTypeId.STRING_TYPE
    case _: ArrayType => TTypeId.ARRAY_TYPE
    case _: MapType => TTypeId.MAP_TYPE
    case _: StructType => TTypeId.STRUCT_TYPE
    // TODO: it is private now, case udt: UserDefinedType => TTypeId.USER_DEFINED_TYPE
    case other =>
      throw new IllegalArgumentException(s"Unrecognized type name: ${other.catalogString}")
  }

  def toTTypeQualifiers(typ: DataType): TTypeQualifiers = {
    val ret = new TTypeQualifiers()
    val qualifiers = typ match {
      case d: DecimalType =>
        Map(TCLIServiceConstants.PRECISION -> TTypeQualifierValue.i32Value(d.precision),
          TCLIServiceConstants.SCALE -> TTypeQualifierValue.i32Value(d.scale)).asJava
      case _ => Collections.emptyMap[String, TTypeQualifierValue]()
    }
    ret.setQualifiers(qualifiers)
    ret
  }

  def toTTypeDesc(typ: DataType): TTypeDesc = {
    val typeEntry = new TPrimitiveTypeEntry(toTTypeId(typ))
    typeEntry.setTypeQualifiers(toTTypeQualifiers(typ))
    val tTypeDesc = new TTypeDesc()
    tTypeDesc.addToTypes(TTypeEntry.primitiveEntry(typeEntry))
    tTypeDesc
  }

  def toTColumnDesc(field: StructField, pos: Int): TColumnDesc = {
    val tColumnDesc = new TColumnDesc()
    tColumnDesc.setColumnName(field.name)
    tColumnDesc.setTypeDesc(toTTypeDesc(field.dataType))
    tColumnDesc.setComment(field.getComment().getOrElse(""))
    tColumnDesc.setPosition(pos)
    tColumnDesc
  }

  def toTTableSchema(schema: StructType): TTableSchema = {
    val tTableSchema = new TTableSchema()
    schema.zipWithIndex.foreach { case (f, i) =>
      tTableSchema.addToColumns(toTColumnDesc(f, i))
    }
    tTableSchema
  }
}
