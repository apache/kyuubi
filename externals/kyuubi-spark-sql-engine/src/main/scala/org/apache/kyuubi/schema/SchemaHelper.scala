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

import org.apache.spark.sql.types._

import org.apache.kyuubi.shade.hive.service.rpc.thrift._

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
    case _: DecimalType => TTypeId.DECIMAL_TYPE
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

  def toJavaSQLType(sparkType: DataType): Int = sparkType match {
    case NullType => java.sql.Types.NULL
    case BooleanType => java.sql.Types.BOOLEAN
    case ByteType => java.sql.Types.TINYINT
    case ShortType => java.sql.Types.SMALLINT
    case IntegerType => java.sql.Types.INTEGER
    case LongType => java.sql.Types.BIGINT
    case FloatType => java.sql.Types.FLOAT
    case DoubleType => java.sql.Types.DOUBLE
    case StringType => java.sql.Types.VARCHAR
    case _: DecimalType => java.sql.Types.DECIMAL
    case DateType => java.sql.Types.DATE
    case TimestampType => java.sql.Types.TIMESTAMP
    case BinaryType => java.sql.Types.BINARY
    case _: ArrayType => java.sql.Types.ARRAY
    case _: MapType => java.sql.Types.JAVA_OBJECT
    case _: StructType => java.sql.Types.STRUCT
    case _ => java.sql.Types.OTHER
  }


  /**
   * For boolean, numeric and datetime types, it returns the default size of its catalyst type
   * For struct type, when its elements are fixed-size, the summation of all element sizes will be
   * returned.
   * For array, map, string, and binaries, the column size is variable, return null as unknown.
   */
  def getColumnSize(sparkType: DataType): Option[Int] = sparkType match {
    case dt @ (BooleanType | _: NumericType | DateType | TimestampType |
               CalendarIntervalType | NullType) =>
      Some(dt.defaultSize)
    case StructType(fields) =>
      val sizeArr = fields.map(f => getColumnSize(f.dataType))
      if (sizeArr.contains(None)) {
        None
      } else {
        Some(sizeArr.map(_.get).sum)
      }
    case _ => None
  }


  /**
   * The number of fractional digits for this type.
   * Null is returned for data types where this is not applicable.
   * For boolean and integrals, the decimal digits is 0
   * For floating types, we follow the IEEE Standard for Floating-Point Arithmetic (IEEE 754)
   * For timestamp values, we support microseconds
   * For decimals, it returns the scale
   */
  def getDecimalDigits(sparkType: DataType): Option[Int] = sparkType match {
    case BooleanType | _: IntegerType => Some(0)
    case FloatType => Some(7)
    case DoubleType => Some(15)
    case d: DecimalType => Some(d.scale)
    case TimestampType => Some(6)
    case _ => None
  }

  def getNumPrecRadix(typ: DataType): Option[Int] = typ match {
    case _: NumericType => Some(10)
    case _ => None
  }
}
