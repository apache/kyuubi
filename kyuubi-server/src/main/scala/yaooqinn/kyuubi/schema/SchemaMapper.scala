/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.schema

import org.apache.hive.service.cli.thrift.{TTableSchema, TTypeId}
import org.apache.spark.sql.types._

object SchemaMapper {

  def toTTableSchema(fields: StructType): TTableSchema = {
    val tTableSchema = new TTableSchema
    if (fields != null) {
      fields.zipWithIndex.map {
        case (field, i) => ColumnDescriptor(field, i)
      }.map(_.toTColumnDesc).foreach(tTableSchema.addToColumns)
    }
    tTableSchema
  }

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
    case _: ArrayType => TTypeId.ARRAY_TYPE
    case _: MapType => TTypeId.MAP_TYPE
    case _: StructType => TTypeId.STRUCT_TYPE
    case other =>
      val catalogString = if (other != null) other.catalogString else null
      throw new IllegalArgumentException("Unrecognized type name: " + catalogString)
  }

  def toJavaSQLType(typ: DataType): Int = typ match {
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

  def getColumnSize(typ: DataType): Option[Int] = typ match {
    case ByteType => Some(3)
    case ShortType => Some(5)
    case IntegerType => Some(10)
    case LongType => Some(19)
    case FloatType => Some(7)
    case DoubleType => Some(15)
    case d: DecimalType => Some(d.precision)
    case StringType | BinaryType | _: ArrayType | _: MapType | _: StructType => Some(Int.MaxValue)
    case DateType => Some(10)
    case TimestampType => Some(29)
    case _ => None
  }

  def getDecimalDigits(typ: DataType): Option[Int] = typ match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType => Some(0)
    case FloatType => Some(7)
    case DoubleType => Some(15)
    case d: DecimalType => Some(d.scale)
    case TimestampType => Some(9)
    case _ => None
  }

  def getNumPrecRadix(typ: DataType): Option[Int] = typ match {
    case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType | _: DecimalType =>
      Some(10)
    case _ => None

  }
}
