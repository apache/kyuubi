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

import java.sql.{Date, Timestamp}

import org.apache.hive.service.cli.thrift.TTypeId
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

class SchemaMapperSuite extends SparkFunSuite {

  test("spark row schema to thrift type id and table schema") {
    val value = Array(null,
      true,
      2.toByte,
      3.toShort,
      4,
      5L,
      6.0f,
      7.02d,
      "88888888",
      BigDecimal(9999999999L),
      new Date(System.currentTimeMillis()),
      new Timestamp(System.currentTimeMillis()),
      Array(12.toByte, 12.1.toByte),
      Array[Long](13L),
      Map(14 -> 14.1f),
      Row("15", 15))

    val innerSchema = new StructType()
      .add("a", StringType, nullable = true, "")
      .add("b", IntegerType, nullable = true, "")

    val outerSchema = new StructType()
      .add("c0", NullType)
      .add("c1", BooleanType)
      .add("c2", ByteType)
      .add("c3", ShortType)
      .add("c4", IntegerType)
      .add("c5", LongType)
      .add("c6", FloatType)
      .add("c7", DoubleType)
      .add("c8", StringType)
      .add("c9", DecimalType(10, 10))
      .add("c10", DateType)
      .add("c11", TimestampType)
      .add("c12", BinaryType)
      .add("c13", ArrayType(LongType))
      .add("c14", MapType(IntegerType, FloatType))
      .add("c15", innerSchema)

    val row = new GenericRowWithSchema(value, outerSchema)

    assert(row.length === 16)
    assert(row.schema === outerSchema)
    assert(row.schema.head.dataType.isInstanceOf[NullType])
    assert(SchemaMapper.toTTypeId(row.schema.head.dataType) === TTypeId.NULL_TYPE)
    assert(SchemaMapper.toTTypeId(row.schema(1).dataType) === TTypeId.BOOLEAN_TYPE)
    assert(SchemaMapper.toTTypeId(row.schema(2).dataType) === TTypeId.TINYINT_TYPE)
    assert(SchemaMapper.toTTypeId(row.schema(3).dataType) === TTypeId.SMALLINT_TYPE)
    assert(SchemaMapper.toTTypeId(row.schema(4).dataType) === TTypeId.INT_TYPE)
    assert(SchemaMapper.toTTypeId(row.schema(5).dataType) === TTypeId.BIGINT_TYPE)
    assert(SchemaMapper.toTTypeId(row.schema(6).dataType) === TTypeId.FLOAT_TYPE)
    assert(SchemaMapper.toTTypeId(row.schema(7).dataType) === TTypeId.DOUBLE_TYPE)
    assert(SchemaMapper.toTTypeId(row.schema(8).dataType) === TTypeId.STRING_TYPE)
    assert(SchemaMapper.toTTypeId(row.schema(9).dataType) === TTypeId.DECIMAL_TYPE)
    assert(SchemaMapper.toTTypeId(row.schema(10).dataType) === TTypeId.DATE_TYPE)
    assert(SchemaMapper.toTTypeId(row.schema(11).dataType) === TTypeId.TIMESTAMP_TYPE)
    assert(SchemaMapper.toTTypeId(row.schema(12).dataType) === TTypeId.BINARY_TYPE)
    assert(SchemaMapper.toTTypeId(row.schema(13).dataType) === TTypeId.ARRAY_TYPE)
    assert(SchemaMapper.toTTypeId(row.schema(14).dataType) === TTypeId.MAP_TYPE)
    assert(SchemaMapper.toTTypeId(row.schema(15).dataType) === TTypeId.STRUCT_TYPE)
    intercept[IllegalArgumentException](SchemaMapper.toTTypeId(ObjectType(classOf[Row])))

    assert(SchemaMapper.toTTableSchema(row.schema).getColumnsSize === 16)
    assert(SchemaMapper.toTTableSchema(row.schema).getColumns.get(0).getColumnName === "c0")
    assert(SchemaMapper.toTTableSchema(row.schema).getColumns.get(0).getTypeDesc ===
      TypeDescriptor(NullType).toTTypeDesc)
    assert(SchemaMapper.toTTableSchema(row.schema).getColumns.get(15).getTypeDesc ===
      TypeDescriptor(innerSchema).toTTypeDesc)
    assert(SchemaMapper.toTTableSchema(new StructType()).getColumnsSize === 0)
    assert(SchemaMapper.toTTableSchema(null).getColumnsSize === 0)
  }

  test("spark sql type to java sql type") {
    assert(SchemaMapper.toJavaSQLType(NullType) === java.sql.Types.NULL)
    assert(SchemaMapper.toJavaSQLType(BooleanType) === java.sql.Types.BOOLEAN)
    assert(SchemaMapper.toJavaSQLType(ByteType) === java.sql.Types.TINYINT)
    assert(SchemaMapper.toJavaSQLType(ShortType) === java.sql.Types.SMALLINT)
    assert(SchemaMapper.toJavaSQLType(IntegerType) === java.sql.Types.INTEGER)
    assert(SchemaMapper.toJavaSQLType(LongType) === java.sql.Types.BIGINT)
    assert(SchemaMapper.toJavaSQLType(FloatType) === java.sql.Types.FLOAT)
    assert(SchemaMapper.toJavaSQLType(DoubleType) === java.sql.Types.DOUBLE)
    assert(SchemaMapper.toJavaSQLType(StringType) === java.sql.Types.VARCHAR)
    assert(SchemaMapper.toJavaSQLType(BinaryType) === java.sql.Types.BINARY)
    assert(SchemaMapper.toJavaSQLType(DecimalType(38, 10)) === java.sql.Types.DECIMAL)
    assert(SchemaMapper.toJavaSQLType(DateType) === java.sql.Types.DATE)
    assert(SchemaMapper.toJavaSQLType(TimestampType) === java.sql.Types.TIMESTAMP)
    assert(SchemaMapper.toJavaSQLType(ArrayType(LongType)) === java.sql.Types.ARRAY)
    assert(SchemaMapper.toJavaSQLType(MapType(IntegerType, IntegerType)) ===
      java.sql.Types.JAVA_OBJECT)
    assert(SchemaMapper.toJavaSQLType(new StructType) === java.sql.Types.STRUCT)
    assert(SchemaMapper.toJavaSQLType(CalendarIntervalType) === java.sql.Types.OTHER)
  }

  test("get column size from spark sql data type") {
    assert(SchemaMapper.getColumnSize(NullType) === None)
    assert(SchemaMapper.getColumnSize(BooleanType) === None)
    assert(SchemaMapper.getColumnSize(ByteType) === Some(3))
    assert(SchemaMapper.getColumnSize(ShortType) === Some(5))
    assert(SchemaMapper.getColumnSize(IntegerType) === Some(10))
    assert(SchemaMapper.getColumnSize(LongType) === Some(19))
    assert(SchemaMapper.getColumnSize(FloatType) === Some(7))
    assert(SchemaMapper.getColumnSize(DoubleType) === Some(15))
    assert(SchemaMapper.getColumnSize(StringType) === Some(Int.MaxValue))
    assert(SchemaMapper.getColumnSize(BinaryType) === Some(Int.MaxValue))
    assert(SchemaMapper.getColumnSize(DecimalType(38, 10)) === Some(38))
    assert(SchemaMapper.getColumnSize(DateType) === Some(10))
    assert(SchemaMapper.getColumnSize(TimestampType) === Some(29))
    assert(SchemaMapper.getColumnSize(ArrayType(LongType)) === Some(Int.MaxValue))
    assert(SchemaMapper.getColumnSize(MapType(IntegerType, IntegerType)) === Some(Int.MaxValue))
    assert(SchemaMapper.getColumnSize(new StructType) === Some(Int.MaxValue))
    assert(SchemaMapper.getColumnSize(CalendarIntervalType) === None)
  }

  test("get decimal digits from spark sql data type") {
    assert(SchemaMapper.getDecimalDigits(NullType) === None)
    assert(SchemaMapper.getDecimalDigits(BooleanType) === Some(0))
    assert(SchemaMapper.getDecimalDigits(ByteType) === Some(0))
    assert(SchemaMapper.getDecimalDigits(ShortType) === Some(0))
    assert(SchemaMapper.getDecimalDigits(IntegerType) === Some(0))
    assert(SchemaMapper.getDecimalDigits(LongType) === Some(0))
    assert(SchemaMapper.getDecimalDigits(FloatType) === Some(7))
    assert(SchemaMapper.getDecimalDigits(DoubleType) === Some(15))
    assert(SchemaMapper.getDecimalDigits(StringType) === None)
    assert(SchemaMapper.getDecimalDigits(BinaryType) === None)
    assert(SchemaMapper.getDecimalDigits(DecimalType(38, 10)) === Some(10))
    assert(SchemaMapper.getDecimalDigits(DateType) === None)
    assert(SchemaMapper.getDecimalDigits(TimestampType) === Some(9))
    assert(SchemaMapper.getDecimalDigits(ArrayType(LongType)) === None)
    assert(SchemaMapper.getDecimalDigits(MapType(IntegerType, IntegerType)) === None)
    assert(SchemaMapper.getDecimalDigits(new StructType) === None)
    assert(SchemaMapper.getDecimalDigits(CalendarIntervalType) === None)
  }

  test("get num prec radix from spark sql data type") {
    assert(SchemaMapper.getNumPrecRadix(NullType) === None)
    assert(SchemaMapper.getNumPrecRadix(BooleanType) === None)
    assert(SchemaMapper.getNumPrecRadix(ByteType) === Some(10))
    assert(SchemaMapper.getNumPrecRadix(ShortType) === Some(10))
    assert(SchemaMapper.getNumPrecRadix(IntegerType) === Some(10))
    assert(SchemaMapper.getNumPrecRadix(LongType) === Some(10))
    assert(SchemaMapper.getNumPrecRadix(FloatType) === Some(10))
    assert(SchemaMapper.getNumPrecRadix(DoubleType) === Some(10))
    assert(SchemaMapper.getNumPrecRadix(StringType) === None)
    assert(SchemaMapper.getNumPrecRadix(BinaryType) === None)
    assert(SchemaMapper.getNumPrecRadix(DecimalType(38, 10)) === Some(10))
    assert(SchemaMapper.getNumPrecRadix(DateType) === None)
    assert(SchemaMapper.getNumPrecRadix(TimestampType) === None)
    assert(SchemaMapper.getNumPrecRadix(ArrayType(LongType)) === None)
    assert(SchemaMapper.getNumPrecRadix(MapType(IntegerType, IntegerType)) === None)
    assert(SchemaMapper.getNumPrecRadix(new StructType) === None)
    assert(SchemaMapper.getNumPrecRadix(CalendarIntervalType) === None)
  }
}
