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
  }
}
