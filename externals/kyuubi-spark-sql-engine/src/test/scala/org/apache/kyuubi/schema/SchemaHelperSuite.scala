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

import scala.collection.JavaConverters._

import org.apache.spark.sql.types._

import org.apache.kyuubi.KyuubiFunSuite
import org.apache.kyuubi.shade.hive.service.rpc.thrift.{TCLIServiceConstants, TTypeId}

class SchemaHelperSuite extends KyuubiFunSuite {

  import SchemaHelper._

  val innerSchema: StructType = new StructType()
    .add("a", StringType, nullable = true, "")
    .add("b", IntegerType, nullable = true, "")

  val outerSchema: StructType = new StructType()
    .add("c0", NullType, true, "this is comment")
    .add("c1", BooleanType, true, "this is comment too")
    .add("c2", ByteType)
    .add("c3", ShortType)
    .add("c4", IntegerType)
    .add("c5", LongType)
    .add("c6", FloatType)
    .add("c7", DoubleType)
    .add("c8", StringType)
    .add("c9", DecimalType(10, 8))
    .add("c10", DateType)
    .add("c11", TimestampType)
    .add("c12", BinaryType)
    .add("c13", ArrayType(LongType))
    .add("c14", MapType(IntegerType, FloatType))
    .add("c15", innerSchema)
    .add("c16", CalendarIntervalType)


  test("toTTypeId") {
    assert(toTTypeId(outerSchema.head.dataType) === TTypeId.NULL_TYPE)
    assert(toTTypeId(outerSchema(1).dataType) === TTypeId.BOOLEAN_TYPE)
    assert(toTTypeId(outerSchema(2).dataType) === TTypeId.TINYINT_TYPE)
    assert(toTTypeId(outerSchema(3).dataType) === TTypeId.SMALLINT_TYPE)
    assert(toTTypeId(outerSchema(4).dataType) === TTypeId.INT_TYPE)
    assert(toTTypeId(outerSchema(5).dataType) === TTypeId.BIGINT_TYPE)
    assert(toTTypeId(outerSchema(6).dataType) === TTypeId.FLOAT_TYPE)
    assert(toTTypeId(outerSchema(7).dataType) === TTypeId.DOUBLE_TYPE)
    assert(toTTypeId(outerSchema(8).dataType) === TTypeId.STRING_TYPE)
    assert(toTTypeId(outerSchema(9).dataType) === TTypeId.DECIMAL_TYPE)
    assert(toTTypeId(outerSchema(10).dataType) === TTypeId.DATE_TYPE)
    assert(toTTypeId(outerSchema(11).dataType) === TTypeId.TIMESTAMP_TYPE)
    assert(toTTypeId(outerSchema(12).dataType) === TTypeId.BINARY_TYPE)
    assert(toTTypeId(outerSchema(13).dataType) === TTypeId.ARRAY_TYPE)
    assert(toTTypeId(outerSchema(14).dataType) === TTypeId.MAP_TYPE)
    assert(toTTypeId(outerSchema(15).dataType) === TTypeId.STRUCT_TYPE)
    assert(toTTypeId(outerSchema(16).dataType) === TTypeId.STRING_TYPE)
    val e1 = intercept[IllegalArgumentException](toTTypeId(CharType(1)))
    assert(e1.getMessage === "Unrecognized type name: char(1)")
    val e2 = intercept[IllegalArgumentException](toTTypeId(VarcharType(1)))
    assert(e2.getMessage === "Unrecognized type name: varchar(1)")
  }

  test("toTTypeQualifiers") {
    val qualifiers = toTTypeQualifiers(outerSchema(9).dataType)
    val q = qualifiers.getQualifiers
    assert(q.size() === 2)
    assert(q.get(TCLIServiceConstants.PRECISION).getI32Value === 10)
    assert(q.get(TCLIServiceConstants.SCALE).getI32Value === 8)

    outerSchema.foreach {
      case f if f.dataType == DecimalType(10, 8) =>
      case f => assert(toTTypeQualifiers(f.dataType).getQualifiers.isEmpty)
    }
  }

  test("toTTableSchema") {
    val tTableSchema = toTTableSchema(outerSchema)
    assert(tTableSchema.getColumnsSize === outerSchema.size)
    val iter = tTableSchema.getColumns

    iter.asScala.zipWithIndex.foreach { case (col, pos) =>
      val field = outerSchema(pos)
      assert(col.getColumnName === field.name)
      assert(col.getComment === field.getComment().getOrElse(""))
      assert(col.getPosition === pos)
      val qualifiers =
        col.getTypeDesc.getTypes.get(0).getPrimitiveEntry.getTypeQualifiers.getQualifiers
      if (pos == 9) {
        assert(qualifiers.get(TCLIServiceConstants.PRECISION).getI32Value === 10)
        assert(qualifiers.get(TCLIServiceConstants.SCALE).getI32Value === 8)
      } else {
        assert(qualifiers.isEmpty)
      }
    }
  }
}
