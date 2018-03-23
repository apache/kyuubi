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

import org.apache.hive.service.cli.thrift.{TCLIServiceConstants, TTypeId}
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.{DecimalType, StringType, StructType}

class ColumnDescriptorSuite extends SparkFunSuite {

  test("Column Descriptor basic test") {
    val col1 = "a"
    val col2 = "b"
    val comments = "no comments"
    val schema = new StructType()
      .add(col1, StringType, nullable = true, comments)
      .add(col2, DecimalType(10, 9), nullable = true, "")

    val tColumnDescs =
      (0 until schema.length).map(i => ColumnDescriptor(schema(i), i)).map(_.toTColumnDesc)
    assert(tColumnDescs.head.getColumnName === col1)
    assert(tColumnDescs.head.getComment === comments)
    assert(tColumnDescs.head.getPosition === 0)
    assert(tColumnDescs.head.getTypeDesc === TypeDescriptor(StringType).toTTypeDesc)
    assert(tColumnDescs.head.getTypeDesc.getTypesSize === 1)
    assert(tColumnDescs.head.getTypeDesc.getTypes.get(0)
      .getPrimitiveEntry.getTypeQualifiers === null)
    assert(tColumnDescs.head.getTypeDesc.getTypes.get(0)
      .getPrimitiveEntry.getType === TTypeId.STRING_TYPE)

    assert(tColumnDescs(1).getColumnName === col2)
    assert(tColumnDescs(1).getComment === "")
    assert(tColumnDescs(1).getPosition === 1)
    assert(tColumnDescs(1).getTypeDesc.getTypesSize === 1)
    assert(tColumnDescs(1)
      .getTypeDesc
      .getTypes.get(0)
      .getPrimitiveEntry
      .getTypeQualifiers
      .getQualifiers
      .get(TCLIServiceConstants.PRECISION).getI32Value === 10)
    assert(tColumnDescs(1)
      .getTypeDesc
      .getTypes.get(0)
      .getPrimitiveEntry
      .getTypeQualifiers
      .getQualifiers
      .get(TCLIServiceConstants.SCALE).getI32Value === 9)
    assert(tColumnDescs(1).getTypeDesc.getTypes.get(0)
      .getPrimitiveEntry.getType === TTypeId.DECIMAL_TYPE)

  }
}
