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
import org.apache.spark.sql.types.{ByteType, DecimalType}

import yaooqinn.kyuubi.utils.ReflectUtils

class TypeDescriptorSuite extends SparkFunSuite {

  test("TypeDescriptor basic tests") {
    val typeDescriptor = new TypeDescriptor(new DecimalType(10, 9))
    val tTypeDesc = typeDescriptor.toTTypeDesc
    assert(tTypeDesc.getTypesSize === 1)
    assert(
      tTypeDesc
        .getTypes.get(0)
        .getPrimitiveEntry
        .getTypeQualifiers
        .getQualifiers
        .get(TCLIServiceConstants.PRECISION).getI32Value === 10)

    val typeDescriptor2 = new TypeDescriptor(ByteType)
    val tTypeDesc2 = typeDescriptor2.toTTypeDesc
    assert(tTypeDesc2.getTypesSize  === 1)
    assert(tTypeDesc2.getTypes.get(0).getPrimitiveEntry.getTypeQualifiers === null)
    assert(tTypeDesc2.getTypes.get(0).getPrimitiveEntry.getType === TTypeId.TINYINT_TYPE)

    assert(ReflectUtils.getFieldValue(typeDescriptor, "typeQualifiers")
      .asInstanceOf[Option[TypeDescriptor]].isDefined)
    assert(ReflectUtils.getFieldValue(typeDescriptor2, "typeQualifiers")
      .asInstanceOf[Option[TypeDescriptor]].isEmpty)
  }

}
