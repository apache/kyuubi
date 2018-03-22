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

import org.apache.hive.service.cli.thrift.TCLIServiceConstants
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.{BooleanType, DecimalType}

import yaooqinn.kyuubi.utils.ReflectUtils

class TypeQualifiersSuite extends SparkFunSuite {

  test("type qualifier basic tests") {
    val typeQualifiers1 = TypeQualifiers.fromTypeInfo(new DecimalType(10, 9))
    val typeQualifiers2 = TypeQualifiers.fromTypeInfo(BooleanType)

    assert(ReflectUtils.getObject(typeQualifiers1, "precision") === Some(10))
    assert(ReflectUtils.getObject(typeQualifiers1, "scale") === Some(9))
    assert(ReflectUtils.getObject(typeQualifiers2, "precision") === None)
    assert(ReflectUtils.getObject(typeQualifiers2, "scale") === None)

    assert(typeQualifiers1.toTTypeQualifiers
      .getQualifiers.get(TCLIServiceConstants.PRECISION).getI32Value === 10)
    assert(typeQualifiers1.toTTypeQualifiers
      .getQualifiers.get(TCLIServiceConstants.SCALE).getI32Value === 9)
    assert(!typeQualifiers1.toTTypeQualifiers
      .getQualifiers.containsKey(TCLIServiceConstants.CHARACTER_MAXIMUM_LENGTH))

    assert(typeQualifiers2.toTTypeQualifiers.getQualifiers.isEmpty)
    assert(!typeQualifiers2.toTTypeQualifiers
      .getQualifiers.containsKey(TCLIServiceConstants.PRECISION))
    assert(!typeQualifiers2.toTTypeQualifiers
      .getQualifiers.containsKey(TCLIServiceConstants.SCALE))

    ReflectUtils.invokeMethod(
      typeQualifiers2,
      "yaooqinn$kyuubi$schema$TypeQualifiers$$setPrecision",
      Seq(classOf[Int]), Seq(Integer.valueOf(8)))
    ReflectUtils.invokeMethod(
      typeQualifiers2,
      "yaooqinn$kyuubi$schema$TypeQualifiers$$setScale",
      Seq(classOf[Int]),
      Seq(Integer.valueOf(8)))

    assert(typeQualifiers2.toTTypeQualifiers
      .getQualifiers.get(TCLIServiceConstants.PRECISION).getI32Value === 8)
    assert(typeQualifiers2.toTTypeQualifiers
      .getQualifiers.get(TCLIServiceConstants.SCALE).getI32Value === 8)

  }
}
