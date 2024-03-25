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

package org.apache.spark.kyuubi

import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, UserDefinedType}

case class ExampleValue(v: Double)

class ExampleValueUDT extends UserDefinedType[ExampleValue] {

  override def sqlType: DataType = ArrayType(DoubleType, false)

  override def pyUDT: String = "pyspark.testing.ExampleValueUDT"

  override def serialize(obj: ExampleValue): GenericArrayData = {
    new GenericArrayData(Array[Any](obj.v))
  }

  override def deserialize(datum: Any): ExampleValue = {
    datum match {
      case values: ArrayData => new ExampleValue(values.getDouble(0))
    }
  }

  override def userClass: Class[ExampleValue] = classOf[ExampleValue]

  override private[spark] def asNullable: ExampleValueUDT = this
}
