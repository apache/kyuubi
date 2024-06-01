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

package org.apache.kyuubi.engine.spark.schema

import java.lang.{Boolean => JBoolean}

import org.apache.spark.sql.execution.HiveResult
import org.apache.spark.sql.execution.HiveResult.TimeFormatters
import org.apache.spark.sql.types._

import org.apache.kyuubi.util.reflect.DynMethods

object RowSet {

  // SPARK-47911 (4.0.0) introduced it
  type BinaryFormatter = Array[Byte] => String

  def getBinaryFormatter: BinaryFormatter =
    DynMethods.builder("getBinaryFormatter")
      .impl(HiveResult.getClass) // for Spark 4.0 and later
      .orNoop() // for Spark 3.5 and before
      .buildChecked(HiveResult)
      .invokeChecked[BinaryFormatter]()

  def toHiveString(
      valueAndType: (Any, DataType),
      nested: JBoolean = false,
      timeFormatters: TimeFormatters,
      binaryFormatter: BinaryFormatter): String =
    DynMethods.builder("toHiveString")
      .impl( // for Spark 3.5 and before
        HiveResult.getClass,
        classOf[(Any, DataType)],
        classOf[Boolean],
        classOf[TimeFormatters])
      .impl( // for Spark 4.0 and later
        HiveResult.getClass,
        classOf[(Any, DataType)],
        classOf[Boolean],
        classOf[TimeFormatters],
        classOf[BinaryFormatter])
      .buildChecked(HiveResult)
      .invokeChecked[String](valueAndType, nested, timeFormatters, binaryFormatter)
}
