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

package org.apache.kyuubi.engine.spark.shim

import org.apache.spark.sql.types.DataType

class Shim_v3_1 extends Shim_v3_0 {

  override def toHiveString(value: Any, typ: DataType): String = {
    val formatters = invokeScalaObject(
      "org.apache.spark.sql.execution.HiveResult$",
      "getTimeFormatters").asInstanceOf[AnyRef]

    invokeScalaObject("org.apache.spark.sql.execution.HiveResult$", "toHiveString",
      (classOf[(Any, DataType)], (value, typ)),
      (classOf[Boolean], Boolean.box(false)),
      (Class.forName("org.apache.spark.sql.execution.HiveResult$TimeFormatters"), formatters)
    ).asInstanceOf[String]
  }
}
