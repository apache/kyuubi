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

package org.apache.kyuubi.engine.spark.udf

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import org.apache.kyuubi.KYUUBI_VERSION

object KDFRegistry {

  val registeredFunctions = new ArrayBuffer[KyuubiDefinedFunction]()

  val kyuubi_version: KyuubiDefinedFunction = create(
    "kyuubi_version",
    udf(() => KYUUBI_VERSION).asNonNullable(),
    "Return the version of Kyuubi Server",
    "string",
    "1.3.0")

  val engine_name: KyuubiDefinedFunction = create(
    "engine_name",
    udf(() => SparkContext.getOrCreate().getConf.get("spark.app.name")).asNonNullable(),
    "Return the engine_name of Kyuubi Server",
    "string",
    "1.3.0"
  )

  def create(
    name: String,
    udf: UserDefinedFunction,
    description: String,
    returnType: String,
    since: String): KyuubiDefinedFunction = {
    val kdf = KyuubiDefinedFunction(name, udf, description, returnType, since)
    registeredFunctions += kdf
    kdf
  }

  def registerAll(spark: SparkSession): Unit = {
    for (func <- registeredFunctions) {
      spark.udf.register(func.name, func.udf)
    }
  }
}
