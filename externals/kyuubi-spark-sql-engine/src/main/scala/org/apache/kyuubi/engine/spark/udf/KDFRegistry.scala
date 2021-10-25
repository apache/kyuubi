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

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import org.apache.kyuubi.{KYUUBI_VERSION, Logging}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.ShareLevel
import org.apache.kyuubi.engine.spark.SparkSQLEngine

object KDFRegistry extends Logging {

  @transient
  val registeredFunctions = new ArrayBuffer[KyuubiDefinedFunction]()

  val appName = SparkEnv.get.conf.get("spark.app.name")
  val appId = SparkEnv.get.conf.get("spark.app.id")

  val kyuubi_version: KyuubiDefinedFunction = create(
    "kyuubi_version",
    udf(() => KYUUBI_VERSION).asNonNullable(),
    "Return the version of Kyuubi Server",
    "string",
    "1.3.0")

  val engine_name: KyuubiDefinedFunction = create(
    "engine_name",
    udf(() => appName).asNonNullable(),
    "Return the spark application name for the associated query engine",
    "string",
    "1.3.0"
  )

  val engine_id: KyuubiDefinedFunction = create(
    "engine_id",
    udf(() => appId).asNonNullable(),
    "Return the spark application id for the associated query engine",
    "string",
    "1.4.0"
  )

  val system_user: KyuubiDefinedFunction = create(
    "system_user",
    udf(() => System.getProperty("user.name")).asNonNullable(),
    "Return the system user name for the associated query engine",
    "string",
    "1.3.0")

  val stop_engine: KyuubiDefinedFunction = create(
    "stop_engine",
    udf(() => stopEngine).asNonNullable(),
    "Stop the backend engine, it is only allowed for USER share level",
    "string",
    "1.4.0")

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

  private def stopEngine: String = {
    SparkSQLEngine.currentEngine.map { engine =>
      if (engine.getConf.get(KyuubiConf.ENGINE_SHARE_LEVEL) == ShareLevel.USER) {
        val msg = "Allow to stop engine due to shared level is USER"
        info(msg)
        engine.stop()
        msg
      } else {
        "stop_engine is only allowed for USER share level"
      }
    }.getOrElse("")
  }
}
