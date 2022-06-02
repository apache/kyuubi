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

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DataType, DataTypes}

import org.apache.kyuubi.{KYUUBI_VERSION, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiReservedKeys.KYUUBI_SESSION_USER_KEY

object KDFRegistry {

  @transient
  val registeredFunctions = new ArrayBuffer[KyuubiDefinedFunction]()

  lazy val udfs: Map[String, String] = new KyuubiConf().loadFileDefaults()
    .getAllWithPrefix("kyuubi.spark.udf", "");

  lazy val appName = SparkEnv.get.conf.get("spark.app.name")
  lazy val appId = SparkEnv.get.conf.get("spark.app.id")

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
    "1.3.0")

  val engine_id: KyuubiDefinedFunction = create(
    "engine_id",
    udf(() => appId).asNonNullable(),
    "Return the spark application id for the associated query engine",
    "string",
    "1.4.0")

  val system_user: KyuubiDefinedFunction = create(
    "system_user",
    udf(() => Utils.currentUser).asNonNullable(),
    "Return the system user name for the associated query engine",
    "string",
    "1.3.0")

  val session_user: KyuubiDefinedFunction = create(
    "session_user",
    udf { () =>
      Option(TaskContext.get()).map(_.getLocalProperty(KYUUBI_SESSION_USER_KEY))
        .getOrElse(throw new RuntimeException("Unable to get session_user"))
    },
    "Return the session username for the associated query engine",
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

    udfs.foreach(entry => {
      val udfName = entry._1
      val defParts = entry._2.split(",")
      val udfFunc = Class.forName(defParts(0)).newInstance()
      val numArgs = defParts(1).toInt
      val field = classOf[DataTypes].getDeclaredField(defParts(2))
      val returnedDataType: DataType = field.get(null).asInstanceOf[DataType]

      numArgs match {
        case 0 => spark.udf.register(udfName, udfFunc.asInstanceOf[UDF0[Any]], returnedDataType)
        case 1 =>
          spark.udf.register(udfName, udfFunc.asInstanceOf[UDF1[Any, Any]], returnedDataType)
        case 2 =>
          spark.udf.register(udfName, udfFunc.asInstanceOf[UDF2[Any, Any, Any]], returnedDataType)
        case 3 =>
          spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF3[Any, Any, Any, Any]],
            returnedDataType)
        case 4 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF4[Any, Any, Any, Any, Any]],
            returnedDataType)
        case 5 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF5[Any, Any, Any, Any, Any, Any]],
            returnedDataType)
        case 6 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF6[Any, Any, Any, Any, Any, Any, Any]],
            returnedDataType)
        case 7 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF7[Any, Any, Any, Any, Any, Any, Any, Any]],
            returnedDataType)
        case 8 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF8[Any, Any, Any, Any, Any, Any, Any, Any, Any]],
            returnedDataType)
        case 9 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF9[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
            returnedDataType)
        case 10 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]],
            returnedDataType)
        case 11 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF11[
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any]],
            returnedDataType)
        case 12 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF12[
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any]],
            returnedDataType)
        case 13 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF13[
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any]],
            returnedDataType)
        case 14 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF14[
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any]],
            returnedDataType)
        case 15 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF15[
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any]],
            returnedDataType)
        case 16 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF16[
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any]],
            returnedDataType)
        case 17 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF17[
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any]],
            returnedDataType)
        case 18 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF18[
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any]],
            returnedDataType)
        case 19 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF19[
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any]],
            returnedDataType)
        case 20 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF20[
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any]],
            returnedDataType)
        case 21 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF21[
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any]],
            returnedDataType)
        case 22 => spark.udf.register(
            udfName,
            udfFunc.asInstanceOf[UDF22[
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any,
              Any]],
            returnedDataType)
      }
    })
  }
}
