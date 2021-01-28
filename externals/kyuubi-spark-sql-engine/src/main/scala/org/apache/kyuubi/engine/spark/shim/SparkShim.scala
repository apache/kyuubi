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

import org.apache.spark.sql.{Row, SparkSession}

import org.apache.kyuubi.{Logging, Utils}

/**
 * A shim that defines the interface interact with Spark's catalogs
 */
trait SparkShim extends Logging {

  /**
   * Get all register catalogs in Spark's `CatalogManager`
   */
  def getCatalogs(ss: SparkSession): Seq[Row]

  protected def getSessionState(ss: SparkSession): Any = {
    invoke(classOf[SparkSession], ss, "sessionState")
  }

  protected def invoke(
      obj: Any,
      methodName: String,
      args: (Class[_], AnyRef)*): Any = {
    val (types, values) = args.unzip
    val method = obj.getClass.getDeclaredMethod(methodName, types: _*)
    method.setAccessible(true)
    method.invoke(obj, values.toSeq: _*)
  }

  protected def invoke(
      clazz: Class[_],
      obj: AnyRef,
      methodName: String,
      args: (Class[_], AnyRef)*): AnyRef = {
    val (types, values) = args.unzip
    val method = clazz.getDeclaredMethod(methodName, types: _*)
    method.setAccessible(true)
    method.invoke(obj, values.toSeq: _*)
  }

  protected def getField(o: Any, fieldName: String): Any = {
    val field = o.getClass.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(o)
  }
}

object SparkShim {
  def apply(): SparkShim = {
    val runtimeSparkVer = org.apache.spark.SPARK_VERSION
    val (major, minor) = Utils.majorMinorVersion(runtimeSparkVer)
    (major, minor) match {
      case (3, _) => new Shim_v3_0
      case (2, _) => new Shim_v2_4
      case _ => throw new IllegalArgumentException(s"Not Support spark version $runtimeSparkVer")
    }
  }
}
