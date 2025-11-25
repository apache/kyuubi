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

import scala.util.matching.Regex

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

import org.apache.kyuubi.Logging
import org.apache.kyuubi.util.reflect.{DynClasses, DynMethods}

/**
 * A place to invoke non-public APIs of [[Utils]], anything to be added here need to
 * think twice
 */
object SparkUtilsHelper extends Logging {

  /**
   * Redact the sensitive information in the given string.
   */
  def redact(regex: Option[Regex], text: String): String = {
    Utils.redact(regex, text)
  }

  private val readOnlySparkConfCls = DynClasses.builder()
    .impl("org.apache.spark.ReadOnlySparkConf")
    .orNull()
    .build()

  private val getLocalDirMethod = DynMethods.builder("getLocalDir")
    .impl(Utils.getClass, readOnlySparkConfCls) // SPARK-53459 (4.1.0)
    .impl(Utils.getClass, classOf[SparkConf])
    .build(Utils)

  /**
   * Get the path of a temporary directory.
   */
  def getLocalDir(conf: SparkConf): String = {
    getLocalDirMethod.invoke(conf)
  }

  def classesArePresent(className: String): Boolean = {
    try {
      Utils.classForName(className)
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }
}
