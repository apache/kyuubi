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

package yaooqinn.kyuubi.utils

import org.apache.spark.SPARK_VERSION

object VersionUtils {

  val spark_1_6_x = """1\.6\.\d+""".r
  val spark_2_0_x = """2\.0\.\d+""".r
  val spark_2_1_x = """2\.1\.\d+""".r
  val spark_2_2_x = """2\.2\.\d+""".r
  val spark_2_3_x = """2\.3\.\d+""".r

  def getSupportedSpark(version: String): Option[String] = version match {
    case spark_1_6_x() | spark_2_0_x() | spark_2_1_x() | spark_2_2_x() | spark_2_3_x() =>
      Some(version)
    case _ => None
  }

  def isSupportedSpark(): Boolean = getSupportedSpark(SPARK_VERSION).nonEmpty

  def isSpark23OrLater(): Boolean = SPARK_VERSION match {
    case spark_2_3_x() => true
    case _ => false
  }

  def isSpark22OrLater(): Boolean = SPARK_VERSION match {
    case spark_2_2_x() | spark_2_3_x() => true
    case _ => false
  }

  def isSpark21OrLater(): Boolean = SPARK_VERSION match {
    case spark_2_1_x() | spark_2_2_x() | spark_2_3_x() => true
    case _ => false
  }

  def isSpark20OrLater(): Boolean = SPARK_VERSION match {
    case spark_2_0_x() | spark_2_1_x() | spark_2_2_x() | spark_2_3_x() => true
    case _ => false
  }
}
