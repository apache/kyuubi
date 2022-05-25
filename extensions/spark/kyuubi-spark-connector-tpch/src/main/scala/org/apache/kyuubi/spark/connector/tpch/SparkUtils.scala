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

package org.apache.kyuubi.spark.connector.tpch

import org.apache.spark.SPARK_VERSION

object SparkUtils {

  /**
   * Given a Kyuubi/Spark/Hive version string,
   * return the (major version number, minor version number).
   * E.g., for 2.0.1-SNAPSHOT, return (2, 0).
   */
  def majorMinorVersion(version: String): (Int, Int) = {
    """^(\d+)\.(\d+)(\..*)?$""".r.findFirstMatchIn(version) match {
      case Some(m) =>
        (m.group(1).toInt, m.group(2).toInt)
      case None =>
        throw new IllegalArgumentException(s"Tried to parse '$version' as a project" +
          s" version string, but it could not find the major and minor version numbers.")
    }
  }

  /**
   * Given a Kyuubi/Spark/Hive version string, return the major version number.
   * E.g., for 2.0.1-SNAPSHOT, return 2.
   */
  def majorVersion(version: String): Int = majorMinorVersion(version)._1

  /**
   * Given a Kyuubi/Spark/Hive version string, return the minor version number.
   * E.g., for 2.0.1-SNAPSHOT, return 0.
   */
  def minorVersion(version: String): Int = majorMinorVersion(version)._2

  def isSparkVersionAtMost(ver: String): Boolean = {
    val runtimeMajor = majorVersion(SPARK_VERSION)
    val targetMajor = majorVersion(ver)
    (runtimeMajor < targetMajor) || {
      val runtimeMinor = minorVersion(SPARK_VERSION)
      val targetMinor = minorVersion(ver)
      runtimeMajor == targetMajor && runtimeMinor <= targetMinor
    }
  }

  def isSparkVersionAtLeast(ver: String): Boolean = {
    val runtimeMajor = majorVersion(SPARK_VERSION)
    val targetMajor = majorVersion(ver)
    (runtimeMajor > targetMajor) || {
      val runtimeMinor = minorVersion(SPARK_VERSION)
      val targetMinor = minorVersion(ver)
      runtimeMajor == targetMajor && runtimeMinor >= targetMinor
    }
  }

  def isSparkVersionEqualTo(ver: String): Boolean = {
    val runtimeMajor = majorVersion(SPARK_VERSION)
    val targetMajor = majorVersion(ver)
    val runtimeMinor = minorVersion(SPARK_VERSION)
    val targetMinor = minorVersion(ver)
    runtimeMajor == targetMajor && runtimeMinor == targetMinor
  }
}
