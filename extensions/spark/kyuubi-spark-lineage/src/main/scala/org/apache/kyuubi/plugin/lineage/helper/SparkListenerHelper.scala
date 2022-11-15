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

package org.apache.kyuubi.plugin.lineage.helper

import org.apache.spark.SPARK_VERSION

object SparkListenerHelper {

  lazy val sparkMajorMinorVersion: (Int, Int) = {
    val runtimeSparkVer = org.apache.spark.SPARK_VERSION
    val runtimeVersion = SemanticVersion(runtimeSparkVer)
    (runtimeVersion.majorVersion, runtimeVersion.minorVersion)
  }

  def isSparkVersionAtMost(targetVersionString: String): Boolean = {
    SemanticVersion(SPARK_VERSION).isVersionAtMost(targetVersionString)
  }

  def isSparkVersionAtLeast(targetVersionString: String): Boolean = {
    SemanticVersion(SPARK_VERSION).isVersionAtLeast(targetVersionString)
  }

  def isSparkVersionEqualTo(targetVersionString: String): Boolean = {
    SemanticVersion(SPARK_VERSION).isVersionEqualTo(targetVersionString)
  }
}
