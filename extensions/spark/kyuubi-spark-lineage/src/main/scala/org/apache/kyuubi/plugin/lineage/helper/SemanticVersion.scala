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

/**
 * Encapsulate a component (Kyuubi/Spark/Hive/Flink etc.) version
 * for the convenience of version checks.
 */
case class SemanticVersion(majorVersion: Int, minorVersion: Int) {

  def isVersionAtMost(targetVersionString: String): Boolean = {
    this.compareVersion(
      targetVersionString,
      (targetMajor: Int, targetMinor: Int, runtimeMajor: Int, runtimeMinor: Int) =>
        (runtimeMajor < targetMajor) || {
          runtimeMajor == targetMajor && runtimeMinor <= targetMinor
        })
  }

  def isVersionAtLeast(targetVersionString: String): Boolean = {
    this.compareVersion(
      targetVersionString,
      (targetMajor: Int, targetMinor: Int, runtimeMajor: Int, runtimeMinor: Int) =>
        (runtimeMajor > targetMajor) || {
          runtimeMajor == targetMajor && runtimeMinor >= targetMinor
        })
  }

  def isVersionEqualTo(targetVersionString: String): Boolean = {
    this.compareVersion(
      targetVersionString,
      (targetMajor: Int, targetMinor: Int, runtimeMajor: Int, runtimeMinor: Int) =>
        runtimeMajor == targetMajor && runtimeMinor == targetMinor)
  }

  def compareVersion(
      targetVersionString: String,
      callback: (Int, Int, Int, Int) => Boolean): Boolean = {
    val targetVersion = SemanticVersion(targetVersionString)
    val targetMajor = targetVersion.majorVersion
    val targetMinor = targetVersion.minorVersion
    callback(targetMajor, targetMinor, this.majorVersion, this.minorVersion)
  }

  override def toString: String = s"$majorVersion.$minorVersion"
}

object SemanticVersion {

  def apply(versionString: String): SemanticVersion = {
    """^(\d+)\.(\d+)(\..*)?$""".r.findFirstMatchIn(versionString) match {
      case Some(m) =>
        SemanticVersion(m.group(1).toInt, m.group(2).toInt)
      case None =>
        throw new IllegalArgumentException(s"Tried to parse '$versionString' as a project" +
          s" version string, but it could not find the major and minor version numbers.")
    }
  }
}
