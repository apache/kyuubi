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

package org.apache.kyuubi.util

/**
 * Encapsulate a component version for the convenience of version checks.
 */
case class SemanticVersion(majorVersion: Int, minorVersion: Int)
  extends Comparable[SemanticVersion] {

  def ===(targetVersionString: String): Boolean = isVersionEqualTo(targetVersionString)

  def <=(targetVersionString: String): Boolean = isVersionAtMost(targetVersionString)

  def >=(targetVersionString: String): Boolean = isVersionAtLeast(targetVersionString)

  def >(targetVersionString: String): Boolean = !isVersionAtMost(targetVersionString)

  def <(targetVersionString: String): Boolean = !isVersionAtLeast(targetVersionString)

  def isVersionAtMost(targetVersionString: String): Boolean =
    compareTo(SemanticVersion(targetVersionString)) <= 0

  def isVersionAtLeast(targetVersionString: String): Boolean =
    compareTo(SemanticVersion(targetVersionString)) >= 0

  def isVersionEqualTo(targetVersionString: String): Boolean =
    compareTo(SemanticVersion(targetVersionString)) == 0

  override def compareTo(v: SemanticVersion): Int = {
    if (majorVersion > v.majorVersion) {
      1
    } else if (majorVersion < v.majorVersion) {
      -1
    } else {
      minorVersion - v.minorVersion
    }
  }

  override def toString: String = s"$majorVersion.$minorVersion"

  /**
   * Returning a double in format of "majorVersion.minorVersion".
   * Note: Not suitable for version comparison, only for logging.
   * @return
   */
  def toDouble: Double = toString.toDouble

}

object SemanticVersion {

  private val semanticVersionRegex = """^(\d+)(?:\.(\d+))?(\..*)?$""".r

  def apply(versionString: String): SemanticVersion = {
    semanticVersionRegex.findFirstMatchIn(versionString) match {
      case Some(m) =>
        val major = m.group(1).toInt
        val minor = Option(m.group(2)).getOrElse("0").toInt
        SemanticVersion(major, minor)
      case None =>
        throw new IllegalArgumentException(s"Tried to parse '$versionString' as a project" +
          s" version string, but it could not find the major and minor version numbers.")
    }
  }
}
