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
// scalastyle:off

import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.util.AssertionUtils._
// scalastyle:on

// scalastyle:off
class SemanticVersionSuite extends AnyFunSuite {
// scalastyle:on
  test("parse normal version") {
    val version = SemanticVersion("1.12.4")
    assert(version.majorVersion === 1)
    assert(version.minorVersion === 12)
  }

  test("parse snapshot version") {
    val version = SemanticVersion("2.14.8-SNAPSHOT")
    assert(version.majorVersion === 2)
    assert(version.minorVersion === 14)
  }

  test("parse binary version") {
    val version = SemanticVersion("0.9")
    assert(version.majorVersion === 0)
    assert(version.minorVersion === 9)
  }

  test("reject parsing illegal formatted version") {
    interceptContains[IllegalArgumentException](SemanticVersion("v1.0"))(
      "Tried to parse 'v1.0' as a project version string, " +
        "but it could not find the major and minor version numbers")
    interceptContains[IllegalArgumentException](SemanticVersion(".1.0"))(
      "Tried to parse '.1.0' as a project version string, " +
        "but it could not find the major and minor version numbers")
  }

  test("companion class compare version at most") {
    assert(SemanticVersion("1.12").isVersionAtMost("2.8.8-SNAPSHOT"))
    val runtimeVersion = SemanticVersion("1.12.4")
    assert(runtimeVersion.isVersionAtMost("2.8.8-SNAPSHOT"))
    assert(runtimeVersion.isVersionAtMost("1.14.4-SNAPSHOT"))
    assert(runtimeVersion.isVersionAtMost("1.12.4-SNAPSHOT"))
    assert(runtimeVersion.isVersionAtMost("1.12.3-SNAPSHOT"))
    assert(!runtimeVersion.isVersionAtMost("1.10.4-SNAPSHOT"))
    assert(!runtimeVersion.isVersionAtMost("0.14.4-SNAPSHOT"))
  }

  test("companion class compare version at least") {
    assert(SemanticVersion("1.12").isVersionAtLeast("1.10.4-SNAPSHOT"))
    val runtimeVersion = SemanticVersion("1.12.4")
    assert(runtimeVersion.isVersionAtLeast("1.10.4-SNAPSHOT"))
    assert(runtimeVersion.isVersionAtLeast("0.14.4-SNAPSHOT"))
    assert(runtimeVersion.isVersionAtLeast("1.12.4-SNAPSHOT"))
    assert(runtimeVersion.isVersionAtLeast("1.12.5-SNAPSHOT"))
    assert(!runtimeVersion.isVersionAtLeast("1.14.4-SNAPSHOT"))
    assert(!runtimeVersion.isVersionAtLeast("2.8.8-SNAPSHOT"))
  }

  test("companion class compare version equal to") {
    assert(SemanticVersion("1.12").isVersionEqualTo("1.12.4"))
    val runtimeVersion = SemanticVersion("1.12.4")
    assert(runtimeVersion.isVersionEqualTo("1.12.4"))
    assert(runtimeVersion.isVersionEqualTo("1.12.5-SNAPSHOT"))
    assert(runtimeVersion.isVersionEqualTo("1.12.4-SNAPSHOT"))
    assert(runtimeVersion.isVersionEqualTo("1.12.3-SNAPSHOT"))
    assert(!runtimeVersion.isVersionEqualTo("1.10.4"))
    assert(!runtimeVersion.isVersionEqualTo("2.12.8"))
  }

  test("compare version to major version only") {
    val versionFromMajorOnly = SemanticVersion("3")
    assert(versionFromMajorOnly === "3.0")
    assert(versionFromMajorOnly < "3.1")
    assert(!(versionFromMajorOnly > "3.0"))

    val runtimeVersion = SemanticVersion("2.3.4")
    assert(runtimeVersion > "1")
    assert(runtimeVersion > "2")
    assert(runtimeVersion >= "2")
    assert(!(runtimeVersion === "2"))
    assert(runtimeVersion < "3")
    assert(runtimeVersion <= "4")
  }

  test("semantic version to double") {
    assertResult(1.0d)(SemanticVersion("1").toDouble)
    assertResult(1.2d)(SemanticVersion("1.2").toDouble)
    assertResult(1.2d)(SemanticVersion("1.2.3").toDouble)
    assertResult(1.2d)(SemanticVersion("1.2.3-SNAPSHOT").toDouble)
    assertResult(1.234d)(SemanticVersion("1.234").toDouble)
    assertResult(1.234d)(SemanticVersion("1.234.567").toDouble)
  }
}
