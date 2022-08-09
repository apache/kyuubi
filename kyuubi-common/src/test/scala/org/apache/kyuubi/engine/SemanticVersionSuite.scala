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

package org.apache.kyuubi.engine

import org.apache.kyuubi.KyuubiFunSuite

class SemanticVersionSuite extends KyuubiFunSuite {

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
}
