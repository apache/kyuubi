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

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.scalatest.exceptions.TestFailedException
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.util.AssertionUtils._
// scalastyle:on

// scalastyle:off
class AssertionUtilsSuite extends AnyFunSuite {
// scalastyle:on

  test("assertFileContent fails on trailing extra file lines") {
    val path = Files.createTempFile("assert-file-content", ".txt")
    Files.write(path, "first\nsecond\nstale extra\n".getBytes(StandardCharsets.UTF_8))
    try {
      val thrown = intercept[TestFailedException] {
        assertFileContent(path, Seq("first", "second"), "regen.sh")
      }
      assert(thrown.getMessage.contains("Line number is not expected"))
    } finally {
      Files.deleteIfExists(path)
    }
  }

  test("assertFileContent passes on exact content") {
    val path = Files.createTempFile("assert-file-content", ".txt")
    Files.write(path, "first\nsecond\n".getBytes(StandardCharsets.UTF_8))
    try {
      assertFileContent(path, Seq("first", "second"), "regen.sh")
    } finally {
      Files.deleteIfExists(path)
    }
  }
}
