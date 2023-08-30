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
import org.apache.kyuubi.util.MathUtils._
class MathUtilsSuite extends AnyFunSuite {
// scalastyle:on
  private val zeroNumbers = Seq(
    0,
    0L,
    0.0f,
    0.0d,
    -0,
    -0L,
    -0.0f,
    -0.0d,
    " 0 ",
    "0.0",
    "-0",
    "-0.0",
    "  -0.0  ",
    Char.MinValue,
    false)

  private val positiveNumbers = Seq(
    Long.MaxValue,
    Double.MaxValue,
    Float.MaxValue,
    Byte.MaxValue,
    Char.MaxValue,
    true,
    "1.23",
    " 1.23 ",
    1.123456789012345678901234567890123456789012345678901234567890d,
    "1.123456789012345678901234567890123456789012345678901234567890")

  private val negativeNumbers = Seq(
    Long.MinValue,
    Double.MinValue,
    Float.MinValue,
    Byte.MinValue,
    "-1.23",
    " -1.23 ",
    -1.123456789012345678901234567890123456789012345678901234567890d,
    "-1.123456789012345678901234567890123456789012345678901234567890")

  private val malFormatNumbers =
    Seq("0.0.0", " 1. 23 ", " 1. 23 abc ", " -1. 23 ", " -1. 23 abc ", Array[Int](0, 1))

  test("check positive number") {
    positiveNumbers.foreach(o => assert(isPositiveNumber(o)))
    Seq(negativeNumbers, zeroNumbers).flatten.foreach(o => assert(!isPositiveNumber(o)))
    malFormatNumbers.foreach { str =>
      interceptContains[IllegalArgumentException](isPositiveNumber(str))(
        s"Cannot check if '$str' is a positive number.")
    }
  }

  test("check non-negative number") {
    Seq(positiveNumbers, zeroNumbers).flatten.foreach(o => assert(isNonNegativeNumber(o)))
    negativeNumbers.foreach(o => assert(!isNonNegativeNumber(o)))
    malFormatNumbers.foreach { str =>
      interceptContains[IllegalArgumentException](isNonNegativeNumber(str))(
        s"Cannot check if '$str' is a non-negative number.")
    }
  }
}
