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

import java.util.Locale

import org.scalactic.source
import org.scalatest.Assertions.fail

object AssertionUtils {

  def assertEqualsIgnoreCase(expected: AnyRef)(actual: AnyRef)(
      implicit pos: source.Position): Unit = {
    val isEqualsIgnoreCase = (Option(expected), Option(actual)) match {
      case (Some(expectedStr: String), Some(actualStr: String)) =>
        expectedStr.equalsIgnoreCase(actualStr)
      case (Some(Some(expectedStr: String)), Some(Some(actualStr: String))) =>
        expectedStr.equalsIgnoreCase(actualStr)
      case (None, None) => true
      case _ => false
    }
    if (!isEqualsIgnoreCase) {
      fail(s"Expected equaling to '$expected' ignoring case, but got '$actual'")(pos)
    }
  }

  def assertStartsWithIgnoreCase(expectedPrefix: String)(actual: String)(
      implicit pos: source.Position): Unit = {
    if (!actual.toLowerCase(Locale.ROOT).startsWith(expectedPrefix.toLowerCase(Locale.ROOT))) {
      fail(s"Expected starting with '$expectedPrefix' ignoring case, but got [$actual]")(pos)
    }
  }

  def assertExistsIgnoreCase(expected: String)(actual: Iterable[String])(
      implicit pos: source.Position): Unit = {
    if (!actual.exists(_.equalsIgnoreCase(expected))) {
      fail(s"Expected containing '$expected' ignoring case, but got [$actual]")(pos)
    }
  }
}
