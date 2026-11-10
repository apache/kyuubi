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
package org.apache.kyuubi.util.command
// scalastyle:off
import org.scalatest.funsuite.AnyFunSuite

import org.apache.kyuubi.util.AssertionUtils._
import org.apache.kyuubi.util.command.CommandLineUtils._

// scalastyle:off
class CommandUtilsSuite extends AnyFunSuite {
// scalastyle:on

  test("assemble key value pair") {
    assertResult("abc=123")(genKeyValuePair("abc", "123"))
    assertResult("abc=123")(genKeyValuePair("   abc", "123   "))
    assertResult("abc.def=xyz.123")(genKeyValuePair("abc.def", "xyz.123"))

    // trim each side of the pair separately, so a dirty key or value cannot leave
    // whitespace inside the assembled argument
    assertResult("abc=123")(genKeyValuePair("abc ", " 123"))
    assertResult("abc=123")(genKeyValuePair("abc\t", "123"))

    assertMatches(genKeyValuePair("abc", "123"), PATTERN_FOR_KEY_VALUE_ARG)
    assertMatches(genKeyValuePair("   abc", "123   "), PATTERN_FOR_KEY_VALUE_ARG)
    assertMatches(genKeyValuePair("abc.def", "xyz.123"), PATTERN_FOR_KEY_VALUE_ARG)
  }

  test("redact key value argument assembled from a dirty key") {
    // the assembled pair carries no inner whitespace, so exact-key redaction sees the
    // clean key; a spaced key name is the shape that slips past the pattern-based
    // stage for keys like the data agent api key, whose name the default redaction
    // regex does not match at all
    val commands = Seq(genKeyValuePair("kyuubi.engine.data.agent.openai.api.key ", "secret"))
    val redacted = redactConfValues(
      commands,
      Set("kyuubi.engine.data.agent.openai.api.key"))
    assert(redacted.head == "kyuubi.engine.data.agent.openai.api.key=" +
      REDACTION_REPLACEMENT_TEXT)
  }

  test("assemble key value pair with config option") {
    assertResult("--conf abc=123")(confKeyValueStr("abc", "123"))
    assertResult("--conf abc.def=xyz.123")(confKeyValueStr("abc.def", "xyz.123"))

    assertResult(Seq("--conf", "abc=123"))(confKeyValue("abc", "123"))
    assertResult(Seq("--conf", "abc.def=xyz.123"))(confKeyValue("abc.def", "xyz.123"))
  }

  test("assemble classpath options") {
    assertResult(Seq("-cp", "/path/a.jar:/path2/b*.jar"))(
      genClasspathOption(Seq("/path/a.jar", "/path2/b*.jar")))
  }

  test("parseOptionString should parse a string as a list of arguments") {
    val input = "\"ab cd\" efgh 'i \" j'"
    val expectedOutput = List("ab cd", "efgh", "i \" j")
    val actualOutput = CommandLineUtils.parseOptionString(input)
    assert(actualOutput == expectedOutput)
  }
}
