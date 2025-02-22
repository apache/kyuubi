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

import java.io.File

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object CommandLineUtils {
  val CONF = "--conf"

  val PATTERN_FOR_KEY_VALUE_ARG: Regex = "(.+?)=(.+)".r

  val REDACTION_REPLACEMENT_TEXT = "*********(redacted)"

  /**
   * The Java command's option name for classpath
   */
  val CP = "-cp"

  /**
   * Assemble key value pair with "=" seperator
   */
  def genKeyValuePair(key: String, value: String): String = s"$key=$value".trim

  /**
   * Assemble key value pair with config option prefix
   */
  def confKeyValue(key: String, value: String, confOption: String = CONF): Iterable[String] =
    Seq(confOption, genKeyValuePair(key, value))

  def confKeyValueStr(key: String, value: String, confOption: String = CONF): String =
    confKeyValue(key, value, confOption).mkString(" ")

  def confKeyValues(configs: Iterable[(String, String)]): Iterable[String] =
    configs.flatMap { case (k, v) => confKeyValue(k, v) }.toSeq

  /**
   * Generate classpath option by assembling the classpath entries with "-cp" prefix
   */
  def genClasspathOption(classpathEntries: Iterable[String]): Iterable[String] =
    Seq(CP, classpathEntries.mkString(File.pathSeparator))

  /**
   * Match the conf string in the form of "key=value"
   * and redact the value with the replacement text if keys are contained in given config keys
   */
  def redactConfValues(
      commands: Iterable[String],
      redactKeys: Iterable[String]): Iterable[String] = {
    redactKeys.toSet match {
      case redactKeySet if redactKeySet.isEmpty => commands
      case redactKeySet => commands.map {
          case PATTERN_FOR_KEY_VALUE_ARG(key, _) if redactKeySet.contains(key) =>
            genKeyValuePair(key, REDACTION_REPLACEMENT_TEXT)
          case part => part
        }
    }
  }

  /**
   * copy from org.apache.spark.launcher.CommandBuilderUtils#parseOptionString
   * Parse a string as if it were a list of arguments, following bash semantics.
   * For example:
   *
   * Input: "\"ab cd\" efgh 'i \" j'"
   * Output: [ "ab cd", "efgh", "i \" j" ]
   */
  def parseOptionString(s: String): List[String] = {
    val opts = ListBuffer[String]()
    val opt = new StringBuilder()
    var inOpt = false
    var inSingleQuote = false
    var inDoubleQuote = false
    var escapeNext = false
    var hasData = false

    var i = 0
    while (i < s.length) {
      val c = s.codePointAt(i)
      if (escapeNext) {
        opt.appendAll(Character.toChars(c))
        escapeNext = false
      } else if (inOpt) {
        c match {
          case '\\' =>
            if (inSingleQuote) {
              opt.appendAll(Character.toChars(c))
            } else {
              escapeNext = true
            }
          case '\'' =>
            if (inDoubleQuote) {
              opt.appendAll(Character.toChars(c))
            } else {
              inSingleQuote = !inSingleQuote
            }
          case '"' =>
            if (inSingleQuote) {
              opt.appendAll(Character.toChars(c))
            } else {
              inDoubleQuote = !inDoubleQuote
            }
          case _ =>
            if (!Character.isWhitespace(c) || inSingleQuote || inDoubleQuote) {
              opt.appendAll(Character.toChars(c))
            } else {
              opts += opt.toString()
              opt.setLength(0)
              inOpt = false
              hasData = false
            }
        }
      } else {
        c match {
          case '\'' =>
            inSingleQuote = true
            inOpt = true
            hasData = true
          case '"' =>
            inDoubleQuote = true
            inOpt = true
            hasData = true
          case '\\' =>
            escapeNext = true
            inOpt = true
            hasData = true
          case _ =>
            if (!Character.isWhitespace(c)) {
              inOpt = true
              hasData = true
              opt.appendAll(Character.toChars(c))
            }
        }
      }
      i += Character.charCount(c)
    }

    require(!inSingleQuote && !inDoubleQuote && !escapeNext, s"Invalid option string: $s")
    if (hasData) {
      opts += opt.toString()
    }
    opts.toList
  }
}
